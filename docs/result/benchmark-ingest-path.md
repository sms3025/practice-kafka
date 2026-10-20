# 적재 경로 벤치마크: 같은 로그를 네 경로로 넣었을 때

> 측정 2026-10-19, Step 4+. 이 문서는 `kafka-load-test` 브랜치에서 이미 잰 것을 다시 재지 않는다. 경로마다의 처리량 한계(Kafka 전송 초당 35만~45만 줄, 적재 컨슈머 1개 초당 14.7만 행, 3개 29.3만 행, 6개 39.8만 행, 장비 2,000대 저장 지연 p50 57~73ms)는 그 브랜치의 `docs/result/benchmark-kafka-throughput.md`, `benchmark-transport.md`를 인용한다. 여기서 새로 잰 것은 같은 부하에서 경로가 바꾸는 것(part 수, 병합, 저장 지연, INSERT 응답), 저장소 장애 때의 복구, 재처리다. 설명은 `../study/11-적재-경로-비교.md`다.

## 한눈에 보기

장비 100대 × 1대당 초당 60줄 × 10분(360만 줄)을 네 경로로 동시에 넣었다.

| | Kafka 경유 | Kafka 테이블 엔진 | 직접 적재 | 직접 + 비동기 INSERT |
|---|---|---|---|---|
| 새 part 수 | 1,192 | 77 | 55,838 | 1,669 |
| part 하나의 평균 행 수 | 3,020 | 46,751 | 64 | 2,157 |
| 병합 횟수 / 병합 시간 합계 | 229 / 14.4초 | 13 / 5.3초 | 5,393 / 197초 | 335 / 23.8초 |
| 병합이 다시 읽은 행 ÷ 넣은 행 | 4.59 | 1.67 | 15.86 | 6.18 |
| active part 최대(10초마다 표본) | 11 | 9 | 62 | 12 |
| 저장 지연 p50 / p99 / 최대 | 1,107 / 2,319 / 21,390ms | 4,833 / 9,142 / 20,912ms | 843 / 1,878 / 15,645ms | 987 / 2,195 / 18,807ms |
| 앱이 보낸 INSERT 수 | 1,192 | (ClickHouse 안) | 55,838 | 55,385 |
| INSERT 응답 p50 / p99 | 10 / 36ms | (ClickHouse 안) | 22 / 97ms | 186 / 227ms |
| 병합 뒤 디스크 크기 | 121.77MiB | 121.46MiB | 121.84MiB | 121.83MiB |
| ClickHouse 3분 정지 뒤 따라잡기 | 22~28초 | 17~22초 | 33~39초 | 33~39초 |

ClickHouse 컨테이너의 CPU는 네 경로를 합쳐 평균 143%(코어 1.4개), 최대 264%였다(10초마다 `docker stats` 표본 51개). 저장 지연의 최대값은 모두 부하를 건 첫 순간에 나왔다. 에이전트 셋이 파일 100개를 한꺼번에 보내다 연결이 거부되어 다시 보낸 묶음이다.

## 측정 환경

PC와 Docker는 `benchmark-storage.md`와 같다. i7-13700H, 메모리 31.6GB, Docker Desktop(엔진 29.7.2, WSL2 VM CPU 20개)이다. Kafka `apache/kafka:4.2.2` 단일 브로커, ClickHouse `clickhouse/clickhouse-server:25.8`(25.8.33), Fluent Bit `fluent/fluent-bit:4.2`(4.2.8) 셋이다. 앱은 Windows에서 `java -jar`로 띄웠다(JBR 25.0.4). 그래서 앱이 Kafka와 ClickHouse에 닿는 길은 모두 Docker Desktop의 포트 전달을 거친다. 이 부하(초당 6,000줄, 약 수 MB/s)는 포트 전달의 한계(초당 약 50MB, 브랜치 실측)에 한참 못 미친다.

## 방법

1. `docker compose --profile compare down -v`로 모두 지우고 `--profile compare up -d`로 띄웠다. 앱은 `--app.compare.enabled=true`와 로그 인자 셋(발행·소비·수집 로그 끄기)으로 띄웠다.
2. `docker/clickhouse/compare/01-tables.sql`로 비교 테이블 셋과 Kafka 테이블 엔진(큐 테이블과 구체화 뷰)을 만들었다. 네 테이블은 `logs.app_logs`와 구조가 같다(`CREATE TABLE ... AS`).
3. 100줄로 네 경로가 모두 100행이 되는 것을 확인했다.
4. `POST /api/logs/simulate/start`(장비 100대, 1대당 초당 60줄, 600초)를 걸고, 10초마다 `system.parts`의 active part 수, 네 테이블의 행 수, `docker stats`의 ClickHouse CPU를 표본으로 남겼다.
5. 끝난 뒤 `docker/clickhouse/compare/02-compare.sql`로 `system.part_log`(NewPart, MergeParts, read_rows, duration_ms), `system.parts`, 저장 지연(`inserted_at - collected_at`), `system.query_log`(INSERT 응답)를 읽었다.
6. 장애 실험: 같은 부하를 480초 걸고 120초째 `docker stop clickhouse`, 300초째 `docker start clickhouse`. 5초마다 행 수, 적재 컨슈머 lag, 세 에이전트의 청크 수(`/api/v1/storage`), 재시도 수를 표본으로 남겼다. "따라잡기"는 다시 켜는 순간까지 쓰인 줄 수를 테이블 행 수가 넘은 첫 표본까지의 시간이다. 표본 간격 때문에 범위로 적었다.
7. 재처리: 앱을 끄고 `kafka-consumer-groups --reset-offsets --to-earliest`로 `clickhouse-sink` 그룹을 되돌린 뒤 다시 띄웠다.

입력은 시뮬레이터가 쓴 가짜 문장이다(평균 63바이트, 대외비 로그가 아니다). 실제 OHT 로그는 한 줄이 평균 169바이트라 같은 줄 수에서 바이트가 2.7배쯤 된다.

## 장애 실험 (ClickHouse 3분 정지)

| | Kafka 경유 | Kafka 테이블 엔진 | 직접 적재 | 직접 + 비동기 INSERT |
|---|---|---|---|---|
| 밀린 곳과 최대량 | Kafka, lag 1,121,952 | Kafka (엔진도 정지) | Fluent Bit 청크 1,810개 | Fluent Bit 청크 2,054개 |
| 수집 서버 응답 | 200 | 200 | 500 9,802번 | 500 11,621번 |
| 재시도 | 적재 컨슈머 156번 | 없음 | Fluent Bit 10,951번 | Fluent Bit 12,634번 |
| 따라잡기 | 22~28초 | 17~22초 | 33~39초 | 33~39초 |
| 진짜 중복(같은 키, 같은 줄) | 0 | 0 | 0 | 0 |

직접 적재 에이전트는 `Retry_Limit False`(무제한), `scheduler.cap 60`(재시도 대기 상한 60초)이다. 기본 상한 2,000초로는 재지 않았다.

## 재처리

`clickhouse-sink` 그룹을 처음으로 되돌리자 토픽 전체 6,480,100행이 INSERT 353번(평균 18,357행)으로 81.9초 동안 들어갔다(초당 약 7.9만 행). 다시 넣기 전후로 `count() FINAL`과 고유 멱등키 수는 6,434,439로 같았다. 처리 속도는 앱이 호스트에서 포트 전달을 두 번 거친 값이다. docker 안에서의 적재 컨슈머 속도는 브랜치의 초당 14.7만 행(컨슈머 1개)을 쓴다.

## 측정 중에 드러난 것

**디스크 버퍼에 남은 청크.** 부하가 끝난 뒤에도 에이전트마다 수백 줄이 보내지지 않고 디스크 버퍼에 남았다. 재시작하면 2초 안에 모두 보냈다.

| | fluent-bit (Kafka 경유) | fluent-bit-direct | fluent-bit-direct-async |
|---|---|---|---|
| 평시 실험 뒤 남은 줄(청크) | 242 (4) | 507 (7) | 160 (2) |
| 장애 실험 뒤 남은 줄(청크) | 584 (10) | 439 (7) | 410 (7) |

남은 청크는 저장 지표에서 up 상태였고 재시도 예약이 없었다. 부하를 건 순간(연결 거부가 몰린 때)과 장애 전후에 만들어진 것이 많았다. 원인은 확정하지 못했다.

**재시작 뒤 Offset_Key.** Fluent Bit 4.2.8은 재시작해 DB의 위치에서 이어 읽을 때 `Offset_Key` 값을 0부터 다시 센다. 장애 실험 앞의 재시작 때문에 그 뒤 288만 줄 중 45,661줄이 재시작 전의 다른 줄과 `(device_id, source, offset)`이 겹쳤고, 네 테이블 모두 `count() FINAL`이 쓴 줄보다 그만큼 적었다(6,434,439 / 6,480,100). 최소 실험(8바이트 줄 세 개 → 재시작 → 세 개 더)에서 4.2.8은 0, 8, 16, 5.1.4는 24, 32, 40을 붙였다.

## 이 숫자를 쓸 때 주의할 것

한 번씩 잰 값이다. 네 경로가 한 ClickHouse와 한 앱을 함께 썼으므로 서로의 CPU를 나눠 썼다. 장비 1,000대에서의 part와 병합은 재지 않았고 `study/11` 4절의 추정은 비례 계산이다. Fluent Bit 하나가 장비 100대 몫의 파일을 읽었으므로, 장비마다 에이전트가 따로 도는 실제 배치와 요청 수는 같지만 청크가 한곳에 쌓이는 점이 다르다.
