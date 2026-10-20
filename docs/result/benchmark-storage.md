# 저장 벤치마크: 실제 로그가 ClickHouse에서 차지하는 크기

> 측정 2026-10-19, Step 4. 이 문서는 `kafka-load-test` 브랜치에서 이미 잰 것을 다시 재지 않는다. 적재 처리량(컨슈머 수별 초당 행 수)은 그 브랜치의 `docs/result/benchmark-transport.md` 7.3절을 인용한다. 그 브랜치에서 잰 압축률(한 행 63.5바이트)은 같은 줄이 반복해 들어가 중복이 지워진 데이터로 잰 것이라 한계가 있었다. 여기서는 중복 없는 실제 로그로 저장 크기와 압축 방식별 차이를 새로 잰다. 측정은 한 번씩이다.

## 한눈에 보기

대외비 생성기 로그 1대분(23개 파일, 351,327줄)을 실제 경로(Fluent Bit → 수집 서버 → Kafka → 적재 컨슈머 → ClickHouse)로 넣고, 같은 행을 압축 방식만 다른 두 테이블에 복사해 병합을 끝낸 뒤 쟀다.

| 형태 | 크기 | 한 줄(행)당 | 원문 대비 |
|---|---|---|---|
| 원문 텍스트 파일 | 59.4MB | 169바이트 | 1.00 |
| Kafka 메시지 (LogEvent JSON) | 191.8MB | 546바이트 | 3.23 |
| ClickHouse 압축 전 (컬럼 형식) | 136.0MB | 387바이트 | 2.29 |
| ClickHouse LZ4 (기본) | 21.6MB | 61.4바이트 | 0.36 |
| ClickHouse ZSTD(9) (7일 뒤 TTL 재압축) | 10.0MB | 28.5바이트 | 0.17 |

압축 전 대비 LZ4는 0.159, ZSTD(9)는 0.074다. 원문 텍스트 파일보다도 LZ4는 3분의 1, ZSTD(9)는 6분의 1 크기다. 이 문서의 MB는 1,000,000바이트다.

## 측정 환경과 방법

PC와 Docker는 `benchmark-transport.md`(브랜치)와 같다. i7-13700H, 메모리 31.6GB, Docker Desktop 4.88(WSL2 VM CPU 20개)이다. ClickHouse는 `clickhouse/clickhouse-server:25.8`(25.8.33.6), 테이블은 `docker/clickhouse/init/01-schema.sql` 그대로다.

1. `tools/OHT-0001`을 `docker/devices/OHT-0001`에 복사해 실제 경로로 적재했다. `count()`, `count() FINAL`, `uniqExact(source, offset)`이 모두 351,327이라 중복이 없다.
2. 그 행만 두 테이블에 복사했다. `c_lz4`는 `CREATE TABLE ... AS logs.app_logs` 뒤 `REMOVE TTL`로 기본 LZ4만 쓰게 했다. `c_zstd`는 TTL을 그대로 두었다. 로그의 이벤트 시각이 2026-10-07이라 이미 7일이 지났으므로, 병합하면 TTL 재압축이 적용된다.
3. 두 테이블에 `OPTIMIZE TABLE ... FINAL`을 한 뒤 `system.parts`(압축 방식, 압축 전·후 바이트)와 `system.parts_columns`(컬럼별)를 읽었다. `c_zstd`의 part는 `default_compression_codec`가 `ZSTD(9)`로 바뀌어 있었다.

```sql
SELECT table, any(default_compression_codec), sum(rows), sum(data_uncompressed_bytes), sum(data_compressed_bytes)
FROM system.parts WHERE database = 'logs' AND table IN ('c_lz4', 'c_zstd') AND active GROUP BY table;
```

## 컬럼별

압축 전은 테이블 하나 기준이다.

| 컬럼 | 압축 전 | LZ4 | ZSTD(9) | 비고 |
|---|---|---|---|---|
| `raw` | 59.6MB | 10.56MB | 5.18MB | 원문. 명세서 필수 |
| `message` | 42.4MB | 7.17MB | 3.66MB | 원문에서 머리(시각·레벨)를 뗀 나머지. raw 대부분과 겹친다 |
| `offset` | 2.8MB | 1.58MB | 0.61MB | 파일 안 바이트 위치. 커지는 정수 |
| `event_time` | 2.8MB | 1.24MB | 0.36MB | |
| `collected_at` | 2.8MB | 0.88MB | 0.18MB | |
| `source` | 21.8MB | 0.09MB | 0.01MB | 정렬 키라 같은 값이 길게 이어진다 |
| `inserted_at` | 2.8MB | 0.05MB | 0.02MB | |
| `device_id`, `level`, `category` | 각 0.4MB | 0.00 | 0.00 | LowCardinality |

LZ4 기준 `raw`와 `message` 두 컬럼이 전체(21.6MB)의 82%다. `message`는 원문을 한 번 더 저장하는 셈이다. `source`는 압축 전에는 세 번째로 크지만 정렬 키의 둘째 컬럼이라 같은 경로가 수만 행씩 이어져 거의 사라진다. 정렬 키가 압축에도 영향을 준다는 예다.

## 기업 프로젝트로 옮기면 (계산)

장비 1,000대가 하루 종일 peak 강도(1대 1초 60.3줄, 브랜치의 실측 입력 기준)로 로그를 낸다고 하면 하루 52.1억 행이다. 이 문서의 한 행 크기를 곱하면 다음과 같다. 실제로는 시간대에 따라 강도가 내려가므로(생성기의 `daily` 모드) 상한이다.

| 보관 상태 | 하루 | 비고 |
|---|---|---|
| LZ4 (7일 이내) | 약 320GB | 52.1억 × 61.4바이트 |
| ZSTD(9) (7일 이후) | 약 149GB | 52.1억 × 28.5바이트 |
| 30일 보존 합계 | 약 5.7TB | 7일 × 320GB + 23일 × 149GB |

`message`를 저장하지 않고 조회할 때 `raw`에서 잘라 쓰면 LZ4 기준 약 33%(7.17 ÷ 21.6)가 줄어든다(계산). 명세서의 필수 필드는 `raw`이고 `message`는 파싱 부가 필드이므로, 저장 비용과 조회 편의 사이의 결정 거리다.

## 함께 확인한 것 (처리량이 아닌 동작)

처리량은 재지 않았지만, 같은 적재 중에 다음을 확인했다.

- INSERT 한 번의 행 수. 토픽에 쌓여 있던 과거분은 한 번 평균 15,425행(`max.poll.records=20000` 근처), 실제 로그가 실시간으로 흘러올 때는 약 1,870행(`fetch.min.bytes` 1MB ÷ 메시지 546바이트 근처)이었다.
- part. 실제 로그 적재 중 INSERT 188번으로 part가 188개 생겼지만, 끝났을 때 테이블 전체의 활성 part는 12개였다. 병합이 따라갔다.
- TTL. 1분 TTL을 건 연습 테이블에서 만료된 행이 `OPTIMIZE` 없이 1분 30초 안에 지워졌다(`merge_with_ttl_timeout=60`).

## 한계

- 1대분 3시간 로그다. 장비마다, 시간대마다 메시지 내용이 다르면 압축률이 달라진다. 특히 이 로그는 같은 장비의 같은 파일이 길게 이어져 `source` 압축이 유리하다.
- 병합을 `OPTIMIZE FINAL`로 끝낸 상태다. 실제 운영 중에는 아직 합쳐지지 않은 작은 part가 있어 조금 더 크다.
- ZSTD(9)는 압축할 때 CPU를 더 쓴다. TTL 재압축은 백그라운드 병합에서 일어나므로, 적재가 바쁜 시간에 병합과 CPU를 다툴 수 있다. 이것은 재지 않았다.
