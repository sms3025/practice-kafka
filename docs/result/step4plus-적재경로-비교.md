# Step 4+. 적재 경로 비교 결과

## 만든 것

명세서의 기본 아키텍처(수집 서버가 ClickHouse에 바로 넣기)와 이 저장소의 설계(Kafka를 사이에 두기)를 같은 입력으로 비교하는 장치다. 경로 넷을 동시에 돌린다.

```
                 ┌─ fluent-bit ──────────── /ingest/fluentbit ─▶ Kafka ─┬─▶ 적재 컨슈머(sink/) ──────────▶ app_logs
장비 로그 파일 ──┤                                                      └─▶ Kafka 테이블 엔진 + 구체화 뷰 ─▶ app_logs_kafka_engine
                 ├─ fluent-bit-direct ───── /ingest/direct ─────────────────────────────────────────▶ app_logs_direct
                 └─ fluent-bit-direct-async  /ingest/direct?async=true ─────────────────────────────▶ app_logs_direct_async
```

| 파일 | 역할 |
|---|---|
| `direct/DirectIngestController.java` | `POST /ingest/direct`(묶음 하나 = INSERT 한 번), `?async=true`(비동기 INSERT), `GET /api/compare/stats`. `app.compare.enabled=true`일 때만 생긴다 |
| `config/ClickHouseConfig.java` | client-v2 클라이언트 하나. 적재 컨슈머와 직접 적재가 함께 쓴다(Step 4의 `SinkConfig`에서 옮김) |
| `sink/ClickHouseWriter.java` | 빈(bean)이 아닌 일반 객체로 바꿨다. 테이블 이름과 INSERT 설정을 만드는 쪽이 정한다 |
| `simulator/DeviceLogSimulator.java` | `start(devices, linesPerSecond, seconds)`, `stop()`, 쓴 줄 수. 장비별로 모아 파일마다 한 번에 쓴다 |
| `api/LogController.java` | `POST /api/logs/simulate/start`, `/stop`. stats에 `simulatorWritten`, `simulatorRunning` |
| `docker/docker-compose.yml` | `compare` 프로필: `fluent-bit-direct`(2021), `fluent-bit-direct-async`(2022). ClickHouse에 `./clickhouse/compare`를 `/compare`로 |
| `docker/fluent-bit/fluent-bit-direct.conf` | 두 비교 에이전트가 함께 쓰는 설정. 보낼 곳은 환경 변수 `INGEST_URI`, 재시도 대기 상한 60초 |
| `docker/fluent-bit/fluent-bit.conf` | `storage.metrics on`(디스크 버퍼의 청크 수를 `/api/v1/storage`로) |
| `docker/clickhouse/compare/01-tables.sql`, `02-compare.sql`, `99-drop.sql` | 비교 테이블과 Kafka 테이블 엔진, 비교 쿼리, 정리 |
| `application.properties`의 `app.compare.*` | 켜기와 테이블 이름. `app.clickhouse.max-connections` 16 → 64 |
| `DirectIngestControllerTest`, `ClickHouseWriterColumnsTest`(1건 추가) | 응답 계약, 큐 테이블 컬럼과 JSON 필드 |

## 설계에서 눈여겨볼 것

**비교가 공정하려면 차이가 하나여야 한다.** 직접 적재 컨트롤러는 Kafka 경유와 같은 정규화(`LogNormalizer`)와 같은 쓰기(`ClickHouseWriter`)를 쓴다. 테이블도 `CREATE TABLE ... AS logs.app_logs`로 복사했다. 에이전트 셋은 같은 파일을 같은 규칙으로 읽고, 보내는 곳과 저장 폴더만 다르다. 그래서 숫자의 차이는 "사이에 무엇이 있는가"만의 차이가 된다.

**응답 계약은 그대로다.** `/ingest/direct`도 저장소가 기록한 뒤에 200, 실패하면 500이다. 비동기 INSERT는 `wait_for_async_insert=1`로 기록 뒤에 응답하게 했다. 0이면 빨라지지만 ClickHouse가 버퍼를 쓰기 전에 죽을 때 at-least-once가 깨진다.

**Kafka 테이블 엔진은 일부러 기본값으로 둔다.** 묶음을 넘기는 주기(`kafka_flush_interval_ms`, 기본 7.5초)를 줄이지 않았다. 아무것도 만지지 않았을 때 무엇이 나오는지가 비교의 출발점이고, 그 결과가 "part는 가장 적고 지연은 가장 길다"였다.

**비교 장치는 꺼져 있는 것이 기본이다.** `app.compare.enabled=false`, compose의 `compare` 프로필, 자동 실행되지 않는 비교 SQL. Step 4까지의 실습과 실행에는 아무것도 바뀌지 않는다. 비교 테이블을 만들면 ClickHouse 안에서 Kafka 컨슈머가 계속 돌므로, 끝나면 `99-drop.sql`로 지운다.

## 실행 절차

```powershell
docker compose -f docker/docker-compose.yml --profile compare down -v
docker compose -f docker/docker-compose.yml --profile compare up -d --wait
$env:JAVA_HOME = "C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr"; .\gradlew.bat bootJar
& "$env:JAVA_HOME\bin\java.exe" -jar build\libs\practice-kafka-0.0.1-SNAPSHOT.jar --app.compare.enabled=true --logging.level.com.practice.kafka.producer=INFO --logging.level.com.practice.kafka.consumer=WARN --logging.level.com.practice.kafka.ingest=INFO
# (다른 터미널) 앱이 토픽을 만든 뒤
docker exec clickhouse clickhouse-client --user app --password app --queries-file /compare/01-tables.sql
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate/start"     # 장비 100대 x 초당 60줄 x 10분
docker exec clickhouse clickhouse-client --user app --password app --format PrettyCompactMonoBlock --queries-file /compare/02-compare.sql
```

장애 실험과 재처리, 기대 화면은 `../practice/08-적재-경로-비교.md`에 있다.

## 확인해야 할 것

2026-10-19 이 PC에서 실측한 값이다. 표 전체는 `benchmark-ingest-path.md`에 있다.

- 100줄 → 네 테이블 모두 100행. 직접 적재 두 경로는 INSERT 20번(장비 파일 수).
- 360만 줄, 10분 → 새 part: Kafka 경유 1,192, Kafka 테이블 엔진 77, 직접 적재 55,838, 비동기 1,669. active part 최대는 직접 적재 62, 나머지 12 이하. 병합 시간 합계는 직접 적재 197초, Kafka 경유 14초.
- 저장 지연 p50: Kafka 경유 1.1초, 테이블 엔진 4.8초, 직접 0.8초, 비동기 1.0초. 비동기 INSERT의 응답 p50 186ms.
- ClickHouse 3분 정지 → Kafka 경유는 수집 서버가 계속 200, lag 최대 112만. 직접 적재는 500을 1만 번 안팎, 에이전트 청크 2,000개 안팎. 다시 켠 뒤 따라잡기 17~39초. 진짜 중복 0.
- 재처리 → 648만 행을 82초에 다시 넣었고 `count() FINAL`은 변하지 않았다.
- 끝난 뒤 에이전트마다 수백 줄이 디스크 버퍼에 남았고, 재시작하자 2초 안에 들어왔다.
- 에이전트를 재시작한 뒤 읽은 줄의 `offset`이 0부터 다시 매겨져, 45,661줄이 다른 줄과 멱등키가 겹쳤다.

## 실험 (해볼 것)

- `kafka_flush_interval_ms`를 1000으로 줄여 Kafka 테이블 엔진의 지연과 part 수가 어떻게 바뀌는지 본다(큐 테이블을 지우고 SETTINGS에 넣어 다시 만든다).
- 비동기 INSERT의 `async_insert_busy_timeout_max_ms`를 1000으로 올려 part 수와 응답 시간을 맞바꿔 본다.
- 직접 적재 테이블에 `parts_to_delay_insert`를 50으로 낮춰(`ALTER TABLE ... MODIFY SETTING`) 장비가 많을 때 ClickHouse가 INSERT를 늦추는 모습을 미리 본다.
- `fluent/fluent-bit:5.1.4`로 바꿔 장애 실험을 다시 하고, 재시작 뒤 오프셋과 디스크 버퍼에 남는 청크가 어떻게 되는지 본다(계획서의 열린 항목).

## 테스트

`./gradlew test` 16건 통과(2026-10-19). Step 4+가 더한 `DirectIngestControllerTest`는 ClickHouse 없이 가짜 writer로 세 가지를 본다. 묶음 하나가 INSERT 한 번이 되는지, `async=true`가 비동기 INSERT 쪽으로 가는지, 저장 실패면 500인지. `ClickHouseWriterColumnsTest`에는 Kafka 테이블 엔진의 큐 테이블 컬럼이 `LogEvent`의 JSON 필드와 같은지를 더했다. 다르면 오류 없이 빈 값이 들어가므로 테스트로만 잡힌다.

## 배운 점

Kafka가 사서 주는 것이 셋으로 나뉘어 보였다. INSERT 크기를 장비 수에서 떼어 놓는 것(part 47배 차이), 저장소의 장애를 장비 쪽에서 떼어 놓는 것(500 1만 번 대 0번), 지난 로그를 다시 넣을 수 있는 것(오프셋 되돌리기 한 줄). 지연은 0.3초 더 내고 산다. 반대로 비동기 INSERT는 첫째를 ClickHouse 설정 둘로 거의 얻는다. 그래서 "Kafka를 넣을까"는 둘째와 셋째가 필요한지의 질문이 된다.

측정은 묻지 않은 것도 보여 줬다. 경로를 비교하려고 쓴 줄 수와 저장된 행 수를 맞춰 보다가, 에이전트의 디스크 버퍼에 청크가 남는 것과, 재시작한 에이전트가 오프셋을 0부터 다시 세는 것을 찾았다. 둘 다 경로와 무관하게 Log Collector 전체에 걸리는 문제다. 특히 둘째는 멱등키로 중복을 지우는 설계를 유실로 바꾼다. "중복 제거는 키가 유일하다는 전제 위에서만 안전하다"(Step 4)를 에이전트 버전 하나가 깨뜨린 사례다.
