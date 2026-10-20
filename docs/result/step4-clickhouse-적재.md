# Step 4. ClickHouse 적재 + Grafana 결과

## 만든 것

세 번째 컨슈머 그룹(`clickhouse-sink`)이 `app-logs`를 배치로 읽어 ClickHouse에 넣고, Grafana가 그것을 그린다. 계획서대로 네 묶음(ClickHouse 단독 체험 → 테이블 설계 → Spring 배치 적재 → Grafana)으로 진행했다. 적재 코드는 `kafka-load-test` 브랜치에서 부하 측정에 쓴 것을 공부용으로 다듬어 가져왔고, 그 브랜치에서 잰 처리량은 다시 재지 않고 인용했다.

```
Fluent Bit ─HTTP─▶ 수집 서버 ─▶ app-logs (파티션 6) ─┬─▶ log-printer (콘솔)
                                                    ├─▶ log-stats (레벨별 건수)
                                                    └─▶ clickhouse-sink (배치, poll 한 번 = INSERT 한 번)
                                                            │ RowBinaryWithDefaults (client-v2)
                                                            ▼
                                               ClickHouse logs.app_logs ─▶ Grafana (3000)
```

| 파일 | 역할 |
|---|---|
| `docker/docker-compose.yml` | `clickhouse`(8123), `grafana`(3000) 서비스와 볼륨 |
| `docker/clickhouse/init/01-schema.sql` | `logs.app_logs`. ReplacingMergeTree, 정렬 키 = 멱등키, 일 단위 파티션, TTL(7일 ZSTD(9) 재압축, 30일 삭제) |
| `docker/clickhouse/config.d/part_log.xml` | part 생성·병합 이력(`system.part_log`) |
| `docker/grafana/provisioning/**`, `docker/grafana/dashboards/logs.json` | 데이터 소스와 대시보드(패널 다섯 개)를 파일로 |
| `sink/SinkConfig.java` | 배치 리스너 팩토리(무한 재시도), lag용 관리 클라이언트, 적재용 `ClickHouseWriter`. client-v2 클라이언트는 Step 4+에서 직접 적재 경로와 함께 쓰려고 `config/ClickHouseConfig.java`로 옮겼다 |
| `sink/ClickHouseSinkListener.java` | `@KafkaListener(batch)`. 묶음 하나 = INSERT 한 번 |
| `sink/ClickHouseWriter.java` | RowBinaryWithDefaults INSERT. 컬럼 목록 `COLUMNS` 한 곳. 테이블 정의는 첫 INSERT 때 읽는다 |
| `sink/SinkStats.java`, `SinkController.java` | `GET /api/sink/stats` (INSERT 수, 행/INSERT, 평균·최대 INSERT 시간, 실패, lag) |
| `application.properties`의 `app.sink.*`, `app.clickhouse.*` | 묶음 크기(poll·fetch), 접속 |
| `build.gradle` | `com.clickhouse:client-v2:0.10.0` |
| `ClickHouseWriterColumnsTest` | 적재 코드의 컬럼과 DDL이 어긋나지 않는지 |

## 설계에서 눈여겨볼 것

**INSERT 한 번 = part 하나이므로 묶어서 넣는다.** 배치 리스너가 `poll()` 한 번에 받은 묶음을 INSERT 한 번으로 넣는다. `max.poll.records=20000`, `fetch.min.bytes=1MB`(최대 0.5초 대기), `max.partition.fetch.bytes=8MB`로 묶음을 키웠다. 쌓여 있던 과거분은 INSERT 한 번에 평균 1.5만 행, 실시간 흐름은 약 1,870행이 실렸다. 대가로 저장이 최대 0.5초 늦다.

**정렬 키를 멱등키로 두어 중복을 저장소가 지운다.** `ORDER BY (device_id, source, offset)`의 ReplacingMergeTree는 병합 때 같은 키를 하나만 남긴다. Fluent Bit 재전송과 적재 컨슈머의 재적재(INSERT 뒤 커밋 전 장애)가 만드는 중복을 여기서 끝낸다. 병합 전에는 남아 있을 수 있어 정확한 수는 `FINAL`이나 `uniqExact`로 센다.

**커밋은 INSERT 성공 뒤, 실패는 끝없이 재시도.** 기본 에러 핸들러는 10번 실패하면 묶음을 건너뛰어 저장소 장애 동안 로그를 버린다. `FixedBackOff(1초, 무제한)`으로 바꿨다. 그동안 로그는 Kafka에 남고 lag만 쌓인다. 계속 실패하는 메시지(예: 형식이 깨진 한 건)가 있으면 그 파티션이 영원히 멈추는 단점이 있고, 그것을 DLT로 빼는 것은 Step 5다.

**ClickHouse가 없어도 수집은 계속된다.** 테이블 정의(RowBinary에 필요한 컬럼 순서와 타입)를 앱 기동이 아니라 첫 INSERT 때 읽게 했다. 그래서 ClickHouse가 늦게 떠도 수집 서버는 뜨고, 적재만 기다린다. Kafka를 사이에 둔 이유가 코드 한 곳에서도 지켜지게 한 것이다.

**TTL은 운영 값을 테이블에, 동작 확인은 연습 테이블에서.** 명세서의 "N일 후 압축 보관, M일 후 삭제"를 연습값 7일·30일로 선언했다. 실제로 지워지는 것은 1분 TTL 연습 테이블로 확인한다. 보존 기간의 실제 값은 아직 열려 있다.

**대시보드는 파일이 원본이다.** 데이터 소스와 대시보드를 provisioning 파일로 넣어 누가 띄워도 같은 화면이 나온다. ClickHouse 플러그인은 첫 기동 때 인터넷에서 받으므로, 망분리 환경에서는 플러그인 파일을 미리 넣어야 한다.

## 실행 절차

```powershell
# 0. 인프라 (clickhouse, grafana 가 추가됐다). ClickHouse 는 빈 볼륨으로 처음 뜰 때 테이블을 만든다
docker compose -f docker/docker-compose.yml up -d
docker exec clickhouse clickhouse-client --user app --password app --query "SHOW TABLES FROM logs"

# 1. 앱 (적재 컨슈머 포함). 뜨자마자 토픽에 쌓여 있던 과거분을 적재한다
$env:JAVA_HOME = "C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr"; .\gradlew.bat bootJar
& "$env:JAVA_HOME\bin\java.exe" -jar build\libs\practice-kafka-0.0.1-SNAPSHOT.jar
Invoke-RestMethod http://localhost:8080/api/sink/stats

# 2. 로그를 흘리고 ClickHouse 에서 센다
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate?lines=1000"
docker exec clickhouse clickhouse-client --user app --password app --query "SELECT level, count() FROM logs.app_logs GROUP BY level"

# 3. ClickHouse 장애: 멈추고 흘린 뒤 다시 켠다
docker stop clickhouse; Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate?lines=20000"
Invoke-RestMethod http://localhost:8080/api/sink/stats        # lag 이 쌓이고 failures 가 는다
docker start clickhouse                                       # 몇 초 안에 lag 0

# 4. 대시보드: http://localhost:3000 → 장비 로그 (ClickHouse)
```

손으로 하는 SQL 실습(part와 병합, 중복 제거, TTL)과 명령마다의 기대 화면은 `../practice/07-ClickHouse-적재.md`에 있다.

## 확인해야 할 것

2026-10-19 이 PC에서 실측한 값이다.

- 연습 테이블에 INSERT 세 번 → `system.parts`에 활성 part 셋. `OPTIMIZE ... FINAL` 뒤 `all_1_3_1` 하나. `system.part_log`에 `NewPart 3`, `MergeParts 1`.
- 같은 멱등키를 두 번 INSERT → `count()` 2, `count() FINAL` 1, `OPTIMIZE FINAL` 뒤 `count()` 1.
- 1분 TTL 연습 테이블 → 넣은 지 약 1분 30초 뒤 만료된 행이 저절로 사라짐.
- 앱 기동 직후 → 토픽의 과거분 123,403건을 INSERT 8번(평균 15,425행, 81ms)으로 적재. 테이블에는 123,198행(연습용 생성기의 가짜 오프셋이 겹쳐 205건이 중복으로 지워짐. `study/09` 2절).
- 실제 로그 351,327줄을 `docker/devices/OHT-0001`에 복사 → 약 20초 뒤 `count()`, `FINAL`, 고유 멱등키 모두 351,327. INSERT 188번, 끝난 뒤 활성 part 12개.
- ClickHouse를 멈추고 2만 줄 → 수집 서버는 2만 줄을 받음, `lag` 20,000, 실패 29회. 다시 켠 지 2초 안에 `lag` 0, 그 2만 줄이 정확히 20,000행(중복 0).
- 평시 흐름(2초마다 1,000줄) → 저장 지연 p50 0.67초, p99 1.17초.
- Grafana: 데이터 소스 상태 "Data source is working". 다섯 패널의 쿼리가 모두 데이터를 돌려줌(레벨 5계열, 장비 10개, ERROR 100행 등).

같은 날 오후, 볼륨을 모두 지운 깨끗한 환경에서 실습 00~07(E 제외)을 처음부터 다시 따라 해 위 결과를 재현했다(`../practice/README.md` 7절). 다른 숫자가 나온 것은 둘이다. 과거분은 B~G절에서 쌓인 485,556건이 INSERT 26번(평균 18,675행)으로 들어갔다. 실제 로그 351,327줄은 앱을 기본 로그 설정으로 띄우면 약 2분 걸렸고(줄마다 콘솔 출력, 158MB), 발행·소비 로그를 끄면 17초였다. 실습 안내의 H6 기동 명령에 로그 인자를 넣었다. `SHOW CREATE TABLE`은 기본 동작인 `DELETE`를 생략해 보여 준다.

## 실험 (해볼 것)

- `app.sink.fetch-min-bytes=1`로 되돌리고 같은 흐름을 보낸다. INSERT 한 번의 행 수가 줄고 INSERT 수와 part가 늘어야 한다(아직 실측하지 않았다).
- `app.sink.concurrency=6`으로 적재 스레드를 늘린다. 처리량의 변화는 브랜치에서 이미 쟀으므로(1개 14.7만, 3개 29.3만, 6개 39.8만 행/s), 여기서는 `system.part_log`의 `NewPart`가 어떻게 늘어나는지만 본다.
- 앱을 강제 종료(INSERT 직후 커밋 전)했다가 다시 띄워, 같은 묶음이 두 번 들어간 뒤 `FINAL`로 하나가 되는지 본다.

## 테스트

`./gradlew test` 12건 통과(2026-10-19). Step 4가 더한 `ClickHouseWriterColumnsTest`는 DDL 파일의 컬럼 이름과 순서가 `ClickHouseWriter.COLUMNS`(서버가 채우는 `inserted_at` 제외)와 같은지 확인한다. RowBinary는 이름이 아니라 순서와 타입으로 값을 보내므로 이 어긋남이 가장 흔한 사고다. ClickHouse를 띄워야 하는 테스트는 두지 않았다(`./gradlew test`가 docker 없이 돌아야 한다). Spring 테스트 둘은 `app.sink.enabled=false`로 적재 컨슈머를 끈다.

## 배운 점

"Kafka가 완충이다"라는 말이 숫자 하나로 보였다. ClickHouse를 멈춘 동안 수집은 아무 일 없이 2만 줄을 받았고, 밀린 양은 `lag 20000`으로만 드러났다가 다시 켠 지 2초 만에 0이 됐다. 저장소의 장애가 장비 쪽까지 번지지 않는다.

멱등키는 "유일하다"는 전제 위에서만 안전하다. Fluent Bit의 파일 오프셋으로 만든 키는 같은 줄을 두 번 넣어도 하나로 접혔지만, 연습용 생성기의 가짜 오프셋은 서로 다른 메시지 205건을 중복으로 지웠다. 중복 제거를 저장소에 맡기면, 키를 무엇으로 만드는지가 곧 데이터 정합성이다.
