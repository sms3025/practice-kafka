# 07. ClickHouse 적재와 대시보드 (워크북 H절, `study/08`~`10`)

ClickHouse를 먼저 손으로 만져 보고(H1~H5), 적재 컨슈머를 붙이고(H6~H7), Grafana로 본다(H8). 설명은 `../study/08-ClickHouse-기초.md`, `09-ClickHouse-적재와-멱등키.md`, `10-대시보드.md`에 있다.

시작 전 상태: 06 파일을 마쳤다. 앱은 꺼 둔다. 명령은 모두 프로젝트 루트의 PowerShell에서 친다. SQL은 아래 둘 중 편한 곳에서 친다.

- 브라우저 SQL 창: http://localhost:8123/play (오른쪽 위에 사용자 `app`, 암호 `app`)
- 터미널: `docker exec -it clickhouse clickhouse-client --user app --password app` (나갈 때 `exit`)

## H1. ClickHouse 띄우기

**왜 하는가.** Step 4부터 compose에 ClickHouse와 Grafana가 들어 있다. 처음 뜰 때 테이블이 만들어지는지 본다.

**할 것.**

```powershell
docker compose -f docker/docker-compose.yml up -d
docker ps --format "{{.Names}} {{.Status}}"
docker exec clickhouse clickhouse-client --user app --password app --query "SHOW TABLES FROM logs"
```

**보여야 하는 것.** `clickhouse ... (healthy)`, `grafana Up`. 테이블 목록에 `app_logs`. Grafana는 첫 기동 때 플러그인을 받느라 1분쯤 걸릴 수 있다.

**안 될 때.** `app_logs`가 없으면 ClickHouse 볼륨이 예전 것이다. 초기화 SQL은 빈 볼륨일 때만 돈다. `docker compose -f docker/docker-compose.yml rm -sf clickhouse`, `docker volume rm practice-kafka_clickhouse-data` 뒤 다시 `up -d`. Grafana가 계속 재시작하면 인터넷에 닿지 않아 플러그인을 못 받은 것이다(`docker logs grafana`).

## H2. INSERT 한 번 = part 하나

**왜 하는가.** 이 단계 전체가 기대는 사실을 직접 본다.

**할 것.** SQL 창에서 한 줄씩:

```sql
CREATE TABLE logs.parts_demo (id UInt32, msg String) ENGINE = MergeTree ORDER BY id;
INSERT INTO logs.parts_demo VALUES (1, 'first insert');
INSERT INTO logs.parts_demo VALUES (2, 'second insert');
INSERT INTO logs.parts_demo VALUES (3, 'third insert');
SELECT name, rows, active FROM system.parts WHERE database = 'logs' AND table = 'parts_demo';
OPTIMIZE TABLE logs.parts_demo FINAL;
SELECT name, rows, active FROM system.parts WHERE database = 'logs' AND table = 'parts_demo' AND active;
SYSTEM FLUSH LOGS;
SELECT event_type, count() FROM system.part_log WHERE database = 'logs' AND table = 'parts_demo' GROUP BY event_type;
```

**보여야 하는 것.** 2026-10-19 실측. 처음에는 `all_1_1_0`, `all_2_2_0`, `all_3_3_0`(각 1행). `OPTIMIZE` 뒤 활성 part는 `all_1_3_1`(3행) 하나. `part_log`에 `NewPart 3`, `MergeParts 1`, `MergePartsStart 1`.

**생각해 볼 것.** 1행씩 1만 번 넣으면 part는 몇 개가 생기는가? ClickHouse는 그것을 어떻게 처리하는가?

## H3. 로그 테이블 설계 보기

**할 것.**

```sql
SHOW CREATE TABLE logs.app_logs;
```

**보여야 하는 것.** `ENGINE = ReplacingMergeTree`, `PARTITION BY toYYYYMMDD(event_time)`, `ORDER BY (device_id, source, offset)`, `TTL toDateTime(event_time) + toIntervalDay(7) RECOMPRESS CODEC(ZSTD(9)), toDateTime(event_time) + toIntervalDay(30)`. 각 선택의 이유는 `study/09` 1절이다. 두 번째 TTL 끝에 `DELETE`가 없는 것은 지운 게 아니다. `DELETE`가 기본 동작이라 ClickHouse가 다시 보여 줄 때 생략한다(2026-10-19 확인). `INTERVAL 7 DAY`도 같은 뜻의 `toIntervalDay(7)`로 바뀌어 보인다.

**생각해 볼 것.** 정렬 키를 멱등키와 같게 둔 이유는?

## H4. 같은 줄이 두 번 오면

**왜 하는가.** ReplacingMergeTree가 멱등키로 중복을 지우는 것을 본다. 운영 테이블을 더럽히지 않도록 같은 구조의 연습 테이블에 가짜 행을 넣는다.

**할 것.**

```sql
CREATE TABLE logs.dedup_demo AS logs.app_logs;
INSERT INTO logs.dedup_demo (device_id, source, event_time, collected_at, offset, raw, category, level, message)
VALUES ('TEST-0001', '/var/log/devices/TEST-0001/UILogger/APP_20261019_15.log', now64(3), now64(6), 0,
        '2026-10-19 15:00:00,000 [1] INFO APP - example', 'APP', 'INFO', 'example');
```

같은 INSERT를 한 번 더 실행한 뒤:

```sql
SELECT count() FROM logs.dedup_demo;
SELECT count() FROM logs.dedup_demo FINAL;
OPTIMIZE TABLE logs.dedup_demo FINAL;
SELECT count() FROM logs.dedup_demo;
```

**보여야 하는 것.** 2, 1, (OPTIMIZE), 1. 2026-10-19 실측과 같다.

**생각해 볼 것.** 병합 전의 `count()`가 2인 것은 문제인가? 어떤 화면에서는 문제이고 어떤 화면에서는 아닌가?

## H5. TTL이 지우는 것 보기

**왜 하는가.** 보존 정책은 며칠을 기다려야 보이므로 1분짜리로 본다.

**할 것.**

```sql
CREATE TABLE logs.ttl_demo (t DateTime DEFAULT now(), msg String)
ENGINE = MergeTree ORDER BY t
TTL t + INTERVAL 1 MINUTE DELETE
SETTINGS merge_with_ttl_timeout = 60;
INSERT INTO logs.ttl_demo (msg) VALUES ('expires in a minute');
INSERT INTO logs.ttl_demo (t, msg) VALUES (now() + INTERVAL 1 HOUR, 'stays');
SELECT count() FROM logs.ttl_demo;
```

2분쯤 기다린 뒤 다시 `SELECT count() FROM logs.ttl_demo;`

**보여야 하는 것.** 처음 2, 2분 뒤 1. 2026-10-19에는 15:30:40에 넣고 15:32:13에 1이었다. 아직 2면 1분 더 기다리거나 `OPTIMIZE TABLE logs.ttl_demo FINAL`로 병합을 일으킨다.

**생각해 볼 것.** TTL은 만료 순간에 지우지 않고 병합 때 지운다. 운영 테이블(기본 `merge_with_ttl_timeout` 4시간)에서는 무엇을 뜻하는가?

## H6. 적재 컨슈머 붙이기

**왜 하는가.** Kafka의 로그가 ClickHouse로 들어가는 것을 본다.

**할 것.**

1. 앱 터미널에서 jar를 만들고 띄운다(IntelliJ로 띄워도 된다). 이번에는 단계 인자 없이 띄워 적재 컨슈머를 켠다. 대신 로그 인자 셋을 붙인다. 이 단계에서는 줄마다 찍히는 발행 성공(DEBUG)과 consume(INFO) 로그가 수십만 줄이 되어 적재까지 느려지기 때문이다(아래 "보여야 하는 것"의 시간 비교).

```powershell
$env:JAVA_HOME = "C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.4\jbr"; .\gradlew.bat bootJar
& "$env:JAVA_HOME\bin\java.exe" -jar build\libs\practice-kafka-0.0.1-SNAPSHOT.jar --logging.level.com.practice.kafka.producer=INFO --logging.level.com.practice.kafka.consumer=WARN --logging.level.com.practice.kafka.ingest=INFO
```

2. 명령 터미널에서:

```powershell
Invoke-RestMethod http://localhost:8080/api/sink/stats
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate?lines=1000"; Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:8080/api/sink/stats
docker exec clickhouse clickhouse-client --user app --password app --query "SELECT level, count() FROM logs.app_logs GROUP BY level ORDER BY level"
```

**보여야 하는 것.** 앱 콘솔에 `clickhouse-sink: partitions assigned: [app-logs-0, ... app-logs-5]`(스레드 하나가 6개 모두). 앱이 뜨자마자 토픽에 쌓여 있던 과거분이 들어간다. 2026-10-19 첫 검증에서는 123,403건이 INSERT 8번(`rowsPerInsert` 15,425)으로 들어갔다. 00 파일부터 깨끗하게 다시 한 날에는 B~G절에서 쌓인 485,556건이 INSERT 26번(`rowsPerInsert` 18,675)으로 들어갔다. 대부분 C절의 `generate`가 보낸 것이다. 1,000줄을 흘린 뒤 `rows`가 1,000 늘고(INSERT 2번) `lag`는 0이다. 레벨별 건수가 보인다.

대외비 생성기 로그가 있으면(`tools/OHT-0001` 등) 실제 경로로도 흘려 본다. `docker/devices`는 gitignore 대상이다.

```powershell
Copy-Item -Recurse tools\OHT-0001 docker\devices\OHT-0001
# 30초쯤 뒤
docker exec clickhouse clickhouse-client --user app --password app --query "SELECT count(), uniqExact(source, offset) FROM logs.app_logs WHERE device_id = 'OHT-0001'"
```

2026-10-19에는 약 20초 뒤 두 값이 모두 351,327이었다. 같은 날 깨끗한 환경에서 다시 할 때 로그 인자 없이 띄웠더니 같은 351,327줄이 모두 들어가기까지 약 2분 걸렸다. Fluent Bit은 3초 만에 다 읽었지만, 앱이 줄마다 콘솔에 두 줄씩 모두 158MB를 찍느라 발행이 밀렸다. 로그 인자를 붙여 다시 띄우고 같은 크기의 입력을 흘리자 17초였다. 03 파일 C0에서 본 "콘솔 로그가 전송을 늦춘다"가 실제 경로에서도 그대로 나타난 것이다.

**안 될 때.** `/api/sink/stats`가 404면 jar를 이 단계의 코드로 다시 만들지 않았거나 `app.sink.enabled=false`다. `failures`가 계속 늘면 ClickHouse가 떠 있는지(`docker ps`), 테이블이 있는지(H1) 본다. 앱 콘솔에 `ClickHouse INSERT 실패`가 1초마다 찍힌다.

**생각해 볼 것.** 앱을 처음 띄우자 과거분이 한꺼번에 들어간 이유는? `rowsPerInsert`가 과거분일 때와 1,000줄을 흘릴 때 다른 이유는?

## H7. ClickHouse가 멈추면

**왜 하는가.** 저장소 장애 동안 로그가 어디서 기다리는지 본다.

**할 것.**

```powershell
docker stop clickhouse
Invoke-RestMethod -Method Post "http://localhost:8080/api/logs/simulate?lines=20000"; Start-Sleep -Seconds 20
Invoke-RestMethod http://localhost:8080/api/logs/stats       # 수집 서버는 계속 받는다
Invoke-RestMethod http://localhost:8080/api/sink/stats       # lag 과 failures
docker start clickhouse; Start-Sleep -Seconds 10
Invoke-RestMethod http://localhost:8080/api/sink/stats       # lag 0
```

**보여야 하는 것.** 2026-10-19 실측. 멈춘 동안 `receivedFromFluentBit`은 2만 늘고, `/api/sink/stats`는 `lag` 20,000, `failures` 29(1초마다 재시도). 다시 켠 지 2초 안에 `lag` 0. 그 2만 줄은 정확히 2만 행으로 들어갔고 중복은 없었다. 깨끗한 환경에서 다시 한 결과도 같았다(`failures` 17, 다시 켠 지 2.1초 만에 `lag` 0, 2만 행에 고유 멱등키 2만). `failures`는 멈춰 있던 시간에 따라 달라진다. Kafka UI → Consumers → clickhouse-sink 에서도 lag이 쌓였다 줄어드는 것이 보인다.

**생각해 볼 것.** 수집 서버가 ClickHouse에 직접 넣었다면 그 2만 줄은 어디서 기다렸을까?

## H8. Grafana

**할 것.** 브라우저로 http://localhost:3000 → 왼쪽 메뉴 Dashboards → practice-kafka → "장비 로그 (ClickHouse)". 오른쪽 위 기간을 "Last 30 days"와 "Last 15 minutes"로 바꿔 본다. H6~H7을 다시 하면서 10초마다 갱신되는 것을 본다.

**보여야 하는 것.** 다섯 패널(저장된 행 수, 레벨별 로그 수, ERROR 많은 장비 상위 10, 수집 → 저장 지연, 최근 ERROR). 2026-10-19에 다섯 쿼리 모두 데이터를 돌려주는 것을 Grafana API로 확인했다(깨끗한 환경에서 다시 한 뒤에도 같음). 지연 패널은 평시 흐름에서 p50 1초 안쪽이다(2초마다 1,000줄일 때 p50 0.67초, p99 1.17초).

**안 될 때.** 패널이 "No data"면 기간을 넓힌다. 대외비 로그는 생성한 날짜에 찍혀 있다. 데이터 소스 오류면 왼쪽 메뉴 Connections → Data sources → ClickHouse → "Save & test"의 메시지를 본다.

## H9. 정리

연습 테이블을 지운다. 운영 테이블(`app_logs`)은 남긴다.

```powershell
docker exec clickhouse clickhouse-client --user app --password app --multiquery --query "DROP TABLE IF EXISTS logs.parts_demo; DROP TABLE IF EXISTS logs.dedup_demo; DROP TABLE IF EXISTS logs.ttl_demo"
```

앱은 정상 종료한다(`Invoke-RestMethod -Method Post http://localhost:8080/actuator/shutdown -ContentType application/json`).

## 다 됐으면

워크북 H절을 채운다. 다음은 [08-적재-경로-비교.md](08-적재-경로-비교.md)(I절, Step 4+)다.
