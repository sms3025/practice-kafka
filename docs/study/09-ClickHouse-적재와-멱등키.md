# 09. ClickHouse 적재와 멱등키: 테이블 설계부터 배치 리스너까지

08장에서 INSERT 한 번이 part 하나라는 것을 봤다. 이 장은 그것을 바탕으로 로그 테이블을 설계하고, Kafka에서 읽어 넣는 적재 컨슈머(`sink/` 패키지)를 붙인다. 중복을 어떻게 지우는지(멱등키), 오래된 로그를 어떻게 줄이고 지우는지(TTL), ClickHouse가 멈추면 로그가 어디서 기다리는지를 차례로 확인한다. 따라 하는 절차는 `../practice/07-ClickHouse-적재.md` H3~H7이다.

## 1. 테이블 설계

`docker/clickhouse/init/01-schema.sql`이 ClickHouse가 처음 뜰 때 만드는 테이블이다.

```sql
CREATE TABLE IF NOT EXISTS logs.app_logs
(
    device_id    LowCardinality(String),
    source       String,
    event_time   DateTime64(3, 'UTC'),
    collected_at DateTime64(6, 'UTC'),
    `offset`     Int64,
    raw          String,
    category     LowCardinality(Nullable(String)),
    level        LowCardinality(Nullable(String)),
    message      String,
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (device_id, source, `offset`)
TTL toDateTime(event_time) + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(9)),
    toDateTime(event_time) + INTERVAL 30 DAY DELETE;
```

컬럼은 `LogEvent`와 같은 이름이다(02장의 명세서 스키마). 몇 가지 선택의 이유는 다음과 같다.

**`LowCardinality`는 값의 가짓수가 적은 문자열에 쓴다.** 장비 ID는 수천 개, 레벨은 다섯 개 남짓이다. ClickHouse는 이런 컬럼을 사전(값 → 번호)과 번호 목록으로 저장해 크기와 비교 비용을 줄인다. `source`도 가짓수가 적지만 장비 수 × 파일 수로 늘어나므로 일반 `String`으로 두었다(정렬 키에 있어 압축은 어차피 잘 된다).

**`DateTime64(3, 'UTC')`는 밀리초까지의 UTC 시각이다.** 수집 서버가 이미 UTC로 정규화해 보낸다(02장). 화면에 한국 시각으로 보이게 하는 것은 조회 쪽의 일이다.

**`inserted_at`은 서버가 채운다.** 적재 컨슈머가 이 값을 비워 보내면 ClickHouse가 기록하는 순간의 시각을 넣는다. 그래서 `inserted_at - collected_at`이 "에이전트가 읽은 뒤 저장되기까지" 걸린 시간이 되고, 10장의 대시보드가 이것을 그린다.

**정렬 키 = 멱등키.** `ORDER BY (device_id, source, offset)`은 02장에서 정한 멱등키 그대로다. 이것이 다음 절의 중복 제거를 가능하게 한다. 장비별 조회(`WHERE device_id = ...`)가 빠른 것은 덤이다.

**일 단위 파티션.** 하루치가 한 파티션이라 보존 기간이 지난 날을 통째로 다룰 수 있다. 한 번의 INSERT가 이틀에 걸치면 날짜마다 part가 생긴다.

## 2. 같은 줄이 두 번 오면: ReplacingMergeTree

Fluent Bit은 2xx를 못 받으면 같은 묶음을 다시 보내고(Step 2), 적재 컨슈머는 INSERT 뒤 커밋 전에 죽으면 같은 묶음을 다시 넣는다(이 장 4절). 둘 다 at-least-once라서 같은 줄이 두 번 들어올 수 있다.

ReplacingMergeTree는 **병합할 때** 같은 정렬 키를 가진 행 중 하나만 남긴다. 연습 테이블로 확인한다(가짜 행, 2026-10-19 실측).

```sql
CREATE TABLE logs.dedup_demo AS logs.app_logs;
-- 멱등키가 같은 행을 두 번 넣는다
INSERT INTO logs.dedup_demo (device_id, source, event_time, collected_at, offset, raw, category, level, message)
VALUES ('TEST-0001', '/var/log/devices/TEST-0001/UILogger/APP_20261019_15.log', now64(3), now64(6), 0,
        '2026-10-19 15:00:00,000 [1] INFO APP - example', 'APP', 'INFO', 'example');   -- 같은 것을 한 번 더
SELECT count() FROM logs.dedup_demo;          -- 2  (병합 전에는 둘 다 있다)
SELECT count() FROM logs.dedup_demo FINAL;    -- 1  (FINAL 은 읽을 때 중복을 접는다)
OPTIMIZE TABLE logs.dedup_demo FINAL;
SELECT count() FROM logs.dedup_demo;          -- 1  (병합이 하나를 지웠다)
```

중요한 점은 "언젠가 병합될 때" 지운다는 것이다. 병합 전에는 중복이 보이므로, 정확한 건수가 필요하면 `FINAL`을 붙이거나 `uniqExact(device_id, source, offset)`로 센다. 대시보드처럼 대략의 추세만 보면 될 때는 그냥 센다.

반대로 멱등키가 줄마다 유일하지 않으면 다른 로그가 지워진다. 연습용 부하 생성기(`LogGenerator`)는 앱을 켤 때마다 가짜 오프셋을 0부터 다시 매긴다. 그래서 적재 컨슈머가 토픽의 과거분 123,403건을 넣었더니 테이블에는 123,198행만 남았다(2026-10-19). 다른 메시지 205건이 같은 키라서 "중복"으로 지워진 것이다. Fluent Bit의 파일 오프셋처럼 실제로 유일한 값을 키로 써야 하는 이유다.

그런데 그 파일 오프셋도 언제나 유일하지는 않았다. Step 4+ 측정 중(2026-10-19) Fluent Bit 4.2.8은 재시작한 뒤 이어 읽은 줄의 `offset`을 실제 파일 위치가 아니라 0부터 다시 셌다. 재시작 전에 읽은 다른 줄과 키가 겹친 4.6만 줄(재시작 뒤 줄의 1.6%)이 병합 때 "중복"으로 지워질 처지가 됐다. 5.1.4에서는 고쳐져 있다. 자세한 것은 11장 6절이고, 고칠 방법(에이전트 버전, 키 구성)은 계획서의 열린 항목이다.

## 3. 보존 정책: TTL

명세서는 "N일 후 압축 보관, M일 후 삭제"를 요구한다. 값은 아직 정해지지 않아(계획서의 "열려 있는 것") 연습값 N=7, M=30을 썼다. 7일이 지난 part는 병합 때 더 강한 압축(ZSTD 레벨 9)으로 다시 쓰고, 30일이 지난 행은 지운다. 기본 압축은 LZ4다. 두 압축의 실제 크기 차이는 `../result/benchmark-storage.md`에 있다(중복 없는 실제 로그로 LZ4는 한 행 61.4바이트, ZSTD(9)는 28.5바이트).

TTL이 실제로 지우는 모습은 며칠을 기다릴 수 없으므로 1분짜리 연습 테이블로 본다.

```sql
CREATE TABLE logs.ttl_demo (t DateTime DEFAULT now(), msg String)
ENGINE = MergeTree ORDER BY t
TTL t + INTERVAL 1 MINUTE DELETE
SETTINGS merge_with_ttl_timeout = 60;               -- TTL 병합을 60초마다 허용 (기본 4시간)
INSERT INTO logs.ttl_demo (msg) VALUES ('expires in a minute');
INSERT INTO logs.ttl_demo (t, msg) VALUES (now() + INTERVAL 1 HOUR, 'stays');
```

2026-10-19에 15:30:40에 넣고 15:32:13에 세었더니 1행만 남아 있었다. 만료된 행은 `OPTIMIZE` 없이 백그라운드 TTL 병합이 지웠다. TTL은 "시각이 되는 순간" 지우는 것이 아니라 "그 뒤의 병합에서" 지운다. 운영 테이블의 기본값(`merge_with_ttl_timeout` 4시간)에서는 만료 후 몇 시간까지 남아 있을 수 있다.

## 4. 적재 컨슈머: 배치 리스너

`sink/ClickHouseSinkListener.java`가 세 번째 컨슈머 그룹(`clickhouse-sink`)으로 `app-logs`를 읽는다.

```java
@KafkaListener(id = "clickhouse-sink", topics = "${app.sink.topic}", groupId = "${app.sink.group}",
        containerFactory = "sinkContainerFactory",
        properties = {"max.poll.records=${app.sink.max-poll-records}", "fetch.min.bytes=${app.sink.fetch-min-bytes}",
                      "fetch.max.wait.ms=${app.sink.fetch-max-wait-ms}", "max.partition.fetch.bytes=${app.sink.max-partition-fetch-bytes}"})
public void onBatch(List<LogEvent> events) {
    writer.insert(events);      // 묶음 전체를 INSERT 한 번으로
}
```

`log-printer`와 다른 점은 셋이다.

**메시지 한 건이 아니라 묶음을 받는다.** `SinkConfig`의 팩토리가 `setBatchListener(true)`라서 `poll()` 한 번에 받은 것을 통째로 넘긴다. 묶음 하나가 INSERT 한 번, 즉 part 하나다. 묶음이 클수록 part가 적게 생긴다.

**묶음 크기를 설정으로 키운다.** 기본 `max.poll.records`(500)면 INSERT가 너무 잦다. 20,000으로 올리고, 로그가 띄엄띄엄 올 때도 묶음이 작게 쪼개지지 않도록 브로커가 1MB가 모일 때까지(최대 0.5초) 기다렸다 주게 했다(`fetch.min.bytes`, `fetch.max.wait.ms`). 2026-10-19 실측에서 쌓여 있던 과거분 123,403건은 INSERT 8번(한 번 평균 15,425행)에 들어갔다. 실제 로그 35만 줄이 흘러올 때는 INSERT 188번, 한 번에 약 1,870행이었다. 1MB를 메시지 하나(약 546바이트)로 나눈 값과 비슷하다. 2초마다 1,000줄씩 띄엄띄엄 올 때는 1MB가 차기 전에 0.5초가 지나 한 번에 약 700행이었다. 대신 저장이 최대 0.5초 늦어진다. 묶음 크기와 지연을 맞바꾸는 손잡이다.

**ClickHouse 형식으로 바꿔 보낸다.** `ClickHouseWriter`가 공식 Java 클라이언트(client-v2)로 묶음을 RowBinaryWithDefaults라는 바이너리 형식으로 보낸다. 문자열은 길이와 바이트, 시각은 정수로 보내므로 ClickHouse가 JSON을 해석할 필요가 없다. 값을 비운 `inserted_at`은 "기본값을 써라"라는 표시로 간다. 컬럼 이름 목록은 `ClickHouseWriter.COLUMNS` 한 곳에 있고, `ClickHouseWriterColumnsTest`가 DDL과 어긋나지 않는지 확인한다.

### 심화: 배치 리스너와 에러 핸들러 (지금은 넘어가도 된다)

`SinkConfig`는 Spring Boot가 만든 기본 팩토리와 별도로 배치용 팩토리를 만든다. 그래서 Step 3의 `spring.kafka.listener.concurrency=3`이 여기에 걸리지 않고, 적재 스레드 수를 `app.sink.concurrency`(기본 1)로 따로 정한다. 에러 핸들러는 `DefaultErrorHandler(new FixedBackOff(1000, UNLIMITED_ATTEMPTS))`다. 기본값은 10번 실패하면 그 묶음을 건너뛰는데, 저장소가 잠깐 멈추는 동안 로그를 버리게 되므로 바꿨다. 재시도와 건너뛰기, DLT는 Step 5에서 자세히 다룬다(`../concepts/02-spring-kafka.md`).

## 5. 커밋 순서와 ClickHouse 장애

오프셋은 리스너 메서드가 정상 반환한 뒤에 커밋된다(배치 리스너의 기본 AckMode.BATCH). INSERT가 실패해 예외가 나면 커밋되지 않고, 에러 핸들러가 1초 뒤 같은 묶음을 다시 넣는다. 그러니 ClickHouse가 멈춘 동안 로그는 Kafka에 남아 있고 lag만 쌓인다.

2026-10-19에 실제로 ClickHouse를 멈추고 로그 2만 줄을 흘렸다.

| 시각 | 일 | 관찰 |
|---|---|---|
| 15:32:35 | `docker stop clickhouse`, 시뮬레이터로 2만 줄 기록 | 수집 서버는 2만 줄을 모두 받았다(Fluent Bit → 수집 서버 → Kafka는 영향 없음) |
| 25초 뒤 | `/api/sink/stats` | `lag` 20,000, INSERT 실패 29회(1초마다 재시도) |
| 15:33:19 | `docker start clickhouse` | 2초 안에 `lag` 0 |
| 그 뒤 | 행 수 확인 | 그 2만 줄이 정확히 20,000행, 멱등키 20,000개. 모두 15:33:21에 기록됐다 |

수집 서버가 ClickHouse에 직접 넣었다면 2만 줄은 수집 서버의 응답 실패 → Fluent Bit의 재시도와 디스크 버퍼로 넘어갔을 것이다. Kafka를 사이에 두면 저장소의 장애가 수집까지 번지지 않고, 밀린 양이 lag 숫자 하나로 보인다. Step 4+에서 두 경로를 측정으로 비교한다.

반대로 INSERT는 성공했는데 커밋 전에 앱이 죽으면, 다시 뜬 컨슈머가 그 묶음을 한 번 더 넣는다. 그 중복이 2절의 ReplacingMergeTree가 지우는 대상이다.

## 6. 실제 로그로 끝에서 끝까지

대외비 생성기 로그 1대분(351,327줄)을 Fluent Bit이 읽는 폴더(`docker/devices/OHT-0001`, gitignore)에 복사했다. 약 20초 만에 Fluent Bit → 수집 서버 → Kafka → 적재 컨슈머 → ClickHouse를 지나 351,327행이 됐다. `count()`, `count() FINAL`, 멱등키 고유 개수가 모두 351,327로 같았다.

이때의 저장 지연은 p50 10.6초였다. Fluent Bit이 파일을 발견하자마자 35만 줄을 한꺼번에 읽고(그때가 `collected_at`) 20초에 걸쳐 나눠 보냈기 때문이다. 평시처럼 2초마다 1,000줄씩 흘렸을 때는 p50 0.67초, p99 1.17초였다(2026-10-19). 그중 최대 0.5초는 4절의 묶음 대기다.

적재 속도의 한계는 이 저장소에서 다시 재지 않았다. `kafka-load-test` 브랜치에서 같은 설계(같은 테이블, client-v2 RowBinary, 같은 poll 설정)로 잰 값이 있다. 적재 컨슈머 1개가 초당 약 14.7만 행, 3개가 29.3만 행을 넣었다(`docs/result/benchmark-transport.md` 7.3절).

## 확인 질문

1. 정렬 키를 `(event_time, device_id)`로 바꾸면 중복 제거는 어떻게 되는가? 장비별 조회는?
2. `count()`와 `count() FINAL`이 다른 값을 보일 수 있는 이유와, 대시보드에서는 어느 쪽을 쓰는 것이 맞는지 설명하라.
3. ClickHouse가 1시간 멈추면 로그는 어디에 얼마나 쌓이는가? Kafka의 보존 기간이 그보다 짧으면 무슨 일이 생기는가?
4. `fetch.min.bytes`를 1(기본값)로 되돌리면 실시간 적재에서 INSERT 한 번의 행 수와 저장 지연은 어떻게 달라질까? 바꿔서 `/api/sink/stats`로 확인하라.
5. 연습용 부하 생성기의 메시지 205건이 사라졌다. 이것은 버그인가, 설계대로인가? 무엇을 고치면 되는가?
