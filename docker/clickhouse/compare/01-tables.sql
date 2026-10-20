-- =============================================================================
-- Step 4+ 적재 경로 비교용 테이블. 컨테이너가 처음 뜰 때 자동으로 돌지 않는다. 앱이 토픽(app-logs)을 만든 뒤에 직접 실행한다.
--   docker exec clickhouse clickhouse-client --user app --password app --queries-file /compare/01-tables.sql
-- 설명은 docs/study/11-적재-경로-비교.md, 절차는 docs/practice/08-적재-경로-비교.md
--
-- 같은 장비 로그가 네 경로로 네 테이블에 들어간다. 테이블 구조는 모두 logs.app_logs 와 같고, 들어오는 방법만 다르다.
--   app_logs              : Fluent Bit → /ingest/fluentbit → Kafka → 앱의 적재 컨슈머(sink/) → INSERT     (Step 4 경로)
--   app_logs_kafka_engine : 같은 Kafka 토픽 → ClickHouse 가 직접 읽는 Kafka 테이블 엔진 → 구체화 뷰 → INSERT
--   app_logs_direct       : 두 번째 Fluent Bit → /ingest/direct → HTTP 묶음 하나 = INSERT 한 번
--   app_logs_direct_async : 세 번째 Fluent Bit → /ingest/direct?async=true → ClickHouse 가 여러 INSERT 를 모아 part 하나로
--
-- 비교를 같은 출발선에서 하려면 docker compose down -v 로 모두 비운 뒤 시작한다. 끝나면 99-drop.sql 로 지운다.
-- 다시 실행해도 이미 있는 테이블은 건드리지 않는다(IF NOT EXISTS).
-- =============================================================================

CREATE TABLE IF NOT EXISTS logs.app_logs_direct       AS logs.app_logs;
CREATE TABLE IF NOT EXISTS logs.app_logs_direct_async AS logs.app_logs;
CREATE TABLE IF NOT EXISTS logs.app_logs_kafka_engine AS logs.app_logs;

-- Kafka 테이블 엔진. 이 테이블은 데이터를 저장하지 않는다. ClickHouse 안에서 Kafka 컨슈머가 돌며 토픽을 읽고,
-- 읽은 묶음을 아래 구체화 뷰(MATERIALIZED VIEW)에 넘긴다. 앱의 적재 컨슈머와 하는 일이 같고, 사는 곳만 ClickHouse 안이다.
-- 컬럼 이름은 토픽에 든 JSON(LogEvent)의 필드 이름과 같아야 한다(ClickHouseWriterColumnsTest 가 확인한다).
-- 시각은 JSON 에 "2026-10-19T01:02:03.456Z" 같은 글자로 들어 있어 String 으로 받고, 구체화 뷰에서 시각으로 바꾼다.
CREATE TABLE IF NOT EXISTS logs.app_logs_kafka_queue
(
    device_id    String,
    source       String,
    event_time   String,
    collected_at String,
    `offset`     Int64,
    raw          String,
    category     Nullable(String),
    level        Nullable(String),
    message      String
)
ENGINE = Kafka
SETTINGS
    -- 컨테이너 네트워크 안의 리스너(PLAINTEXT, kafka:19092). ClickHouse 는 docker 안에 있으므로 localhost:9092 가 아니다
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'app-logs',
    -- 앱의 그룹(log-printer, log-stats, clickhouse-sink)과 다른 그룹이라 같은 메시지를 따로 전부 받는다.
    -- 이 그룹에 저장된 오프셋이 없으면 토픽의 처음부터 읽는다
    kafka_group_name = 'clickhouse-kafka-engine',
    kafka_format = 'JSONEachRow',
    -- ClickHouse 안에서 돌릴 컨슈머 수. 파티션(6) 이하여야 하고, 공식 문서는 CPU 코어 수 이하를 권한다
    kafka_num_consumers = 1;
    -- 읽은 것을 구체화 뷰로 넘기는 주기는 kafka_flush_interval_ms(기본값 stream_flush_interval_ms = 7500ms)다.
    -- 묶음이 크고 part 가 적은 대신, 저장까지 최대 7.5초 남짓 늦는다. 이 손잡이를 일부러 기본값으로 두고 잰다.

-- 구체화 뷰: 큐 테이블이 넘긴 묶음을 변환해 저장 테이블에 INSERT 한다. 앱의 ClickHouseWriter 가 하는 일을 SQL 로 쓴 것이다.
-- inserted_at 은 고르지 않으므로 저장 테이블의 DEFAULT now64(3) 이 들어간다.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs.app_logs_kafka_engine_mv TO logs.app_logs_kafka_engine AS
SELECT device_id,
       source,
       parseDateTime64BestEffort(event_time, 3, 'UTC')   AS event_time,
       parseDateTime64BestEffort(collected_at, 6, 'UTC') AS collected_at,
       `offset`,
       raw,
       category,
       level,
       message
FROM logs.app_logs_kafka_queue;
