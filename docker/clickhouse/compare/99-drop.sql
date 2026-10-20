-- =============================================================================
-- Step 4+ 비교 테이블을 지운다. 큐 테이블(Kafka 엔진)을 지워야 ClickHouse 안의 컨슈머가 멈춘다.
--   docker exec clickhouse clickhouse-client --user app --password app --queries-file /compare/99-drop.sql
-- Kafka 쪽 그룹(clickhouse-kafka-engine)의 오프셋은 남는다. 다시 만들면 그 오프셋부터 이어 읽는다.
-- logs.app_logs(Step 4 테이블)는 건드리지 않는다.
-- =============================================================================

DROP VIEW  IF EXISTS logs.app_logs_kafka_engine_mv;
DROP TABLE IF EXISTS logs.app_logs_kafka_queue;
DROP TABLE IF EXISTS logs.app_logs_kafka_engine;
DROP TABLE IF EXISTS logs.app_logs_direct;
DROP TABLE IF EXISTS logs.app_logs_direct_async;
