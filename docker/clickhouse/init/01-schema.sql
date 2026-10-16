-- =============================================================================
-- 로그 저장 테이블. ClickHouse 컨테이너가 빈 데이터 디렉터리로 처음 뜰 때 한 번 실행된다.
-- 다시 만들려면 볼륨을 지운다: docker compose -f docker/docker-compose.yml -f docker/docker-compose.loadtest.yml down -v
-- (kafka 볼륨도 함께 지워지므로 주의. clickhouse 만 지우려면 docker volume rm practice-kafka_clickhouse-data)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.app_logs
(
    -- 명세서 공통 스키마 6필드 (LogEvent 와 같은 이름, snake_case)
    device_id    LowCardinality(String),          -- 장비 수천 개 수준이라 사전 인코딩(LowCardinality)이 유리하다
    source       String,
    event_time   DateTime64(3, 'UTC'),            -- 수집 서버가 UTC 로 정규화해 보낸다
    collected_at DateTime64(6, 'UTC'),            -- Fluent Bit 이 마이크로초까지 찍는다. 없으면 수집 서버가 받은 시각
    `offset`     Int64,                           -- 파일 내 바이트 위치. 값이 없으면 수집 서버가 -1 을 넣는다
    raw          String,
    -- 파싱 필드
    category     LowCardinality(Nullable(String)),
    level        LowCardinality(Nullable(String)),
    message      String,
    -- 이 행이 ClickHouse 에 기록된 시각. 적재 컨슈머가 값을 비워 보내면(RowBinaryWithDefaults) 서버가 채운다.
    -- inserted_at - collected_at 이 "수집 서버가 받은 뒤 저장되기까지" 걸린 시간이다.
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- 같은 정렬 키를 가진 행은 백그라운드 병합 때 하나만 남는다. 정렬 키 = 멱등키 (device_id, source, offset) 이므로
-- at-least-once 재전송으로 생긴 중복이 결국 사라진다. 병합 전에는 남아 있을 수 있어 건수는 uniqExact 나 FINAL 로 센다.
ENGINE = ReplacingMergeTree
-- 하루 단위 파티션. INSERT 한 번이 여러 날짜에 걸치면 날짜마다 part 가 하나씩 생긴다.
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (device_id, source, `offset`);
