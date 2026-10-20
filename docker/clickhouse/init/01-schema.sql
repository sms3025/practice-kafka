-- =============================================================================
-- 로그 저장 테이블 (Step 4). ClickHouse 컨테이너가 빈 데이터 디렉터리로 처음 뜰 때 한 번 실행된다.
-- 다시 만들려면 볼륨만 지운다: docker compose -f docker/docker-compose.yml rm -sf clickhouse
--                              docker volume rm practice-kafka_clickhouse-data
-- 설계 설명은 docs/study/09-ClickHouse-적재와-멱등키.md
-- =============================================================================

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.app_logs
(
    -- 명세서 공통 스키마 6필드 (LogEvent 와 같은 이름, snake_case)
    device_id    LowCardinality(String),          -- 장비 수천 개 수준이라 사전 인코딩(LowCardinality)이 유리하다
    source       String,
    event_time   DateTime64(3, 'UTC'),            -- 수집 서버가 UTC 로 정규화해 보낸다
    collected_at DateTime64(6, 'UTC'),            -- Fluent Bit 이 마이크로초까지 찍는다
    `offset`     Int64,                           -- 파일 내 바이트 위치. 값이 없으면 수집 서버가 -1 을 넣는다
    raw          String,
    -- 파싱 필드
    category     LowCardinality(Nullable(String)),
    level        LowCardinality(Nullable(String)),
    message      String,
    -- 이 행이 ClickHouse 에 기록된 시각. 적재 컨슈머가 값을 비워 보내면(RowBinaryWithDefaults) 서버가 채운다.
    -- inserted_at - collected_at 이 "에이전트가 읽은 뒤 저장되기까지" 걸린 시간이다.
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- 같은 정렬 키를 가진 행은 백그라운드 병합 때 하나만 남는다. 정렬 키 = 멱등키 (device_id, source, offset) 이므로
-- at-least-once 재전송으로 생긴 중복이 결국 사라진다. 병합 전에는 남아 있을 수 있어 정확한 건수는 FINAL 이나 uniqExact 로 센다.
ENGINE = ReplacingMergeTree
-- 하루 단위 파티션. 보존 기간이 지난 날은 파티션째 지울 수 있고, 하루치 조회는 그 파티션만 읽는다.
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (device_id, source, `offset`)
-- 보존 정책(명세서: N일 후 압축 보관, M일 후 삭제). 연습값 N=7, M=30. 실제 값은 아직 정해지지 않았다(계획서 "열려 있는 것").
-- 7일이 지난 part 는 병합 때 더 강한 압축(ZSTD(9))으로 다시 쓰고, 30일이 지난 행은 지운다. 기본 압축은 LZ4 다.
-- TTL 이 실제로 지우는 모습은 몇 분짜리 TTL 을 건 연습 테이블로 본다(docs/practice/07-ClickHouse-적재.md H5).
TTL toDateTime(event_time) + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(9)),
    toDateTime(event_time) + INTERVAL 30 DAY DELETE;
