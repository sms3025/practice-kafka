-- =============================================================================
-- Step 4+ 적재 경로 비교 결과. 부하를 다 흘리고 경로마다 행 수가 더 늘지 않을 때 실행한다.
--   docker exec clickhouse clickhouse-client --user app --password app --format PrettyCompactMonoBlock --queries-file /compare/02-compare.sql
-- 결과 문서에는 이 숫자(개수, 크기, 시간)만 옮긴다. 행 내용(raw, message)은 옮기지 않는다.
-- =============================================================================

-- system.part_log, system.query_log 는 몇 초마다 모아서 기록된다. 방금 일까지 보려면 먼저 내려 쓴다.
SYSTEM FLUSH LOGS;

-- 1. 행 수와 멱등키 수. rows 가 시뮬레이터가 쓴 줄 수(GET /api/logs/stats 의 simulatorWritten)보다 적으면 유실이다.
--    rows - unique_keys 는 at-least-once 재전송으로 두 번 들어간 행 중 아직 병합으로 합쳐지지 않은 것이다.
SELECT * FROM (
SELECT 'app_logs' AS tbl, count() AS rows, uniqExact(device_id, source, `offset`) AS unique_keys, rows - unique_keys AS duplicates
FROM logs.app_logs
UNION ALL
SELECT 'app_logs_kafka_engine', count(), uniqExact(device_id, source, `offset`), count() - uniqExact(device_id, source, `offset`)
FROM logs.app_logs_kafka_engine
UNION ALL
SELECT 'app_logs_direct', count(), uniqExact(device_id, source, `offset`), count() - uniqExact(device_id, source, `offset`)
FROM logs.app_logs_direct
UNION ALL
SELECT 'app_logs_direct_async', count(), uniqExact(device_id, source, `offset`), count() - uniqExact(device_id, source, `offset`)
FROM logs.app_logs_direct_async
)
ORDER BY tbl;

-- 2. INSERT 가 만든 part 와 병합. 경로마다 "INSERT 한 번"의 크기가 다르므로 part 수가 크게 갈린다.
--    merge_read_rows / inserted_rows 는 한 행이 병합으로 평균 몇 번 다시 읽히고 쓰였는지다(쓰기 증폭). 작은 part 가 많을수록 커진다.
--    merge_seconds 는 병합 스레드가 쓴 시간의 합이다. 저장소가 백그라운드에서 치르는 비용이다.
SELECT table AS tbl,
       countIf(event_type = 'NewPart')                               AS new_parts,
       sumIf(rows, event_type = 'NewPart')                           AS inserted_rows,
       round(inserted_rows / new_parts)                              AS rows_per_part,
       countIf(event_type = 'MergeParts')                            AS merges,
       round(sumIf(read_rows, event_type = 'MergeParts') / inserted_rows, 2) AS merge_read_x,
       round(sumIf(duration_ms, event_type = 'MergeParts') / 1000, 1) AS merge_seconds
FROM system.part_log
WHERE database = 'logs'
  AND table IN ('app_logs', 'app_logs_kafka_engine', 'app_logs_direct', 'app_logs_direct_async')
GROUP BY tbl
ORDER BY tbl;

-- 3. 지금 살아 있는 part 수. 병합이 INSERT 를 따라가면 시간이 지나며 줄어든다.
--    파티션 하나의 active part 가 parts_to_delay_insert(기본 1000)를 넘으면 INSERT 가 느려지고, parts_to_throw_insert(기본 3000)를 넘으면
--    "Too many parts" 로 거부된다.
SELECT table AS tbl, partition, count() AS active_parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = 'logs' AND active
  AND table IN ('app_logs', 'app_logs_kafka_engine', 'app_logs_direct', 'app_logs_direct_async')
GROUP BY tbl, partition
ORDER BY tbl, partition;

-- 4. 에이전트가 줄을 읽은 뒤 ClickHouse 에 기록되기까지 걸린 시간(ms). collected_at 은 그 경로의 Fluent Bit 이 찍었고,
--    inserted_at 은 ClickHouse 가 기록할 때 찍었다. 두 시계는 같은 docker VM 의 시계다. Fluent Bit 의 Flush(1초)도 포함된다.
SELECT * FROM (
SELECT 'app_logs' AS tbl,
       quantileExact(0.5)(dateDiff('millisecond', collected_at, inserted_at))  AS p50_ms,
       quantileExact(0.99)(dateDiff('millisecond', collected_at, inserted_at)) AS p99_ms,
       max(dateDiff('millisecond', collected_at, inserted_at))                 AS max_ms
FROM logs.app_logs
UNION ALL
SELECT 'app_logs_kafka_engine',
       quantileExact(0.5)(dateDiff('millisecond', collected_at, inserted_at)),
       quantileExact(0.99)(dateDiff('millisecond', collected_at, inserted_at)),
       max(dateDiff('millisecond', collected_at, inserted_at))
FROM logs.app_logs_kafka_engine
UNION ALL
SELECT 'app_logs_direct',
       quantileExact(0.5)(dateDiff('millisecond', collected_at, inserted_at)),
       quantileExact(0.99)(dateDiff('millisecond', collected_at, inserted_at)),
       max(dateDiff('millisecond', collected_at, inserted_at))
FROM logs.app_logs_direct
UNION ALL
SELECT 'app_logs_direct_async',
       quantileExact(0.5)(dateDiff('millisecond', collected_at, inserted_at)),
       quantileExact(0.99)(dateDiff('millisecond', collected_at, inserted_at)),
       max(dateDiff('millisecond', collected_at, inserted_at))
FROM logs.app_logs_direct_async
)
ORDER BY tbl;

-- 5. 앱이 보낸 INSERT 한 번이 응답을 받기까지 걸린 시간(ms). 직접 적재에서는 이것이 곧 수집 서버의 응답 시간에 더해진다.
--    비동기 INSERT 는 wait_for_async_insert=1 이라 서버 버퍼가 비워질 때까지 기다린 시간이 들어 있다.
--    Kafka 테이블 엔진은 ClickHouse 안에서 INSERT 하므로 여기 나오지 않는다. 오류로 끝난 INSERT 는 failed 에 센다.
SELECT arrayJoin(tables)                                         AS tbl,
       countIf(type = 'QueryFinish')                             AS inserts,
       countIf(type != 'QueryFinish')                            AS failed,
       quantileExactIf(0.5)(query_duration_ms, type = 'QueryFinish')  AS p50_ms,
       quantileExactIf(0.99)(query_duration_ms, type = 'QueryFinish') AS p99_ms
FROM system.query_log
WHERE query_kind = 'Insert' AND type != 'QueryStart' AND has(databases, 'logs')
GROUP BY tbl
ORDER BY tbl;

-- 6. Kafka 테이블 엔진 컨슈머 상태. 읽은 메시지 수, 마지막 커밋 시각, 쌓인 오류 수.
SELECT table, num_messages_read, num_commits, last_commit_time, length(exceptions.text) AS exceptions
FROM system.kafka_consumers
WHERE database = 'logs';
