package com.practice.kafka.direct;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertSettings;
import com.practice.kafka.ingest.FluentBitRecord;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import com.practice.kafka.sink.ClickHouseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Step 4+ 비교용 수집 경로: Kafka 없이 ClickHouse 에 바로 넣는다. 명세서의 기본 아키텍처(HTTP → 수집 서버 → ClickHouse)가 이것이다.
 *
 * <ul>
 *   <li>{@code POST /ingest/direct}: HTTP 묶음 하나 = INSERT 한 번 → {@code logs.app_logs_direct}</li>
 *   <li>{@code POST /ingest/direct?async=true}: 같은 묶음을 {@code async_insert=1} 로 → {@code logs.app_logs_direct_async}.
 *       ClickHouse 가 여러 INSERT 를 서버 쪽 버퍼에 모았다가 한 part 로 쓴다</li>
 * </ul>
 *
 * <p>응답 계약은 {@code /ingest/fluentbit} 와 같다. 저장소가 기록한 뒤에 200 을 돌려주고, 실패하면 500 을 돌려 Fluent Bit 이 다시 보내게 한다.
 * 다른 점은 "기록"의 대상이 Kafka 가 아니라 ClickHouse 라는 것 하나다. 그래서 ClickHouse 가 멈추면 수집 서버의 응답이 실패하고,
 * 밀린 로그는 장비 쪽 Fluent Bit 의 디스크 버퍼에서 기다린다. Kafka 경유 경로와 이것을 같은 입력으로 비교하는 것이 Step 4+ 다.
 *
 * <p>비동기 INSERT 는 {@code wait_for_async_insert=1} 로 쓴다. 0 이면 ClickHouse 가 버퍼에 받기만 하고 바로 응답하므로,
 * 버퍼를 디스크에 쓰기 전에 ClickHouse 가 죽으면 그 묶음을 잃는다. 1 이면 실제로 기록한 뒤 응답하므로 at-least-once 계약이 지켜진다.
 */
@RestController
@ConditionalOnProperty(name = "app.compare.enabled", havingValue = "true")
public class DirectIngestController {

    private static final Logger log = LoggerFactory.getLogger(DirectIngestController.class);

    private final LogNormalizer normalizer;
    private final ClickHouseWriter direct;
    private final ClickHouseWriter directAsync;
    private final Counters directCounters = new Counters();
    private final Counters asyncCounters = new Counters();

    @Autowired
    public DirectIngestController(LogNormalizer normalizer, Client client,
                                  @Value("${app.compare.direct-table}") String directTable,
                                  @Value("${app.compare.direct-async-table}") String asyncTable) {
        this(normalizer,
                new ClickHouseWriter(client, directTable, InsertSettings::new),
                new ClickHouseWriter(client, asyncTable, () -> new InsertSettings()
                        .serverSetting("async_insert", "1")
                        .serverSetting("wait_for_async_insert", "1")));
    }

    /** 테스트에서 가짜 writer 를 넣기 위한 생성자. */
    DirectIngestController(LogNormalizer normalizer, ClickHouseWriter direct, ClickHouseWriter directAsync) {
        this.normalizer = normalizer;
        this.direct = direct;
        this.directAsync = directAsync;
    }

    @PostMapping("/ingest/direct")
    public ResponseEntity<Void> ingest(@RequestBody List<FluentBitRecord> records,
                                       @RequestParam(defaultValue = "false") boolean async) {
        Instant now = Instant.now();
        List<LogEvent> events = records.stream()
                .map(r -> normalizer.normalize(r.source(), r.offset() == null ? -1 : r.offset(), r.log(),
                        r.collectedAt() == null ? now : r.collectedAt()))
                .toList();
        ClickHouseWriter writer = async ? directAsync : direct;
        Counters counters = async ? asyncCounters : directCounters;
        try {
            writer.insert(events);
        } catch (RuntimeException e) {
            counters.failures.increment();
            log.warn("직접 적재 실패. Fluent Bit 에 재전송을 요청한다 ({}, {}건): {}", writer.table(), events.size(), e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
        counters.requests.increment();
        counters.rows.add(events.size());
        return ResponseEntity.ok().build();
    }

    /** 경로별로 성공한 요청(= INSERT) 수, 행 수, 실패(500) 수. part 와 지연은 ClickHouse 쪽 표(compare.sql)로 본다. */
    @GetMapping("/api/compare/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(direct.table(), directCounters.snapshot());
        m.put(directAsync.table(), asyncCounters.snapshot());
        return m;
    }

    private static final class Counters {
        final LongAdder requests = new LongAdder();
        final LongAdder rows = new LongAdder();
        final LongAdder failures = new LongAdder();

        Map<String, Long> snapshot() {
            Map<String, Long> m = new LinkedHashMap<>();
            m.put("inserts", requests.sum());
            m.put("rows", rows.sum());
            m.put("failures", failures.sum());
            return m;
        }
    }
}
