package com.practice.kafka.loadtest;

import com.practice.kafka.ingest.FluentBitIngestController;
import com.practice.kafka.ingest.FluentBitRecord;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 장비 → 수집 서버 구간을 쪼개 재기 위한 엔드포인트 둘. 실제 경로 {@code POST /ingest/fluentbit} 와 같은 본문을 받는다.
 *
 * <ul>
 *   <li>{@code /loadtest/ingest/receive}: HTTP 수신과 JSON 배열 읽기까지만 하고 200</li>
 *   <li>{@code /loadtest/ingest/normalize}: 위에 더해 레코드마다 {@link LogNormalizer} 까지 하고 200 (Kafka 로 보내지 않음)</li>
 * </ul>
 * 같은 부하를 세 주소에 차례로 걸면, 실제 경로의 비용을 "HTTP + JSON / 정규화 / Kafka 발행과 ack 대기"로 나눌 수 있다.
 * 앞의 둘은 Kafka 에 아무것도 쓰지 않으므로 운영에서는 쓰지 않는다.
 */
@RestController
@RequestMapping("/loadtest/ingest")
public class IngestProbeController {

    private final LogNormalizer normalizer;
    private final FluentBitIngestController ingest;
    private final LongAdder records = new LongAdder();
    /** 정규화 결과를 어딘가에 쓰지 않으면 JIT 가 계산을 통째로 지울 수 있어, 결과의 해시를 더해 둔다. */
    private final LongAdder blackhole = new LongAdder();

    public IngestProbeController(LogNormalizer normalizer, FluentBitIngestController ingest) {
        this.normalizer = normalizer;
        this.ingest = ingest;
    }

    @PostMapping("/receive")
    public ResponseEntity<Void> receive(@RequestBody List<FluentBitRecord> batch) {
        records.add(batch.size());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/normalize")
    public ResponseEntity<Void> normalize(@RequestBody List<FluentBitRecord> batch) {
        Instant now = Instant.now();
        long h = 0;
        for (FluentBitRecord r : batch) {
            LogEvent e = normalizer.normalize(r.source(), r.offset() == null ? -1 : r.offset(), r.log(),
                    r.collectedAt() == null ? now : r.collectedAt());
            h += e.message().length();
        }
        blackhole.add(h);
        records.add(batch.size());
        return ResponseEntity.ok().build();
    }

    /** 이 둘과 실제 경로가 받은 레코드 수. 부하 발생기 쪽 숫자와 맞는지 확인한다. */
    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return Map.of("probeRecords", records.sum(), "ingestRecords", ingest.receivedCount());
    }
}
