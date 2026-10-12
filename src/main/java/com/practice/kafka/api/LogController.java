package com.practice.kafka.api;

import com.practice.kafka.consumer.LogEventConsumer;
import com.practice.kafka.ingest.FluentBitIngestController;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import com.practice.kafka.producer.LogEventProducer;
import com.practice.kafka.producer.LogGenerator;
import com.practice.kafka.simulator.DeviceLogSimulator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * 학습·실험용 REST API. (운영 경로인 /ingest/fluentbit 은 {@link FluentBitIngestController} 에 있다)
 *
 * <ul>
 *   <li>POST /api/logs                       : 원문 한 줄을 직접 정규화해 발행 (정규화 결과를 응답으로 확인)</li>
 *   <li>POST /api/logs/generate?count=N      : Kafka 에 직접 N건 발행 (Kafka 처리량 실험)</li>
 *   <li>POST /api/logs/simulate?lines=N      : 장비 로그 파일에 N줄 기록 (Fluent Bit 수집 경로 실험)</li>
 *   <li>GET  /api/logs/stats                 : 수신/소비 건수</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/logs")
public class LogController {

    private final LogEventProducer producer;
    private final LogGenerator generator;
    private final LogEventConsumer consumer;
    private final LogNormalizer normalizer;
    private final DeviceLogSimulator simulator;
    private final FluentBitIngestController ingest;

    public LogController(LogEventProducer producer, LogGenerator generator, LogEventConsumer consumer,
                         LogNormalizer normalizer, DeviceLogSimulator simulator, FluentBitIngestController ingest) {
        this.producer = producer;
        this.generator = generator;
        this.consumer = consumer;
        this.normalizer = normalizer;
        this.simulator = simulator;
        this.ingest = ingest;
    }

    public record PublishRequest(@NotBlank String deviceId, @NotBlank String raw) {}

    @PostMapping
    public ResponseEntity<LogEvent> publish(@Valid @RequestBody PublishRequest req) {
        LogEvent event = normalizer.normalize("manual://" + req.deviceId() + ".log", -1, req.raw(), Instant.now());
        producer.send(event);   // 비동기 발행. 응답은 브로커 ack 를 기다리지 않고 바로 돌려준다.
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(event);
    }

    @PostMapping("/generate")
    public Map<String, Object> generate(@RequestParam(defaultValue = "100") int count,
                                        @RequestParam(defaultValue = "50") int devices) {
        long elapsedMs = generator.generate(count, devices);
        return Map.of("requested", count, "devices", devices, "elapsedMs", elapsedMs);
    }

    @PostMapping("/simulate")
    public Map<String, Object> simulate(@RequestParam(defaultValue = "100") int lines) {
        int written = simulator.writeBurst(lines);
        return Map.of("written", written);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of(
                "receivedFromFluentBit", ingest.receivedCount(),
                "consumed", consumer.consumedCount());
    }
}
