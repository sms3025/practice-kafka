package com.practice.kafka.api;

import com.practice.kafka.consumer.LogEventConsumer;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.producer.LogEventProducer;
import com.practice.kafka.producer.LogGenerator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 로그 이벤트를 발행하기 위한 REST API.
 *
 * <ul>
 *   <li>POST /api/logs                 : 로그 한 건을 직접 발행</li>
 *   <li>POST /api/logs/generate?count=N: 무작위 로그 N건을 빠르게 발행 (부하 실험용)</li>
 *   <li>GET  /api/logs/stats           : 이 인스턴스가 소비한 건수 확인</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/logs")
public class LogController {

    private final LogEventProducer producer;
    private final LogGenerator generator;
    private final LogEventConsumer consumer;

    public LogController(LogEventProducer producer, LogGenerator generator, LogEventConsumer consumer) {
        this.producer = producer;
        this.generator = generator;
        this.consumer = consumer;
    }

    public record PublishRequest(
            @NotBlank String service,
            @NotNull LogEvent.LogLevel level,
            @NotBlank String message,
            String host
    ) {}

    @PostMapping
    public ResponseEntity<LogEvent> publish(@Valid @RequestBody PublishRequest req) {
        LogEvent event = LogEvent.of(req.service(), req.level(), req.message(),
                req.host() == null ? "manual" : req.host());
        producer.send(event);   // 비동기 발행. 응답은 브로커 ack를 기다리지 않고 바로 돌려준다.
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(event);
    }

    @PostMapping("/generate")
    public Map<String, Object> generate(@RequestParam(defaultValue = "100") int count) {
        long elapsedMs = generator.generate(count);
        return Map.of("requested", count, "elapsedMs", elapsedMs);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of("consumed", consumer.consumedCount());
    }
}
