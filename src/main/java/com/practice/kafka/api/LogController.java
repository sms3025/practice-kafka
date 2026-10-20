package com.practice.kafka.api;

import com.practice.kafka.consumer.LogEventConsumer;
import com.practice.kafka.consumer.LogStatsConsumer;
import com.practice.kafka.ingest.FluentBitIngestController;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import com.practice.kafka.producer.LogEventProducer;
import com.practice.kafka.producer.LogGenerator;
import com.practice.kafka.simulator.DeviceLogSimulator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 학습·실험용 REST API. (운영 경로인 /ingest/fluentbit 은 {@link FluentBitIngestController} 에 있다)
 *
 * <ul>
 *   <li>POST /api/logs                       : 원문 한 줄을 직접 정규화해 발행 (정규화 결과를 응답으로 확인)</li>
 *   <li>POST /api/logs/generate?count=N      : Kafka 에 직접 N건 발행 (Kafka 처리량 실험)</li>
 *   <li>POST /api/logs/simulate?lines=N      : 장비 로그 파일에 N줄 기록 (Fluent Bit 수집 경로 실험)</li>
 *   <li>POST /api/logs/simulate/start        : 장비 N대가 초당 M줄을 S초 동안 쓰게 한다 (Step 4+ 평시 부하)</li>
 *   <li>POST /api/logs/simulate/stop         : 위 기록을 멈춘다</li>
 *   <li>GET  /api/logs/stats                 : 수신/소비 건수, log-stats 그룹의 레벨별 건수</li>
 *   <li>GET  /api/logs/assignments           : 리스너별로 컨슈머 스레드가 맡은 파티션 (Step 3 리밸런스 관찰)</li>
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
    private final LogStatsConsumer stats;
    private final KafkaListenerEndpointRegistry registry;

    public LogController(LogEventProducer producer, LogGenerator generator, LogEventConsumer consumer,
                         LogNormalizer normalizer, DeviceLogSimulator simulator, FluentBitIngestController ingest,
                         LogStatsConsumer stats, KafkaListenerEndpointRegistry registry) {
        this.producer = producer;
        this.generator = generator;
        this.consumer = consumer;
        this.normalizer = normalizer;
        this.simulator = simulator;
        this.ingest = ingest;
        this.stats = stats;
        this.registry = registry;
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

    /**
     * Step 4+ 평시 부하. 장비 devices 대가 1대당 초당 perDevice 줄씩 seconds 초 동안 쓴다.
     * 기본값은 비교 실험의 조건(장비 100대, 1대당 초당 60줄, 10분 = 360만 줄)이다.
     */
    @PostMapping("/simulate/start")
    public Map<String, Object> simulateStart(@RequestParam(defaultValue = "100") int devices,
                                             @RequestParam(defaultValue = "60") int perDevice,
                                             @RequestParam(defaultValue = "600") int seconds) {
        simulator.start(devices, devices * perDevice, seconds);
        return Map.of("devices", devices, "linesPerSecond", devices * perDevice, "seconds", seconds,
                "expectedLines", (long) devices * perDevice * seconds);
    }

    @PostMapping("/simulate/stop")
    public Map<String, Object> simulateStop() {
        simulator.stop();
        return Map.of("written", simulator.writtenCount());
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("receivedFromFluentBit", ingest.receivedCount());
        m.put("consumed", consumer.consumedCount());
        // Step 3: 두 번째 그룹(log-stats)이 센 건수. 그룹이 다르므로 log-printer 와 같은 메시지를 따로 전부 받는다.
        m.put("statsTotal", stats.total());
        m.put("statsByLevel", stats.countsByLevel());
        // Step 4+: 시뮬레이터가 지금까지 쓴 줄 수. 경로별 적재 행 수를 맞춰 보는 기준이다.
        m.put("simulatorWritten", simulator.writtenCount());
        m.put("simulatorRunning", simulator.running());
        return m;
    }

    /**
     * 이 앱의 리스너마다, 컨슈머 스레드(client.id) 하나가 지금 어떤 파티션을 맡고 있는지 보여 준다 (Step 3).
     * 앱을 하나 더 띄우거나 끄면 리밸런스가 일어나 이 표가 바뀐다. 두 앱의 응답을 합치면 그룹 전체의 할당이 된다.
     */
    @GetMapping("/assignments")
    public Map<String, Object> assignments() {
        Map<String, Object> result = new TreeMap<>();
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            Map<String, List<Integer>> byClient = new TreeMap<>();
            Map<String, Collection<TopicPartition>> assigned = container.getAssignmentsByClientId();
            if (assigned != null) {
                assigned.forEach((clientId, partitions) -> byClient.put(clientId,
                        partitions == null ? List.of() : partitions.stream().map(TopicPartition::partition).sorted().toList()));
            }
            result.put(container.getGroupId(), byClient);
        }
        return result;
    }
}
