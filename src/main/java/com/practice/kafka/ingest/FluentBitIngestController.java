package com.practice.kafka.ingest;

import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import com.practice.kafka.producer.LogEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 수집 서버의 진입점. Fluent Bit 에이전트가 HTTP 로 보낸 로그 묶음을 받아 정규화한 뒤 Kafka 에 넣는다.
 *
 * <p>명세서의 아키텍처에서 "HTTP 전송 → 수집 서버" 에 해당한다. 이 컨트롤러가 하는 일은 딱 세 가지다.
 * 받는다 → 정규화한다 → Kafka 에 넣는다. 저장소(ClickHouse) 적재는 여기서 하지 않는다.
 * 수집 서버가 저장소를 직접 쓰면 저장소가 느려질 때 에이전트 1,000대가 전부 밀리지만,
 * Kafka 에 넣고 끝내면 수집 서버는 항상 빠르게 응답할 수 있다. 이것이 "완충(buffering)" 의 의미다.
 *
 * <p><b>응답 코드가 곧 전송 보장이다.</b> Fluent Bit 은 2xx 가 아니면 같은 묶음을 재전송한다.
 * 그래서 Kafka 브로커의 ack 를 받은 뒤에 200 을 돌려준다. Kafka 가 죽어 있으면 5xx 를 돌려주고,
 * 에이전트는 자기 디스크 버퍼에 들고 있다가 다시 보낸다. 대신 같은 줄이 두 번 올 수 있으므로(at-least-once),
 * (device_id, source, offset) 멱등키가 필요하다.
 */
@RestController
public class FluentBitIngestController {

    private static final Logger log = LoggerFactory.getLogger(FluentBitIngestController.class);

    private final LogNormalizer normalizer;
    private final LogEventProducer producer;
    private final AtomicLong received = new AtomicLong();

    public FluentBitIngestController(LogNormalizer normalizer, LogEventProducer producer) {
        this.normalizer = normalizer;
        this.producer = producer;
    }

    @PostMapping("/ingest/fluentbit")
    public ResponseEntity<Void> ingest(@RequestBody List<FluentBitRecord> records) {
        Instant now = Instant.now();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[records.size()];
        try {
            for (int i = 0; i < records.size(); i++) {
                FluentBitRecord r = records.get(i);
                LogEvent event = normalizer.normalize(
                        r.source(),
                        r.offset() == null ? -1 : r.offset(),
                        r.log(),
                        r.collectedAt() == null ? now : r.collectedAt());
                // send() 자체가 예외를 던질 수도 있어 try 안에 둔다. 알던 브로커가 모두 끊기면 Kafka 4 클라이언트는
                // 메타데이터를 버리고 bootstrap 부터 다시 시작한다(metadata.recovery.strategy=rebootstrap 기본값).
                // 그러면 토픽 정보가 없어 send() 가 max.block.ms(기본 60초) 동안 막힌 뒤 Future 가 아니라 예외로 실패한다.
                futures[i] = producer.send(event);
            }
            // 묶음 전체가 브로커에 기록된 것을 확인한 뒤에야 성공을 알린다.
            CompletableFuture.allOf(futures).join();
        } catch (RuntimeException e) {
            log.error("Kafka 발행 실패. Fluent Bit 에 재전송을 요청한다 (batch={}건)", records.size(), e);
            return ResponseEntity.internalServerError().build();
        }
        long total = received.addAndGet(records.size());
        log.debug("Fluent Bit 묶음 수신 {}건 (누적 {})", records.size(), total);
        return ResponseEntity.ok().build();
    }

    public long receivedCount() {
        return received.get();
    }
}
