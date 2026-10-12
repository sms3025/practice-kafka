package com.practice.kafka.producer;

import com.practice.kafka.log.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 로그 이벤트를 Kafka 토픽으로 발행(produce)하는 컴포넌트.
 *
 * <p>{@link KafkaTemplate}은 Spring Boot가 application.properties의 {@code spring.kafka.producer.*}
 * 설정으로 자동 구성한다. 직렬화기(serializer)도 거기서 지정했으므로 여기서는 객체를 그대로 넘기면 된다.
 *
 * <p>중요한 점: {@code send()}는 <b>비동기</b>다. 메서드가 반환되는 시점에 메시지는 아직 브로커에
 * 도착하지 않았을 수 있다. 프로듀서는 메시지를 내부 버퍼(RecordAccumulator)에 쌓았다가
 * 별도 I/O 스레드가 배치로 전송한다. 전송 결과는 {@link CompletableFuture}로 받는다.
 */
@Component
public class LogEventProducer {

    private static final Logger log = LoggerFactory.getLogger(LogEventProducer.class);

    private final KafkaTemplate<String, LogEvent> kafkaTemplate;
    private final String topic;

    public LogEventProducer(KafkaTemplate<String, LogEvent> kafkaTemplate,
                            @Value("${app.kafka.topics.app-logs.name}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * 이벤트를 발행한다. 메시지 키는 device_id 다.
     *
     * <p>같은 키를 가진 메시지는 항상 같은 파티션으로 간다(기본 파티셔너: murmur2(key) % 파티션 수).
     * 따라서 "같은 장비의 로그는 발생 순서대로 처리된다"는 보장을 얻는다.
     * 1,000대 장비가 3개 파티션에 해시로 분산되므로 특정 파티션으로 쏠리지 않는다.
     * 키가 null이면 파티션에 라운드로빈(정확히는 sticky) 방식으로 분산된다.
     */
    public CompletableFuture<SendResult<String, LogEvent>> send(LogEvent event) {
        return kafkaTemplate.send(topic, event.deviceId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("발행 실패 key={}", event.idempotencyKey(), ex);
                    } else {
                        var meta = result.getRecordMetadata();
                        log.debug("발행 성공 key={} -> topic={} partition={} offset={}",
                                event.idempotencyKey(), meta.topic(), meta.partition(), meta.offset());
                    }
                });
    }
}
