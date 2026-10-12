package com.practice.kafka;

import com.practice.kafka.consumer.LogEventConsumer;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.producer.LogEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Producer → (Embedded Kafka) → Consumer 가 실제로 이어지는지 확인하는 통합 테스트.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "app-logs", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class LogPipelineIntegrationTest {

    @Autowired
    LogEventProducer producer;

    @Autowired
    LogEventConsumer consumer;

    @Test
    void producedEventIsConsumed() throws Exception {
        long before = consumer.consumedCount();

        producer.send(LogEvent.of("order-api", LogEvent.LogLevel.INFO, "hello kafka", "test-host"))
                .get(); // 테스트에서는 브로커 ack까지 기다린다.

        // 컨슈머는 별도 스레드에서 poll 하므로 잠시 기다리며 확인한다.
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (consumer.consumedCount() == before && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        assertThat(consumer.consumedCount()).isGreaterThan(before);
    }
}
