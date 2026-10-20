package com.practice.kafka;

import com.practice.kafka.consumer.LogEventConsumer;
import com.practice.kafka.consumer.LogStatsConsumer;
import com.practice.kafka.log.LogEvent;
import com.practice.kafka.producer.LogEventProducer;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Producer → (Embedded Kafka) → Consumer 가 실제로 이어지는지 확인하는 통합 테스트.
 */
@SpringBootTest(properties = "app.sink.enabled=false")   // 적재 컨슈머(Step 4)는 ClickHouse 가 필요하므로 테스트에서는 끈다
@EmbeddedKafka(partitions = 3, topics = "app-logs", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class LogPipelineIntegrationTest {

    @Autowired
    LogEventProducer producer;

    @Autowired
    LogEventConsumer consumer;

    @Autowired
    LogStatsConsumer stats;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Test
    void producedEventIsConsumedByBothGroups() throws Exception {
        long before = consumer.consumedCount();
        long statsBefore = stats.total();
        Instant now = Instant.now();

        producer.send(new LogEvent("dev-0001", "/var/log/devices/dev-0001.log", now, now, 0,
                        "2026-10-12 14:03:21.123 [INFO] hello kafka", null, "INFO", "hello kafka"))
                .get(); // 테스트에서는 브로커 ack 까지 기다린다.

        // 컨슈머는 별도 스레드에서 poll 하므로 잠시 기다리며 확인한다.
        Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
        while ((consumer.consumedCount() == before || stats.total() == statsBefore) && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        // Step 3: 그룹이 다르면 같은 메시지를 둘 다 받는다 (log-printer 와 log-stats)
        assertThat(consumer.consumedCount()).isGreaterThan(before);
        assertThat(stats.total()).isGreaterThan(statsBefore);
        assertThat(stats.countsByLevel()).containsKey("INFO");
    }

    @Test
    void existingTopicIsGrownToConfiguredPartitions() {
        // Step 3: 브로커에는 3개로 만들어 두었지만(@EmbeddedKafka), 앱 설정은 6개다. 앱이 뜰 때 KafkaAdmin 이 늘린다.
        TopicDescription topic = kafkaAdmin.describeTopics("app-logs").get("app-logs");
        assertThat(topic.partitions()).hasSize(6);
    }
}
