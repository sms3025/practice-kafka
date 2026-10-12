package com.practice.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * 컨텍스트 로딩 테스트.
 *
 * <p>{@link EmbeddedKafka}가 테스트용 인메모리 브로커를 띄우고,
 * {@code bootstrapServersProperty}로 그 주소를 spring.kafka.bootstrap-servers 에 꽂아준다.
 * 따라서 docker 없이도 테스트가 돈다.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "app-logs", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class PracticeKafkaApplicationTests {

    @Test
    void contextLoads() {
    }
}
