package com.practice.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 애플리케이션 기동 시 토픽을 선언적으로 생성한다.
 *
 * <p>Spring Boot가 자동 구성한 {@code KafkaAdmin}이 컨텍스트에 등록된 {@link NewTopic} 빈을 모아
 * 브로커에 토픽을 만든다. 이미 존재하면 아무것도 하지 않는다(파티션 수가 더 크면 늘려주기는 한다).
 *
 * <p>브로커에서 {@code auto.create.topics.enable=false} 로 꺼두었기 때문에,
 * 토픽은 반드시 이렇게 명시적으로 만들어야 한다. 실무에서도 토픽은 "코드/IaC로 선언"하는 것이 일반적이다.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic appLogsTopic(
            @Value("${app.kafka.topics.app-logs.name}") String name,
            @Value("${app.kafka.topics.app-logs.partitions}") int partitions,
            @Value("${app.kafka.topics.app-logs.replicas}") int replicas) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }
}
