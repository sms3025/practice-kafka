package com.practice.kafka.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertSettings;
import com.practice.kafka.log.LogEvent;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 적재 컨슈머(Kafka → ClickHouse) 구성 (Step 4). {@code app.sink.enabled=true} 일 때만 뜬다(기본값 true. 테스트에서는 끈다).
 * 이 연습에서는 수집 서버와 같은 프로세스에서 돌지만, 그룹(clickhouse-sink)과 리스너 팩토리가 따로라 log-printer 와 서로 영향이 없다.
 * 운영에서는 별도 서비스로 떼어 낸다. 처리량 실측은 kafka-load-test 브랜치의 docs/result/benchmark-transport.md 7.3절에 있다.
 */
@Configuration
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class SinkConfig {

    /**
     * 적재 컨슈머가 쓰는 INSERT 도구. 대상은 logs.app_logs 이고, 설정을 덧붙이지 않은 보통 INSERT 다.
     * 클라이언트는 {@link com.practice.kafka.config.ClickHouseConfig} 가 만든다(Step 4+ 의 직접 적재 경로와 함께 쓴다).
     */
    @Bean
    public ClickHouseWriter sinkWriter(Client client, @Value("${app.clickhouse.table}") String table) {
        return new ClickHouseWriter(client, table, InsertSettings::new);
    }

    /**
     * 배치 리스너용 컨테이너 팩토리. Spring Boot 가 만드는 기본 팩토리와 따로 둔다.
     * 그래서 기본 팩토리에 건 {@code spring.kafka.listener.concurrency=3} 이 여기에는 걸리지 않고, 컨슈머 수를 따로 정할 수 있다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LogEvent> sinkContainerFactory(
            ConsumerFactory<String, LogEvent> consumerFactory,
            @Value("${app.sink.concurrency}") int concurrency) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, LogEvent>();
        factory.setConsumerFactory(consumerFactory);
        // poll() 한 번에 받은 묶음 전체를 리스너에 넘긴다. 묶음 하나 = INSERT 한 번이다.
        factory.setBatchListener(true);
        // 컨슈머 스레드 수. 각 스레드가 KafkaConsumer 하나이고, 그룹 안에서 파티션을 나눠 갖는다. 파티션 수보다 많으면 남는 스레드는 논다.
        factory.setConcurrency(concurrency);
        // INSERT 가 실패하면 같은 묶음을 1초 간격으로 끝없이 다시 시도한다. 기본 에러 핸들러는 10번 실패하면 그 묶음을 건너뛰는데,
        // 저장소 장애 동안 로그를 버리게 되므로 쓰지 않는다. 오프셋은 성공한 뒤에만 커밋되므로 그동안 로그는 Kafka 에 남아 있다.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }

    /** lag(아직 적재하지 않은 메시지 수)을 재는 관리 클라이언트. 앱의 KafkaAdmin 과 같은 접속 설정을 쓴다. */
    @Bean(destroyMethod = "close")
    public Admin sinkAdmin(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }
}
