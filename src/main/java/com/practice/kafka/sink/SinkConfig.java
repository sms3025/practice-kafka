package com.practice.kafka.sink;

import com.clickhouse.client.api.Client;
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
 * 적재 컨슈머(Kafka → ClickHouse) 구성. {@code app.sink.enabled=true} 인 프로세스에서만 뜬다(부하 테스트 compose 의 sink 컨테이너).
 * 수집 서버 프로세스와 나눈 것은 운영에서도 둘이 다른 서비스이고, CPU 를 따로 재야 병목을 가를 수 있기 때문이다.
 */
@Configuration
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class SinkConfig {

    /**
     * ClickHouse 공식 Java 클라이언트(client-v2). HTTP 인터페이스(8123)로 RowBinary 를 보낸다. 스레드 안전하므로 하나를 함께 쓴다.
     * 동시 연결 수 기본값(10)은 컨슈머 12개가 동시에 INSERT 하기에 모자라므로 늘린다.
     */
    @Bean(destroyMethod = "close")
    public Client clickHouseClient(@Value("${app.clickhouse.url}") String url,
                                   @Value("${app.clickhouse.user}") String user,
                                   @Value("${app.clickhouse.password}") String password,
                                   @Value("${app.clickhouse.database}") String database,
                                   @Value("${app.clickhouse.max-connections}") int maxConnections) {
        return new Client.Builder()
                .addEndpoint(url)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setMaxConnections(maxConnections)
                .build();
    }

    /**
     * 배치 리스너용 컨테이너 팩토리. Spring Boot 가 만드는 기본 팩토리와 따로 둔다.
     * 그래서 sink 컨테이너에서 {@code spring.kafka.listener.auto-startup=false} 로 기본 팩토리의 리스너(log-printer)를 꺼도 이것은 뜬다.
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
