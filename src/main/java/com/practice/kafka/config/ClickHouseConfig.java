package com.practice.kafka.config;

import com.clickhouse.client.api.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * ClickHouse 공식 Java 클라이언트(client-v2) 하나를 만든다. 적재 컨슈머(Step 4, sink/)와 직접 적재 비교 경로(Step 4+, direct/)가 함께 쓴다.
 *
 * <p>둘 중 하나라도 켜져 있을 때만 만든다. 둘 다 끈 테스트에서는 ClickHouse 가 필요 없다.
 * HTTP 인터페이스(8123)로 RowBinary 를 보낸다. 스레드 안전하므로 하나를 함께 쓴다.
 */
@Configuration
@ConditionalOnExpression("${app.sink.enabled:false} or ${app.compare.enabled:false}")
public class ClickHouseConfig {

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
                // 동시 INSERT 연결 수 상한. 적재 스레드 수와 직접 적재 요청 동시 처리 수보다 넉넉하면 된다
                .setMaxConnections(maxConnections)
                .build();
    }
}
