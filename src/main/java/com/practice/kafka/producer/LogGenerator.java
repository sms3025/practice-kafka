package com.practice.kafka.producer;

import com.practice.kafka.log.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * "대용량 로그"를 흉내 내기 위한 무작위 로그 생성기.
 *
 * <p>여러 서비스/호스트에서 다양한 레벨의 로그가 섞여 들어오는 상황을 만든다.
 * {@link #generate(int)}는 모든 전송의 Future가 완료될 때까지 기다린 뒤 걸린 시간을 돌려주므로,
 * 프로듀서 설정(batch.size, linger.ms, acks 등)을 바꿔가며 처리량 차이를 체감할 수 있다.
 */
@Component
public class LogGenerator {

    private static final Logger log = LoggerFactory.getLogger(LogGenerator.class);

    private static final List<String> SERVICES = List.of(
            "order-api", "payment-api", "inventory-api", "user-api", "notification-worker");
    private static final List<String> HOSTS = List.of("host-a", "host-b", "host-c");
    private static final List<String> MESSAGES = List.of(
            "request handled", "cache miss", "slow query detected", "retrying upstream call",
            "connection reset by peer", "NullPointerException at OrderService.create",
            "payment approved", "stock reserved", "token expired", "scheduled job finished");

    private final LogEventProducer producer;

    public LogGenerator(LogEventProducer producer) {
        this.producer = producer;
    }

    /** count 건을 발행하고, 모든 전송이 완료(브로커 ack 수신)될 때까지 걸린 시간을 ms로 돌려준다. */
    public long generate(int count) {
        long start = System.nanoTime();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            futures[i] = producer.send(randomEvent());
        }
        CompletableFuture.allOf(futures).join();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("{}건 발행 완료 ({} ms, 약 {} msg/s)", count, elapsedMs,
                elapsedMs == 0 ? "∞" : count * 1000L / elapsedMs);
        return elapsedMs;
    }

    private LogEvent randomEvent() {
        var rnd = ThreadLocalRandom.current();
        // 레벨 분포: INFO 70%, DEBUG 15%, WARN 10%, ERROR 5% 정도로 현실적인 비율을 흉내 낸다.
        int r = rnd.nextInt(100);
        LogEvent.LogLevel level = r < 70 ? LogEvent.LogLevel.INFO
                : r < 85 ? LogEvent.LogLevel.DEBUG
                : r < 95 ? LogEvent.LogLevel.WARN
                : LogEvent.LogLevel.ERROR;
        return LogEvent.of(
                SERVICES.get(rnd.nextInt(SERVICES.size())),
                level,
                MESSAGES.get(rnd.nextInt(MESSAGES.size())),
                HOSTS.get(rnd.nextInt(HOSTS.size())));
    }
}
