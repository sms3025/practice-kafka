package com.practice.kafka.producer;

import com.practice.kafka.log.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fluent Bit 을 거치지 않고 Kafka 에 직접 "대용량 로그"를 밀어 넣는 부하 생성기.
 *
 * <p>수집 경로(Fluent Bit → HTTP)의 병목과 Kafka 자체의 처리량을 분리해서 보고 싶을 때 쓴다.
 * {@link #generate(int)}는 모든 전송의 Future 가 완료(브로커 ack)될 때까지 기다린 뒤 걸린 시간을 돌려주므로,
 * 프로듀서 설정(batch.size, linger.ms, acks 등)을 바꿔가며 처리량 차이를 체감할 수 있다.
 */
@Component
public class LogGenerator {

    private static final Logger log = LoggerFactory.getLogger(LogGenerator.class);

    private static final List<String> MESSAGES = List.of(
            "ENGINE rpm=2400 temp=88C", "GPS fix acquired", "CAN bus frame dropped",
            "BATTERY voltage=12.6V", "SENSOR vibration threshold exceeded",
            "FW update check: version 2.3.1", "DOOR state changed", "ECU watchdog reset");

    private final LogEventProducer producer;
    private final AtomicLong fakeOffset = new AtomicLong();

    public LogGenerator(LogEventProducer producer) {
        this.producer = producer;
    }

    /** count 건을 발행하고, 모든 전송이 완료(브로커 ack 수신)될 때까지 걸린 시간을 ms 로 돌려준다. */
    public long generate(int count, int deviceCount) {
        long start = System.nanoTime();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            futures[i] = producer.send(randomEvent(deviceCount));
        }
        CompletableFuture.allOf(futures).join();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("{}건 발행 완료 ({} ms, 약 {} msg/s)", count, elapsedMs,
                elapsedMs == 0 ? "∞" : count * 1000L / elapsedMs);
        return elapsedMs;
    }

    private LogEvent randomEvent(int deviceCount) {
        var rnd = ThreadLocalRandom.current();
        int r = rnd.nextInt(100);
        String level = r < 65 ? "INFO" : r < 85 ? "DEBUG" : r < 95 ? "WARN" : "ERROR";
        String deviceId = String.format("dev-%04d", rnd.nextInt(deviceCount) + 1);
        String message = MESSAGES.get(rnd.nextInt(MESSAGES.size()));
        Instant now = Instant.now();
        String raw = now + " [" + level + "] " + message;
        return new LogEvent(deviceId, "generator://" + deviceId, now, now,
                fakeOffset.getAndIncrement(), raw, "GEN", level, message);
    }
}
