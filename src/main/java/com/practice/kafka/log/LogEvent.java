package com.practice.kafka.log;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka 토픽을 오가는 로그 이벤트 한 건.
 *
 * <p>Java record는 불변(immutable) 데이터 홀더다. 메시지는 "한 번 발행되면 바뀌지 않는 사실"이므로
 * record가 잘 어울린다. Jackson이 이 record를 JSON으로 직렬화/역직렬화한다.
 *
 * @param id        이벤트 고유 ID (중복 처리 감지에 사용 가능)
 * @param timestamp 로그가 발생한 시각
 * @param service   로그를 남긴 서비스 이름 (예: order-api, payment-api). Kafka 메시지 키로도 사용한다.
 * @param level     로그 레벨
 * @param message   로그 본문
 * @param host      로그가 발생한 호스트/인스턴스 이름
 */
public record LogEvent(
        String id,
        Instant timestamp,
        String service,
        LogLevel level,
        String message,
        String host
) {

    /** 새 이벤트를 만들 때 id와 timestamp를 자동으로 채워주는 팩토리 메서드. */
    public static LogEvent of(String service, LogLevel level, String message, String host) {
        return new LogEvent(UUID.randomUUID().toString(), Instant.now(), service, level, message, host);
    }

    public enum LogLevel {
        DEBUG, INFO, WARN, ERROR
    }
}
