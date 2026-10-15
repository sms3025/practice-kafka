package com.practice.kafka.consumer;

import com.practice.kafka.log.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 로그 이벤트를 소비(consume)하는 컴포넌트. Step 1에서는 받은 메시지를 로그로 출력만 한다.
 *
 * <p>{@link KafkaListener}가 붙은 메서드는 Spring이 만든 리스너 컨테이너가 백그라운드 스레드에서
 * {@code poll()} 루프를 돌리며 호출한다. 우리는 "메시지 한 건이 왔을 때 무엇을 할지"만 적으면 된다.
 *
 * <p>groupId: 같은 그룹에 속한 컨슈머들은 토픽의 파티션을 나눠 갖는다(각 파티션은 그룹 내 한 컨슈머에만 할당).
 * 다른 그룹은 같은 메시지를 독립적으로 전부 받는다. (Step 2에서 실험한다)
 */
@Component
public class LogEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogEventConsumer.class);

    private final AtomicLong consumedCount = new AtomicLong();

    @KafkaListener(
            topics = "${app.kafka.topics.app-logs.name}",
            groupId = "${app.kafka.consumer-groups.printer}"
    )
    public void onLogEvent(@Payload LogEvent event,
                           // 키 없이 보낸 메시지는 이 헤더 자체가 없다. 필수로 두면 "Missing header" 예외가 나고
                           // 에러 핸들러가 10번 재시도한 뒤 그 메시지를 건너뛴다. 그래서 required = false.
                           @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                           @Header(KafkaHeaders.OFFSET) long offset) {
        long n = consumedCount.incrementAndGet();
        // Kafka 오프셋(offset)과 파일 오프셋(event.offset())은 다른 것이다. 전자는 파티션 내 위치, 후자는 원본 파일 내 바이트 위치.
        log.info("[consume #{}] p{}@{} key={} | event_time={} level={} file_offset={} | {}",
                n, partition, offset, key,
                event.eventTime(), event.level(), event.offset(), event.message());
    }

    public long consumedCount() {
        return consumedCount.get();
    }
}
