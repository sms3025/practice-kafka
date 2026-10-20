package com.practice.kafka.consumer;

import com.practice.kafka.log.LogEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 두 번째 컨슈머 그룹(log-stats). 같은 app-logs 토픽을 읽어 레벨별 건수만 센다 (Step 3).
 *
 * <p>{@link LogEventConsumer}(log-printer)와 그룹이 다르므로 같은 메시지를 둘 다 전부 받는다. Kafka 는 그룹마다 오프셋을 따로 기억하기
 * 때문이다. 그래서 "콘솔 출력", "통계", 나중의 "ClickHouse 적재"처럼 하는 일이 다른 소비자를 서로 영향 없이 붙일 수 있다.
 * 한 그룹이 느려지거나 멈춰도 다른 그룹의 진행(lag)은 그대로다.
 *
 * <p>메시지마다 로그를 찍지 않는다. 숫자만 더하므로 log-printer 보다 훨씬 빨리 따라간다. 두 그룹의 lag 를 Kafka UI 에서 비교해 볼 수 있다.
 */
@Component
public class LogStatsConsumer {

    /** 레벨 → 건수. 컨슈머 스레드 여럿(concurrency)이 동시에 더하므로 동시성 맵과 LongAdder 를 쓴다. 레벨이 없으면 "NONE". */
    private final Map<String, LongAdder> byLevel = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${app.kafka.topics.app-logs.name}",
            groupId = "${app.kafka.consumer-groups.stats}"
    )
    public void onLogEvent(LogEvent event) {
        String level = event.level() == null ? "NONE" : event.level();
        byLevel.computeIfAbsent(level, k -> new LongAdder()).increment();
    }

    /** 레벨 이름 순으로 정렬한 건수. */
    public Map<String, Long> countsByLevel() {
        Map<String, Long> result = new TreeMap<>();
        byLevel.forEach((level, count) -> result.put(level, count.sum()));
        return result;
    }

    public long total() {
        return byLevel.values().stream().mapToLong(LongAdder::sum).sum();
    }
}
