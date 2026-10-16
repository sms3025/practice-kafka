package com.practice.kafka.sink;

import com.practice.kafka.log.LogEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka → ClickHouse 적재 컨슈머. 토픽을 배치로 읽어 poll 한 번에 받은 묶음을 INSERT 한 번으로 넣는다.
 *
 * <p>poll 한 번에 얼마나 모이는지가 적재 처리량과 ClickHouse 의 part 수를 함께 정한다. 손잡이는 넷이다.
 * <ul>
 *   <li>{@code max.poll.records}: 한 번에 받을 최대 건수. 기본값 500 은 ClickHouse 에는 너무 작다</li>
 *   <li>{@code fetch.min.bytes}: 브로커가 이만큼 모일 때까지 응답을 미룬다. 부하가 낮을 때 묶음을 키운다</li>
 *   <li>{@code fetch.max.wait.ms}: 그래도 이 시간이 지나면 모인 만큼 돌려준다. 지연과 묶음 크기를 맞바꾸는 손잡이다</li>
 *   <li>{@code max.partition.fetch.bytes}: 파티션 하나에서 한 번에 가져올 최대 바이트. 기본 1MB 면 큰 묶음이 안 찬다</li>
 * </ul>
 *
 * <p>오프셋은 이 메서드가 정상 반환한 뒤에 커밋된다(배치 리스너의 기본 AckMode.BATCH). INSERT 가 성공했는데 커밋 전에 프로세스가
 * 죽으면 같은 묶음이 한 번 더 들어간다(at-least-once). 그 중복은 ReplacingMergeTree 가 멱등키 (device_id, source, offset) 로 지운다.
 */
@Component
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class ClickHouseSinkListener {

    private final ClickHouseWriter writer;

    public ClickHouseSinkListener(ClickHouseWriter writer) {
        this.writer = writer;
    }

    @KafkaListener(
            id = "clickhouse-sink",
            topics = "${app.sink.topic}",
            groupId = "${app.sink.group}",
            containerFactory = "sinkContainerFactory",
            properties = {
                    "max.poll.records=${app.sink.max-poll-records}",
                    "fetch.min.bytes=${app.sink.fetch-min-bytes}",
                    "fetch.max.wait.ms=${app.sink.fetch-max-wait-ms}",
                    "max.partition.fetch.bytes=${app.sink.max-partition-fetch-bytes}"
            })
    public void onBatch(List<LogEvent> events) {
        writer.insert(events);
    }
}
