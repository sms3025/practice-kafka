package com.practice.kafka.sink;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 적재 컨슈머의 상태 API. 스크립트(scripts/loadtest/sink.ps1)가 2초마다 읽어 lag 이 0 이 될 때까지의 적재 속도를 잰다.
 *
 * <ul>
 *   <li>GET  /api/sink/stats : INSERT 통계 + 지금 lag</li>
 *   <li>POST /api/sink/reset : 통계를 비운다</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/sink")
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class SinkController {

    private final SinkStats stats;
    private final Admin admin;
    private final String topic;
    private final String group;

    public SinkController(SinkStats stats, Admin sinkAdmin,
                          @Value("${app.sink.topic}") String topic, @Value("${app.sink.group}") String group) {
        this.stats = stats;
        this.admin = sinkAdmin;
        this.topic = topic;
        this.group = group;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> m = new LinkedHashMap<>(stats.snapshot());
        m.put("lag", lag());
        return m;
    }

    @PostMapping("/reset")
    public Map<String, Object> reset() {
        stats.reset();
        return stats.snapshot();
    }

    /** 파티션마다 (끝 오프셋 - 그룹이 커밋한 오프셋) 의 합. 커밋이 없는 파티션은 처음부터 읽어야 하므로 끝 오프셋 전체가 lag 이다. */
    private long lag() throws ExecutionException, InterruptedException, TimeoutException {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic)
                .partitions().forEach(p -> latest.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(latest).all()
                .get(10, TimeUnit.SECONDS);
        long lag = 0;
        for (var e : ends.entrySet()) {
            OffsetAndMetadata c = committed.get(e.getKey());
            lag += e.getValue().offset() - (c == null ? 0 : c.offset());
        }
        return lag;
    }
}
