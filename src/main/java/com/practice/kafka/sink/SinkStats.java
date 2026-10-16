package com.practice.kafka.sink;

import com.practice.kafka.loadtest.LatencyHistogram;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 적재 컨슈머가 센 숫자. INSERT 횟수, 행 수, INSERT 한 번에 걸린 시간(묶음 인코딩 + 전송 + ClickHouse 기록)을 센다.
 * 컨슈머 스레드 여럿이 동시에 더하므로 동시성 타입을 쓴다. 측정 사이에는 {@link #reset()} 으로 비운다.
 */
@Component
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class SinkStats {

    private final LongAdder inserts = new LongAdder();
    private final LongAdder rows = new LongAdder();
    private final LongAdder writtenBytes = new LongAdder();
    private final LongAdder serverNanos = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final AtomicLong firstInsertStartMs = new AtomicLong();
    private final AtomicLong lastInsertEndMs = new AtomicLong();
    private volatile LatencyHistogram insertLatency = new LatencyHistogram();

    void recordInsert(long startEpochMs, int rowCount, long bytes, long elapsedNanos, long serverTimeNanos) {
        inserts.increment();
        rows.add(rowCount);
        writtenBytes.add(Math.max(0, bytes));
        serverNanos.add(Math.max(0, serverTimeNanos));
        insertLatency.recordNanos(elapsedNanos);
        firstInsertStartMs.compareAndSet(0, startEpochMs);
        lastInsertEndMs.accumulateAndGet(System.currentTimeMillis(), Math::max);
    }

    void recordFailure() {
        failures.increment();
    }

    public void reset() {
        inserts.reset();
        rows.reset();
        writtenBytes.reset();
        serverNanos.reset();
        failures.reset();
        firstInsertStartMs.set(0);
        lastInsertEndMs.set(0);
        insertLatency = new LatencyHistogram();
    }

    public Map<String, Object> snapshot() {
        long n = inserts.sum();
        long r = rows.sum();
        long first = firstInsertStartMs.get();
        long last = lastInsertEndMs.get();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("inserts", n);
        m.put("rows", r);
        m.put("rowsPerInsert", n == 0 ? 0 : r / n);
        m.put("writtenBytes", writtenBytes.sum());
        m.put("failures", failures.sum());
        // 첫 INSERT 시작부터 마지막 INSERT 끝까지의 평균 적재 속도. 쌓아 둔 메시지를 비우는 실험에서 "적재 처리량"이 된다.
        m.put("rowsPerSecond", last > first && first > 0 ? r * 1000.0 / (last - first) : 0);
        m.put("insertP50Ms", insertLatency.percentileMs(0.50));
        m.put("insertP99Ms", insertLatency.percentileMs(0.99));
        m.put("insertMaxMs", insertLatency.maxMs());
        // ClickHouse 가 응답 헤더(X-ClickHouse-Summary 의 elapsed_ns)로 알려 준, 서버 안에서 INSERT 를 처리한 시간의 평균(ms).
        // INSERT 시간에서 이것을 빼면 앱 쪽(인코딩, 전송) 몫이다.
        m.put("serverMsPerInsert", n == 0 ? 0 : serverNanos.sum() / 1e6 / n);
        m.put("firstInsertStartEpochMs", first);
        m.put("lastInsertEndEpochMs", last);
        return m;
    }
}
