package com.practice.kafka.sink;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 적재 컨슈머가 센 숫자. INSERT 횟수, 행 수, INSERT 한 번에 걸린 시간을 센다 (Step 4).
 *
 * <p>"INSERT 한 번에 몇 행이 실렸는가"가 이 단계에서 가장 중요한 숫자다. ClickHouse 는 INSERT 한 번마다 디스크에 part 를 하나 만들므로,
 * 같은 행 수를 적은 INSERT 로 넣을수록 part 가 적게 생기고 병합 부담이 줄어든다. 컨슈머 스레드 여럿이 동시에 더하므로 동시성 타입을 쓴다.
 */
@Component
@ConditionalOnProperty(name = "app.sink.enabled", havingValue = "true")
public class SinkStats {

    private final LongAdder inserts = new LongAdder();
    private final LongAdder rows = new LongAdder();
    private final LongAdder insertMillis = new LongAdder();
    private final LongAccumulator maxInsertMillis = new LongAccumulator(Math::max, 0);
    private final LongAdder failures = new LongAdder();
    private final AtomicLong lastInsertEpochMs = new AtomicLong();

    void recordInsert(int rowCount, long elapsedMillis) {
        inserts.increment();
        rows.add(rowCount);
        insertMillis.add(elapsedMillis);
        maxInsertMillis.accumulate(elapsedMillis);
        lastInsertEpochMs.set(System.currentTimeMillis());
    }

    void recordFailure() {
        failures.increment();
    }

    public Map<String, Object> snapshot() {
        long n = inserts.sum();
        long r = rows.sum();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("inserts", n);
        m.put("rows", r);
        m.put("rowsPerInsert", n == 0 ? 0 : r / n);
        m.put("avgInsertMs", n == 0 ? 0 : insertMillis.sum() / n);
        m.put("maxInsertMs", maxInsertMillis.get());
        m.put("failures", failures.sum());
        m.put("lastInsertEpochMs", lastInsertEpochMs.get());
        return m;
    }
}
