package com.practice.kafka.loadtest;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 전송에서 브로커 ack 까지 걸린 시간을 1ms 칸에 세어 두는 히스토그램.
 *
 * <p>평균은 꼬리를 숨긴다. 대부분 5ms 에 끝나도 몇 퍼센트가 2초씩 걸리면 평균은 괜찮아 보이지만 그 몇 퍼센트가
 * 버퍼를 채우고 결국 전송을 막는다. 그래서 p50(가운데), p99(100건 중 가장 느린 1건의 경계), 최댓값을 함께 본다.
 * kafka-producer-perf-test 도 같은 세 값을 ms 정수로 보여 주므로 두 층의 숫자를 그대로 비교할 수 있다.
 *
 * <p>콜백은 프로듀서의 I/O 스레드 하나에서 주로 불리지만, ack 가 send 보다 먼저 끝난 경우에는 보내는 스레드에서 불린다.
 * 그래서 칸과 합계는 여러 스레드가 동시에 더해도 되는 타입으로 둔다.
 */
public final class LatencyHistogram {

    /** 이보다 긴 지연은 마지막 칸에 모은다. delivery.timeout.ms 기본값(120초)과 같다. */
    static final int MAX_MS = 120_000;

    private final AtomicLongArray buckets = new AtomicLongArray(MAX_MS + 1);
    private final LongAdder count = new LongAdder();
    private final LongAccumulator max = new LongAccumulator(Math::max, 0);

    public void recordNanos(long nanos) {
        long ms = Math.max(0, nanos / 1_000_000);
        buckets.incrementAndGet((int) Math.min(ms, MAX_MS));
        count.increment();
        max.accumulate(ms);
    }

    public long count() {
        return count.sum();
    }

    /** p(0~1) 백분위 지연(ms). 해당 칸의 아래 경계를 돌려준다. 기록이 없으면 0. */
    public long percentileMs(double p) {
        long total = count.sum();
        if (total == 0) {
            return 0;
        }
        long rank = Math.max(1, (long) Math.ceil(total * p));
        long seen = 0;
        for (int i = 0; i <= MAX_MS; i++) {
            seen += buckets.get(i);
            if (seen >= rank) {
                return i;
            }
        }
        return MAX_MS;
    }

    public long maxMs() {
        return max.get();
    }
}
