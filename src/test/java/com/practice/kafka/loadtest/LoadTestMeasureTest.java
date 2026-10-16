package com.practice.kafka.loadtest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 계단을 "버텼다"고 판정하는 규칙과 지연 백분위 계산. 결과 문서의 표가 이 두 가지에 기대므로 고정해 둔다.
 */
class LoadTestMeasureTest {

    @Test
    void percentilesComeFromMillisecondBuckets() {
        LatencyHistogram h = new LatencyHistogram();
        for (int ms = 1; ms <= 100; ms++) {
            h.recordNanos(ms * 1_000_000L + 400_000);   // 1.4ms, 2.4ms, ... 100.4ms → 칸은 1, 2, ... 100
        }
        assertThat(h.count()).isEqualTo(100);
        assertThat(h.percentileMs(0.50)).isEqualTo(50);
        assertThat(h.percentileMs(0.99)).isEqualTo(99);
        assertThat(h.maxMs()).isEqualTo(100);
    }

    @Test
    void latencyBeyondLastBucketIsClamped() {
        LatencyHistogram h = new LatencyHistogram();
        h.recordNanos(500_000L * 1_000_000);   // 500초
        assertThat(h.percentileMs(0.99)).isEqualTo(LatencyHistogram.MAX_MS);
        assertThat(h.maxMs()).isEqualTo(500_000);
    }

    @Test
    void stepPassesOnlyWhenAllThreeConditionsHold() {
        LoadTestPlan plan = new LoadTestPlan(ProducerPreset.BASELINE, 100_000, 50_000, 1_000_000,
                30, 5, 4, 1, 2, 0.95, 1000);

        assertThat(plan.passes(0.96, 800, 0)).isTrue();
        assertThat(plan.passes(0.94, 800, 0)).isFalse();    // 목표의 95% 미만
        assertThat(plan.passes(0.99, 1001, 0)).isFalse();   // p99 1초 초과
        assertThat(plan.passes(0.99, 10, 1)).isFalse();     // 오류 1건
    }

    @Test
    void rejectsPlanWhoseWarmupEatsTheWholeStep() {
        assertThatThrownBy(() -> new LoadTestPlan(ProducerPreset.BASELINE, 100_000, 50_000, 1_000_000,
                5, 5, 4, 1, 2, 0.95, 1000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMoreProducersThanThreads() {
        // 스레드 i 는 프로듀서 i % producers 를 쓰므로, 프로듀서가 스레드보다 많으면 남는 프로듀서는 아무것도 보내지 않는다.
        assertThatThrownBy(() -> new LoadTestPlan(ProducerPreset.BASELINE, 100_000, 50_000, 1_000_000,
                30, 5, 2, 4, 2, 0.95, 1000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void presetKeysMatchTheScripts() {
        // scripts/loadtest/common.ps1 의 $Presets 와 같은 이름이어야 한다.
        assertThat(ProducerPreset.of("baseline")).isEqualTo(ProducerPreset.BASELINE);
        assertThat(ProducerPreset.of("batch")).isEqualTo(ProducerPreset.BATCH);
        assertThat(ProducerPreset.of("batch-lz4")).isEqualTo(ProducerPreset.BATCH_LZ4);
        assertThatThrownBy(() -> ProducerPreset.of("fast")).isInstanceOf(IllegalArgumentException.class);
    }
}
