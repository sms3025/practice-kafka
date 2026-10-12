package com.practice.kafka.log;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Spring 없이 도는 순수 단위 테스트. 정규화 규칙이 바뀌면 여기부터 깨진다. */
class LogNormalizerTest {

    private final LogNormalizer normalizer = new LogNormalizer("Asia/Seoul");
    private final Instant collectedAt = Instant.parse("2026-10-12T05:00:00Z");

    @Test
    void parsesTimeLevelAndMessage_andNormalizesToUtc() {
        LogEvent e = normalizer.normalize("/var/log/devices/dev-0007.log", 1842,
                "2026-10-12 14:03:21.123 [WARN] ECU temperature high: 97C", collectedAt);

        assertThat(e.deviceId()).isEqualTo("dev-0007");
        assertThat(e.eventTime()).isEqualTo(Instant.parse("2026-10-12T05:03:21.123Z")); // KST 14:03 → UTC 05:03
        assertThat(e.level()).isEqualTo("WARN");
        assertThat(e.message()).isEqualTo("ECU temperature high: 97C");
        assertThat(e.offset()).isEqualTo(1842);
        assertThat(e.idempotencyKey()).isEqualTo("dev-0007|/var/log/devices/dev-0007.log|1842");
    }

    @Test
    void acceptsVariousTimeFormats() {
        assertThat(normalizer.normalize("a.log", 0, "2026-10-12T14:03:21 ERROR boom", collectedAt).eventTime())
                .isEqualTo(Instant.parse("2026-10-12T05:03:21Z"));
        assertThat(normalizer.normalize("a.log", 0, "2026-10-12 14:03:21,123456 INFO ok", collectedAt).eventTime())
                .isEqualTo(Instant.parse("2026-10-12T05:03:21.123456Z"));
    }

    @Test
    void warningIsUnifiedToWarn() {
        LogEvent e = normalizer.normalize("a.log", 0, "2026-10-12 14:03:21 WARNING low fuel", collectedAt);
        assertThat(e.level()).isEqualTo("WARN");
        assertThat(e.message()).isEqualTo("low fuel");
    }

    @Test
    void unparsableLineFallsBackToCollectedAt_andKeepsRaw() {
        LogEvent e = normalizer.normalize("a.log", 0, "garbage line without time", collectedAt);
        assertThat(e.eventTime()).isEqualTo(collectedAt);
        assertThat(e.level()).isNull();
        assertThat(e.raw()).isEqualTo("garbage line without time");
    }

    @Test
    void deviceIdFromWindowsPath() {
        assertThat(normalizer.normalize("C:\\logs\\truck-12.log", 0, "x", collectedAt).deviceId())
                .isEqualTo("truck-12");
    }
}
