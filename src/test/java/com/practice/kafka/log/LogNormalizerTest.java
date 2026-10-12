package com.practice.kafka.log;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spring 없이 도는 순수 단위 테스트. 정규화 규칙이 바뀌면 여기부터 깨진다.
 *
 * <p>아래 예시 줄은 OHT 로그의 "형식"만 흉내 낸 가짜 내용이다. (실제 생성기 출력은 대외비이므로 저장소에 넣지 않는다)
 */
class LogNormalizerTest {

    private final LogNormalizer normalizer = new LogNormalizer("Asia/Seoul", "([A-Z]{2,5}-[0-9]{3,5})");
    private final Instant collectedAt = Instant.parse("2026-10-12T05:00:00Z");

    @Test
    void handlerLoggerFormat_isParsed() {
        LogEvent e = normalizer.normalize(
                "/var/log/devices/OHT-0007/OHTHandlerLogger/SEQ_INFO/20261012_14_SEQ_INFO.txt", 1842,
                "[2026-10-12 14:03:21.1234],[SEQ_INFO],[INFORMATION],[316710],[msg => example body text]\r",
                collectedAt);

        assertThat(e.deviceId()).isEqualTo("OHT-0007");                   // 파일명이 아니라 폴더 이름에서 추출
        assertThat(e.eventTime()).isEqualTo(Instant.parse("2026-10-12T05:03:21.1234Z")); // KST 14:03 → UTC 05:03
        assertThat(e.category()).isEqualTo("SEQ_INFO");
        assertThat(e.level()).isEqualTo("INFO");                           // INFORMATION → INFO 통일
        assertThat(e.message()).isEqualTo("example body text");
        assertThat(e.raw()).doesNotEndWith("\r");                          // CR 제거
        assertThat(e.idempotencyKey())
                .isEqualTo("OHT-0007|/var/log/devices/OHT-0007/OHTHandlerLogger/SEQ_INFO/20261012_14_SEQ_INFO.txt|1842");
    }

    @Test
    void uiLoggerFormat_isParsed() {
        LogEvent e = normalizer.normalize(
                "/var/log/devices/OHT-0007/UILogger/NET_20261012_14.log", 0,
                "2026-10-12 14:03:21,123 [17] INFO NET - [EXAMPLE] key=value", collectedAt);

        assertThat(e.deviceId()).isEqualTo("OHT-0007");
        assertThat(e.eventTime()).isEqualTo(Instant.parse("2026-10-12T05:03:21.123Z"));
        assertThat(e.category()).isEqualTo("NET");
        assertThat(e.level()).isEqualTo("INFO");
        assertThat(e.message()).isEqualTo("[EXAMPLE] key=value");
    }

    @Test
    void simpleFormat_isParsed_andDeviceIdFallsBackToFileName() {
        LogEvent e = normalizer.normalize("/var/log/devices/sim/dev-0007.log", 10,
                "2026-10-12 14:03:21.123 [WARN] ECU temperature high: 97C", collectedAt);

        assertThat(e.deviceId()).isEqualTo("dev-0007");                   // 패턴 불일치 → 파일명
        assertThat(e.category()).isNull();
        assertThat(e.level()).isEqualTo("WARN");
        assertThat(e.message()).isEqualTo("ECU temperature high: 97C");
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
        assertThat(e.category()).isNull();
        assertThat(e.raw()).isEqualTo("garbage line without time");
    }

    @Test
    void deviceIdFromWindowsPath() {
        assertThat(normalizer.normalize("C:\\sim-logs\\OHT-0123\\UILogger\\APP_20261012_14.log", 0, "x", collectedAt).deviceId())
                .isEqualTo("OHT-0123");
        assertThat(normalizer.normalize("C:\\logs\\truck-12.log", 0, "x", collectedAt).deviceId())
                .isEqualTo("truck-12");
    }
}
