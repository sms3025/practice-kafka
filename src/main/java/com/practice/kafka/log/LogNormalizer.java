package com.practice.kafka.log;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 원문 한 줄(raw)을 명세서 스키마의 {@link LogEvent}로 정규화한다.
 *
 * <p>명세서가 요구하는 정규화는 두 가지다.
 * <ol>
 *   <li>device_id 추출: 에이전트는 파일 경로만 알고 있으므로 경로의 파일명에서 장비 ID를 뽑는다.
 *       (예: {@code /var/log/devices/dev-0007.log} → {@code dev-0007})</li>
 *   <li>event_time 정규화: 장비가 찍은 로컬 시각 문자열을 UTC {@link Instant}로 바꾼다.
 *       장비마다 시각 표기와 타임존이 다른 것이 명세서 [배경]에 적힌 문제이므로, 여기서 기준을 통일한다.</li>
 * </ol>
 *
 * <p>원문에서 시각을 찾지 못하면 수집 시각(collected_at)으로 대체한다. 버리지 않고 어떻게든 저장하는 것이
 * 로그 파이프라인의 원칙이다(raw 는 그대로 남아 있으므로 나중에 재처리할 수 있다).
 */
@Component
public class LogNormalizer {

    /**
     * 지원하는 원문 형식:  {@code 2026-10-12 14:03:21.123 [WARN] message...}
     * 시각 구분자는 공백 또는 T, 밀리초는 선택, 레벨의 대괄호는 선택.
     */
    private static final Pattern LINE = Pattern.compile(
            "^(?<time>\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}(?:[.,]\\d{1,6})?)"
            // WARNING 을 WARN 보다 앞에 두어야 한다. 정규식 대안(|)은 왼쪽부터 시도하므로 WARN 이 먼저 매칭되면 "ING" 가 본문으로 남는다.
            + "\\s+\\[?(?<level>TRACE|DEBUG|INFO|WARNING|WARN|ERROR|FATAL)\\]?\\s*"
            + "(?<message>.*)$");

    /** 날짜/시각 구분자(공백 또는 T)와 0~9자리 소수부를 모두 허용하는 파서. */
    private static final DateTimeFormatter TIME = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart().appendLiteral(' ').optionalEnd()
            .optionalStart().appendLiteral('T').optionalEnd()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter(Locale.ROOT);

    private final ZoneId deviceZone;

    public LogNormalizer(@Value("${app.ingest.device-zone:Asia/Seoul}") String deviceZone) {
        this.deviceZone = ZoneId.of(deviceZone);
    }

    public LogEvent normalize(String source, long offset, String raw, Instant collectedAt) {
        String deviceId = deviceIdFrom(source);
        Matcher m = LINE.matcher(raw == null ? "" : raw.strip());
        if (!m.matches()) {
            return new LogEvent(deviceId, source, collectedAt, collectedAt, offset, raw, null, raw);
        }
        Instant eventTime = parseTime(m.group("time"), collectedAt);
        String level = m.group("level");
        if ("WARNING".equals(level)) {
            level = "WARN";   // 표기 통일
        }
        return new LogEvent(deviceId, source, eventTime, collectedAt, offset, raw, level, m.group("message"));
    }

    /** 파일명에서 확장자를 뗀 것을 장비 ID로 본다. 경로가 없으면 "unknown". */
    static String deviceIdFrom(String source) {
        if (source == null || source.isBlank()) {
            return "unknown";
        }
        String fileName = Paths.get(source.replace('\\', '/')).getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private Instant parseTime(String text, Instant fallback) {
        try {
            return LocalDateTime.parse(text.replace(',', '.'), TIME).atZone(deviceZone).toInstant();
        } catch (DateTimeParseException e) {
            return fallback;
        }
    }
}
