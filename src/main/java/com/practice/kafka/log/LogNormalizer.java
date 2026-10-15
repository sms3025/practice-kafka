package com.practice.kafka.log;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
 *   <li>device_id 추출: 에이전트는 파일 경로만 알고 있으므로 경로에서 장비 ID를 뽑는다.
 *       OHT 차량 로그는 {@code .../OHT-0001/UILogger/APP_20260105_09.log} 처럼 장비 ID가 <b>폴더 이름</b>에 있으므로
 *       파일명이 아니라 경로 전체에서 {@code app.ingest.device-id-pattern} 정규식으로 찾는다.
 *       못 찾으면 파일명(확장자 제외)을 쓴다.</li>
 *   <li>event_time 정규화: 장비가 찍은 로컬 시각 문자열을 UTC {@link Instant}로 바꾼다.
 *       장비마다 시각 표기와 타임존이 다른 것이 명세서 [배경]에 적힌 문제이므로, 여기서 기준을 통일한다.</li>
 * </ol>
 *
 * <p>지원하는 원문 형식은 세 가지이며, 위에서부터 차례로 시도한다.
 * <pre>
 * (A) OHTHandlerLogger : [2026-01-05 09:12:30.1234],[SEQ_INFO],[INFORMATION],[316710],[msg => 본문]
 * (B) UILogger         : 2026-01-05 09:12:30,123 [1] INFO APP - 본문
 * (C) 단순 형식        : 2026-01-05 09:12:30.123 [WARN] 본문        (DeviceLogSimulator 가 만드는 연습용)
 * </pre>
 * 어느 것도 맞지 않으면 시각을 수집 시각(collected_at)으로 대체하고 본문은 raw 전체로 둔다.
 * 버리지 않고 어떻게든 저장하는 것이 로그 파이프라인의 원칙이다(raw 가 그대로 남아 있으므로 나중에 재처리할 수 있다).
 */
@Component
public class LogNormalizer {

    // 공통: 날짜 시각 토큰. 구분자는 공백 또는 T, 소수부는 . 또는 , 뒤 1~9자리 (OHT 로그는 4자리를 쓴다)
    private static final String TS = "\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}(?:[.,]\\d{1,9})?";

    /** (A) OHTHandlerLogger 계열 */
    private static final Pattern HANDLER = Pattern.compile(
            "^\\[(?<time>" + TS + ")\\],\\[(?<category>[^\\]]+)\\],\\[(?<level>[^\\]]+)\\],\\[(?<seq>\\d+)\\],"
            + "\\[msg => (?<message>.*)\\]$");

    /** (B) UILogger 계열 (log4net 스타일: 시각 [스레드] 레벨 로거 - 본문) */
    private static final Pattern UILOGGER = Pattern.compile(
            "^(?<time>" + TS + ") \\[(?<thread>\\d+)\\] (?<level>[A-Z]+) (?<category>[A-Za-z0-9_]+) - (?<message>.*)$");

    /** (C) 단순 형식 */
    private static final Pattern SIMPLE = Pattern.compile(
            "^(?<time>" + TS + ")\\s+\\[?(?<level>TRACE|DEBUG|INFO|INFORMATION|WARNING|WARN|ERROR|FATAL)\\]?\\s*(?<message>.*)$");

    /** 날짜/시각 구분자(공백 또는 T)와 0~9자리 소수부를 모두 허용하는 파서. */
    private static final DateTimeFormatter TIME = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart().appendLiteral(' ').optionalEnd()
            .optionalStart().appendLiteral('T').optionalEnd()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter(Locale.ROOT);

    private final ZoneId deviceZone;
    private final Pattern deviceIdPattern;

    public LogNormalizer(@Value("${app.ingest.device-zone:Asia/Seoul}") String deviceZone,
                         @Value("${app.ingest.device-id-pattern:([A-Z]{2,5}-[0-9]{3,5})}") String deviceIdPattern) {
        this.deviceZone = ZoneId.of(deviceZone);
        this.deviceIdPattern = Pattern.compile(deviceIdPattern);
    }

    public LogEvent normalize(String source, long offset, String rawLine, Instant collectedAt) {
        String raw = stripLineEnding(rawLine);
        String deviceId = deviceIdFrom(source);

        Matcher m = HANDLER.matcher(raw);
        if (m.matches()) {
            return new LogEvent(deviceId, source, parseTime(m.group("time"), collectedAt), collectedAt, offset, raw,
                    m.group("category"), unifyLevel(m.group("level")), m.group("message"));
        }
        m = UILOGGER.matcher(raw);
        if (m.matches()) {
            return new LogEvent(deviceId, source, parseTime(m.group("time"), collectedAt), collectedAt, offset, raw,
                    m.group("category"), unifyLevel(m.group("level")), m.group("message"));
        }
        m = SIMPLE.matcher(raw);
        if (m.matches()) {
            return new LogEvent(deviceId, source, parseTime(m.group("time"), collectedAt), collectedAt, offset, raw,
                    null, unifyLevel(m.group("level")), m.group("message"));
        }
        return new LogEvent(deviceId, source, collectedAt, collectedAt, offset, raw, null, null, raw);
    }

    /** 경로에서 장비 ID 패턴을 찾고, 없으면 파일명(확장자 제외)을 쓴다. 경로가 없으면 "unknown". */
    String deviceIdFrom(String source) {
        if (source == null || source.isBlank()) {
            return "unknown";
        }
        String normalized = source.replace('\\', '/');
        Matcher m = deviceIdPattern.matcher(normalized);
        if (m.find()) {
            return m.group(1);
        }
        // Paths.get 은 쓰지 않는다. Windows 에서 "manual://dev-0001.log" 같은 URI 형태의 source 를 넘기면
        // ':' 때문에 InvalidPathException 이 난다. 마지막 '/' 뒤를 자르는 것으로 충분하다.
        String fileName = normalized.substring(normalized.lastIndexOf('/') + 1);
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    /** 장비마다 다른 레벨 표기를 통일한다. INFORMATION→INFO, WARNING→WARN, 그 외는 대문자 그대로. */
    static String unifyLevel(String level) {
        if (level == null) {
            return null;
        }
        return switch (level.toUpperCase(Locale.ROOT)) {
            case "INFORMATION", "INFO" -> "INFO";
            case "WARNING", "WARN" -> "WARN";
            case "ERR", "ERROR" -> "ERROR";
            default -> level.toUpperCase(Locale.ROOT);
        };
    }

    /** 줄 끝의 CR/LF 만 제거한다. OHT 로그 파일은 CRLF 이며, Fluent Bit 이 LF 는 떼지만 CR 은 남길 수 있다. */
    private static String stripLineEnding(String s) {
        if (s == null) {
            return "";
        }
        int end = s.length();
        while (end > 0 && (s.charAt(end - 1) == '\r' || s.charAt(end - 1) == '\n')) {
            end--;
        }
        return s.substring(0, end);
    }

    private Instant parseTime(String text, Instant fallback) {
        try {
            return LocalDateTime.parse(text.replace(',', '.'), TIME).atZone(deviceZone).toInstant();
        } catch (DateTimeParseException e) {
            return fallback;
        }
    }
}
