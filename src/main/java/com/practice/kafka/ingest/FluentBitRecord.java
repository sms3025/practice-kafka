package com.practice.kafka.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Fluent Bit HTTP output(format=json)이 보내는 레코드 한 건.
 *
 * <p>Fluent Bit tail 입력은 기본적으로 {@code log} 키에 원문 한 줄을 담고,
 * 우리가 fluent-bit.conf 에서 {@code Path_Key source}, {@code Offset_Key offset},
 * {@code Json_date_key collected_at} 로 지정한 키가 추가된다. 실제 요청 본문 예:
 * <pre>
 * [
 *   {"collected_at":"2026-10-12T05:03:21.123456Z",
 *    "log":"2026-10-12 14:03:21.123 [WARN] ECU temperature high: 97C",
 *    "source":"/var/log/devices/dev-0007.log",
 *    "offset":1842},
 *   ...
 * ]
 * </pre>
 * 알 수 없는 키는 무시된다(Jackson 3 기본값).
 */
public record FluentBitRecord(
        @JsonProperty("collected_at") Instant collectedAt,
        @JsonProperty("log") String log,
        @JsonProperty("source") String source,
        @JsonProperty("offset") Long offset
) {
}
