package com.practice.kafka.log;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Kafka 토픽을 오가는 로그 이벤트 한 건. 기업 명세서의 공통 JSON 스키마를 따른다.
 *
 * <p>JSON 필드명은 명세서대로 snake_case 이며, Java 필드명은 camelCase 이므로
 * {@link JsonProperty}로 둘을 매핑한다. (Jackson 3 에서도 어노테이션 패키지는 그대로다)
 *
 * <p>필수 필드 6개(device_id ~ raw)는 명세서가 정한 것이고, {@code category}, {@code level}, {@code message}는
 * "본문 상세 필드 파싱·정규화"(추가 구현 기능)에 해당하는 부가 필드다. 파싱에 실패하면 null 이 들어간다.
 *
 * @param deviceId    장비 식별자 (예: OHT-0001). Kafka 메시지 키로 사용 → 같은 장비의 로그는 같은 파티션에서 순서대로 처리된다.
 * @param source      원본 파일 경로 (에이전트가 붙여준다)
 * @param eventTime   원문에서 추출한 이벤트 발생 시각. 장비마다 제각각인 시각 표기를 UTC Instant 로 정규화한 값
 * @param collectedAt 에이전트가 수집한 시각
 * @param offset      원본 파일 내 바이트 오프셋. (deviceId, source, offset) 조합이 멱등키가 된다
 * @param raw         원문 원형(줄 끝 CR/LF 만 제거). 전문 검색 대상이며 그 외에는 가공하지 않는다
 * @param category    로그 계열/카테고리. UILogger 는 로거 이름(APP, OHT, OPS, NET, IO), OHTHandlerLogger 는 SEQ_INFO 등. 없으면 null
 * @param level       raw 에서 파싱해 통일한 로그 레벨 (INFO/WARN/ERROR/DEBUG/TRACE/FATAL). 없으면 null
 * @param message     raw 에서 시각·레벨 등 머리 부분을 떼어낸 본문 (없으면 raw 와 동일)
 */
public record LogEvent(
        @JsonProperty("device_id") String deviceId,
        @JsonProperty("source") String source,
        @JsonProperty("event_time") Instant eventTime,
        @JsonProperty("collected_at") Instant collectedAt,
        @JsonProperty("offset") long offset,
        @JsonProperty("raw") String raw,
        @JsonProperty("category") String category,
        @JsonProperty("level") String level,
        @JsonProperty("message") String message
) {

    /**
     * 무중복 멱등키. 같은 파일의 같은 위치에서 읽은 줄은 몇 번 전송되더라도 같은 키를 가진다.
     * 에이전트의 재전송(at-least-once)으로 인한 중복을 저장소에서 걸러낼 때 쓴다.
     */
    public String idempotencyKey() {
        return deviceId + "|" + source + "|" + offset;
    }
}
