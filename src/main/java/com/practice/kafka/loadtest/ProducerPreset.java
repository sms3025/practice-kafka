package com.practice.kafka.loadtest;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Map;

/**
 * 부하 테스트에서 비교하는 프로듀서 설정 세 가지. application.properties 의 producer 설정 위에 이 값만 덮어쓴다.
 *
 * <p>브로커 단독 층을 재는 {@code scripts/loadtest/common.ps1} 의 {@code $Presets} 와 이름·값이 같아야
 * 두 층의 결과를 나란히 놓고 비교할 수 있다. 하나를 바꾸면 다른 쪽도 바꾼다.
 */
public enum ProducerPreset {

    /**
     * application.properties 의 기본값. acks=all, batch.size=16384(16KB), linger.ms=5, 압축 없음.
     * 값을 직접 적는다. "덮어쓰지 않음"으로 두면 앱을 다른 설정으로 띄운 환경(부하 테스트 compose 의 ingest 는 128KB/20ms)에서
     * 기준선이 그 설정을 물려받는다. 2026-10-16 에 그렇게 잘못 잰 일이 있었다.
     */
    BASELINE("baseline", Map.of(
            ProducerConfig.BATCH_SIZE_CONFIG, 16384,
            ProducerConfig.LINGER_MS_CONFIG, 5,
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "none")),

    /**
     * 배치를 키운다. 같은 파티션으로 가는 레코드를 128KB 까지, 최대 20ms 기다려 모은다.
     * 요청 하나에 실리는 레코드가 늘어 요청 수와 요청마다 기다리는 왕복 시간이 줄어든다.
     */
    BATCH("batch", Map.of(
            ProducerConfig.BATCH_SIZE_CONFIG, 131072,
            ProducerConfig.LINGER_MS_CONFIG, 20,
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "none")),

    /**
     * BATCH 에 lz4 압축을 더한다. 압축은 배치 단위로 하므로 배치가 클수록 잘 줄어든다.
     * 보내는 쪽 CPU 를 쓰는 대신 네트워크와 브로커 디스크로 가는 바이트가 준다.
     */
    BATCH_LZ4("batch-lz4", Map.of(
            ProducerConfig.BATCH_SIZE_CONFIG, 131072,
            ProducerConfig.LINGER_MS_CONFIG, 20,
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"));

    private final String key;
    private final Map<String, Object> overrides;

    ProducerPreset(String key, Map<String, Object> overrides) {
        this.key = key;
        this.overrides = overrides;
    }

    public String key() {
        return key;
    }

    public Map<String, Object> overrides() {
        return overrides;
    }

    public static ProducerPreset of(String key) {
        return Arrays.stream(values())
                .filter(p -> p.key.equals(key))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "알 수 없는 preset: " + key + " (baseline, batch, batch-lz4 중 하나)"));
    }
}
