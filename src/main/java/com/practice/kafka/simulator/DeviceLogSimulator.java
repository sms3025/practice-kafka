package com.practice.kafka.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * "차량/장비가 로컬에 .txt 로그를 쌓는다" 는 상황을 흉내 내는 시뮬레이터.
 *
 * <p>{@code app.simulator.dir} 아래에 장비 수만큼 {@code dev-0001.log ... } 파일을 만들고 줄을 계속 덧붙인다.
 * 이 디렉터리는 docker-compose 에서 Fluent Bit 컨테이너에 마운트되어 있으므로,
 * 여기 쓰인 줄은 Fluent Bit → HTTP → {@code /ingest/fluentbit} → Kafka 로 흘러간다.
 *
 * <p>원문 형식은 실제 임베디드 장비 로그를 참고해 {@code yyyy-MM-dd HH:mm:ss.SSS [LEVEL] message} 로 두었다.
 * 장비 시각은 로컬 타임존(Asia/Seoul)으로 찍히며, 수집 서버의 LogNormalizer 가 이를 UTC 로 정규화한다.
 */
@Component
public class DeviceLogSimulator {

    private static final Logger log = LoggerFactory.getLogger(DeviceLogSimulator.class);
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final List<String> MESSAGES = List.of(
            "ENGINE rpm=%d temp=%dC",
            "GPS fix acquired lat=37.%d lon=127.%d",
            "CAN bus frame id=0x%03X dropped",
            "BATTERY voltage=%d.%dV",
            "SENSOR vibration=%d.%dg threshold exceeded",
            "FW update check: version 2.%d.%d",
            "DOOR state changed: %d",
            "ECU watchdog reset count=%d");


    private final Path dir;
    private final ZoneId zone;
    /** 장비 수와 초당 줄 수. 설정값으로 시작하고, {@link #start} 로 실행 중에 바꿀 수 있다(Step 4+ 비교 실험). */
    private volatile int deviceCount;
    private volatile int linesPerSecond;
    /** 이 시각(epoch ms)까지 1초마다 쓴다. 0 이면 멈춤. app.simulator.enabled=true 면 끝없이. */
    private volatile long runUntilMillis;
    private final AtomicLong written = new AtomicLong();

    public DeviceLogSimulator(@Value("${app.simulator.dir}") String dir,
                              @Value("${app.simulator.devices}") int deviceCount,
                              @Value("${app.simulator.lines-per-second}") int linesPerSecond,
                              @Value("${app.simulator.enabled}") boolean enabled,
                              @Value("${app.ingest.device-zone:Asia/Seoul}") String zone) {
        this.dir = Path.of(dir);
        this.deviceCount = deviceCount;
        this.linesPerSecond = linesPerSecond;
        this.runUntilMillis = enabled ? Long.MAX_VALUE : 0;
        this.zone = ZoneId.of(zone);
    }

    /** 켜져 있으면 1초마다 lines-per-second 줄을 여러 장비 파일에 나눠 쓴다. */
    @Scheduled(fixedRate = 1000)
    public void tick() {
        if (System.currentTimeMillis() < runUntilMillis) {
            writeBurst(linesPerSecond);
        }
    }

    /**
     * 장비 devices 대에 초당 linesPerSecond 줄을 seconds 초 동안 쓴다 (Step 4+ 평시 부하).
     * 장비 1대가 초당 60줄을 쓰는 실제 차량에 맞추려면 linesPerSecond = devices x 60 으로 준다.
     */
    public void start(int devices, int linesPerSecond, int seconds) {
        this.deviceCount = devices;
        this.linesPerSecond = linesPerSecond;
        this.runUntilMillis = System.currentTimeMillis() + seconds * 1000L;
        log.info("시뮬레이터 시작: 장비 {}대, 초당 {}줄, {}초", devices, linesPerSecond, seconds);
    }

    public void stop() {
        this.runUntilMillis = 0;
    }

    /** 지금까지 쓴 줄 수 (앱이 뜬 뒤 누적). 적재 경로별로 빠짐없이 들어갔는지 맞춰 보는 기준이다. */
    public long writtenCount() {
        return written.get();
    }

    public boolean running() {
        return System.currentTimeMillis() < runUntilMillis;
    }

    /**
     * 즉시 count 줄을 쓴다. 줄마다 장비를 골라 장비별로 모은 뒤 파일마다 한 번에 덧붙인다.
     * 줄마다 파일을 열고 닫으면 초당 수천 줄에서 쓰기가 밀린다(Windows 바인드 마운트에서 특히).
     */
    public int writeBurst(int count) {
        try {
            Files.createDirectories(dir);
            var rnd = ThreadLocalRandom.current();
            int devices = deviceCount;
            Map<String, StringBuilder> byDevice = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String deviceId = String.format("dev-%04d", rnd.nextInt(devices) + 1);
                byDevice.computeIfAbsent(deviceId, k -> new StringBuilder())
                        .append(randomLine()).append(System.lineSeparator());
            }
            for (Map.Entry<String, StringBuilder> e : byDevice.entrySet()) {
                Files.writeString(dir.resolve(e.getKey() + ".log"), e.getValue(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            written.addAndGet(count);
            log.debug("시뮬레이터: {}줄 기록 → {}", count, dir.toAbsolutePath());
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String randomLine() {
        var rnd = ThreadLocalRandom.current();
        int r = rnd.nextInt(100);
        String level = r < 65 ? "INFO" : r < 85 ? "DEBUG" : r < 95 ? "WARN" : "ERROR";
        String template = MESSAGES.get(rnd.nextInt(MESSAGES.size()));
        String message = String.format(template, rnd.nextInt(1, 4000), rnd.nextInt(1, 120), rnd.nextInt(1, 999));
        // 일부 장비는 시각을 안 찍는 "지저분한" 로그를 남긴다고 가정 (정규화 실패 경로 테스트용)
        if (rnd.nextInt(100) < 3) {
            return level + " " + message;
        }
        return TS.format(LocalDateTime.now(zone)) + " [" + level + "] " + message;
    }
}
