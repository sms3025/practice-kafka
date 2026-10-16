package com.practice.kafka.loadtest;

import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/**
 * 장비 → 수집 서버 구간(HTTP)의 부하 입력. 실제 로그를 Fluent Bit 이 보냈을 요청 본문으로 바꿔 파일에 적는다. k6 가 이 파일을 읽어 보낸다.
 *
 * <p>Fluent Bit 은 레코드를 태그별 묶음(chunk)에 모으고, Flush 주기(1초)마다 묶음 하나를 HTTP 요청 하나로 보낸다.
 * 지금 {@code fluent-bit.conf} 는 {@code Tag device.*} 라서 파일 경로가 태그가 된다. 그러니 장비 한 대는 1초에
 * "그 1초 동안 새 줄이 생긴 파일 수"만큼 요청을 보낸다. 이 클래스는 실제 로그의 이벤트 시각을 1초 단위로 잘라 그 묶음을 재현한다.
 * <ul>
 *   <li>{@code per-file}: (초, 파일)마다 요청 하나. 지금 설정의 동작이다</li>
 *   <li>{@code per-device}: 초마다 요청 하나(그 초의 모든 파일). 태그를 하나로 고정하면 이렇게 된다. 요청 수를 줄이는 대안을 재 보기 위함</li>
 * </ul>
 * 로그는 1대분이므로, 실제 1대의 "1초"를 가상 장비 한 대의 "1초"로 쓴다. k초째 묶음은 장비 {@code OHT-((k % devices) + 1)} 의 것이 된다.
 *
 * <p>레코드에 {@code collected_at} 은 넣지 않는다. 수집 서버가 받은 시각으로 채우므로, 저장소에서 {@code inserted_at - collected_at} 이
 * "수집 서버가 받은 뒤 저장되기까지" 걸린 시간이 된다. 파일에는 실제 로그 내용이 들어 있으므로 대외비다(gitignore 된 {@code build/}).
 */
@Component
public class FluentBitBatches {

    public enum Mode {
        PER_FILE("per-file"), PER_DEVICE("per-device");

        private final String key;

        Mode(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }

        public static Mode of(String key) {
            for (Mode m : values()) {
                if (m.key.equals(key)) {
                    return m;
                }
            }
            throw new IllegalArgumentException("알 수 없는 mode: " + key + " (per-file, per-device 중 하나)");
        }
    }

    /**
     * 만든 파일과 그 규모. 스크립트가 "장비 N대"를 "초당 요청 수"로 바꿀 때 requestsPerDeviceSecond 를 쓴다.
     *
     * @param deviceSeconds           실제 로그에서 줄이 하나라도 있는 초의 수 (= 가상 장비-초의 수)
     * @param requestsPerDeviceSecond 장비 한 대가 1초에 보내는 평균 요청 수
     * @param linesPerDeviceSecond    장비 한 대가 1초에 보내는 평균 줄 수
     * @param avgBodyBytes            요청 본문 평균 바이트 (압축 전)
     * @param gzipRatio               요청마다 따로 gzip 했을 때 압축 후 ÷ 압축 전. 본문이 작을수록 덜 줄어든다
     */
    public record Written(String file, String mode, int devices, int requests, int deviceSeconds,
                          double requestsPerDeviceSecond, double linesPerDeviceSecond, double linesPerRequest,
                          double avgBodyBytes, double gzipRatio) {
    }

    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** 시각을 못 읽은 줄을 알아보기 위한 표시. 그런 줄은 같은 파일의 바로 앞 줄과 같은 초에 넣는다. */
    private static final Instant UNPARSED = Instant.EPOCH;

    private final LoadTestCorpus corpus;
    private final LogNormalizer normalizer;
    private final Path dir;

    public FluentBitBatches(LoadTestCorpus corpus, LogNormalizer normalizer,
                            @Value("${app.loadtest.payload-file}") String payloadFile) {
        this.corpus = corpus;
        this.normalizer = normalizer;
        this.dir = Path.of(payloadFile).toAbsolutePath().getParent();
    }

    public Written write(Mode mode, int devices) {
        Map<Long, Map<String, List<LoadTestCorpus.Line>>> bySecond = groupBySecond(corpus.lines());
        Path file = dir.resolve("fluentbit-" + mode.key() + ".jsonl");
        int requests = 0;
        long lines = 0;
        long bodyBytes = 0;
        long gzipBytes = 0;
        try {
            Files.createDirectories(dir);
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file), 1 << 20)) {
                int k = 0;
                for (Map<String, List<LoadTestCorpus.Line>> files : bySecond.values()) {
                    String device = "OHT-%04d".formatted(k++ % devices + 1);
                    List<List<LoadTestCorpus.Line>> bodies = mode == Mode.PER_FILE
                            ? new ArrayList<>(files.values())
                            : List.of(files.values().stream().flatMap(List::stream).toList());
                    for (List<LoadTestCorpus.Line> body : bodies) {
                        byte[] json = toRequestBody(body, device);
                        // 한 줄 = 요청 하나. JSON 문자열 안의 줄바꿈은 \n 으로 이스케이프되므로 실제 줄바꿈은 경계에만 있다.
                        out.write(json);
                        out.write('\n');
                        requests++;
                        lines += body.size();
                        bodyBytes += json.length;
                        gzipBytes += gzipSize(json);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int deviceSeconds = bySecond.size();
        return new Written(file.toString(), mode.key(), devices, requests, deviceSeconds,
                (double) requests / deviceSeconds, (double) lines / deviceSeconds, (double) lines / requests,
                (double) bodyBytes / requests, (double) gzipBytes / bodyBytes);
    }

    /** 초 → (파일 → 그 초에 그 파일에 쓰인 줄들, 파일 안 순서대로). */
    Map<Long, Map<String, List<LoadTestCorpus.Line>>> groupBySecond(List<LoadTestCorpus.Line> shuffled) {
        // 입력은 섞여 있으므로 파일별로 모아 오프셋 순서(= 기록 순서)로 되돌린다.
        Map<String, List<LoadTestCorpus.Line>> byFile = new TreeMap<>();
        for (LoadTestCorpus.Line line : shuffled) {
            byFile.computeIfAbsent(line.relativePath(), f -> new ArrayList<>()).add(line);
        }
        Map<Long, Map<String, List<LoadTestCorpus.Line>>> bySecond = new TreeMap<>();
        for (Map.Entry<String, List<LoadTestCorpus.Line>> e : byFile.entrySet()) {
            List<LoadTestCorpus.Line> lines = e.getValue();
            lines.sort(Comparator.comparingLong(LoadTestCorpus.Line::offset));
            long second = Long.MIN_VALUE;
            for (LoadTestCorpus.Line line : lines) {
                LogEvent event = normalizer.normalize(line.sourceFor("OHT-0001"), line.offset(), line.raw(), UNPARSED);
                if (!event.eventTime().equals(UNPARSED)) {
                    second = event.eventTime().getEpochSecond();
                }
                if (second == Long.MIN_VALUE) {
                    continue;   // 파일 첫머리부터 시각이 없는 줄은 어느 초에 넣을지 알 수 없어 뺀다
                }
                bySecond.computeIfAbsent(second, s -> new LinkedHashMap<>())
                        .computeIfAbsent(e.getKey(), f -> new ArrayList<>())
                        .add(line);
            }
        }
        return bySecond;
    }

    /** Fluent Bit http 출력(format json)과 같은 모양: [{"log":..., "source":..., "offset":...}, ...] */
    private static byte[] toRequestBody(List<LoadTestCorpus.Line> lines, String device) {
        List<Map<String, Object>> records = new ArrayList<>(lines.size());
        for (LoadTestCorpus.Line line : lines) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("log", line.raw());
            r.put("source", line.sourceFor(device));
            r.put("offset", line.offset());
            records.add(r);
        }
        return JSON.writeValueAsBytes(records);
    }

    private static int gzipSize(byte[] body) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(body.length / 4 + 64);
        try (GZIPOutputStream gz = new GZIPOutputStream(buf)) {
            gz.write(body);
        }
        return buf.size();
    }
}
