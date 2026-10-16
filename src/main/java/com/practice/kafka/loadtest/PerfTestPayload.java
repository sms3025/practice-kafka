package com.practice.kafka.loadtest;

import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * 브로커 단독 층(kafka-producer-perf-test)이 보낼 메시지 파일을 만든다.
 *
 * <p>perf-test 는 {@code --payload-file} 의 줄 하나를 메시지 하나로 쓴다. 여기에 앱이 실제로 보내는 것과 같은 바이트,
 * 즉 실제 로그를 {@link LogNormalizer} 로 정규화하고 앱과 같은 {@link JacksonJsonSerializer} 로 만든 JSON 을 한 줄씩 적는다.
 * 그러면 두 층이 크기와 내용이 같은 메시지로 재게 되어, 남는 차이는 보내는 쪽(우리 코드, 키, 네트워크 경로)의 차이뿐이다.
 *
 * <p>파일에는 실제 로그 내용이 들어 있으므로 대외비다. 기본 위치가 gitignore 된 {@code build/} 아래인 이유이며,
 * {@code gradlew clean} 이 함께 지운다.
 */
@Component
public class PerfTestPayload {

    /**
     * @param file         쓴 파일의 절대 경로
     * @param lines        메시지 수
     * @param bytes        JSON 바이트 합 (줄바꿈 제외)
     * @param avgJsonBytes 메시지 하나의 평균 바이트. 결과 문서의 "메시지 크기"
     * @param corpus       입력 원문의 규모
     */
    public record Written(String file, int lines, long bytes, double avgJsonBytes, LoadTestCorpus.Summary corpus) {
    }

    private final LoadTestCorpus corpus;
    private final LogNormalizer normalizer;
    private final Path file;
    private final String topic;

    public PerfTestPayload(LoadTestCorpus corpus, LogNormalizer normalizer,
                           @Value("${app.loadtest.payload-file}") String file,
                           @Value("${app.loadtest.topic}") String topic) {
        this.corpus = corpus;
        this.normalizer = normalizer;
        this.file = Path.of(file);
        this.topic = topic;
    }

    public Written write(int vehicles) {
        List<LoadTestCorpus.Line> lines = corpus.lines();
        Instant collectedAt = Instant.now();
        long bytes = 0;
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            try (JacksonJsonSerializer<LogEvent> serializer = new JacksonJsonSerializer<>();
                 OutputStream out = new BufferedOutputStream(Files.newOutputStream(file), 1 << 20)) {
                for (int i = 0; i < lines.size(); i++) {
                    LoadTestCorpus.Line line = lines.get(i);
                    String vehicle = "OHT-%04d".formatted(i % vehicles + 1);
                    byte[] json = serializer.serialize(topic,
                            normalizer.normalize(line.sourceFor(vehicle), line.offset(), line.raw(), collectedAt));
                    // JSON 문자열 안의 줄바꿈은 \n 으로 이스케이프되므로, 실제 줄바꿈은 메시지 경계에만 생긴다.
                    out.write(json);
                    out.write('\n');
                    bytes += json.length;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Written(file.toAbsolutePath().toString(), lines.size(), bytes,
                (double) bytes / lines.size(), corpus.summary());
    }
}
