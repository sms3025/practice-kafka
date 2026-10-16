package com.practice.kafka.loadtest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * 부하 테스트의 입력. 실제 OHT 로그 파일(대외비, gitignore 된 {@code tools/})을 한 번 읽어 메모리에 들고 있는다.
 *
 * <p>폴더 구조는 Fluent Bit 이 읽는 것과 같다. {@code <corpus-dir>/<장비ID>/UILogger/*.log} 와
 * {@code <corpus-dir>/<장비ID>/OHTHandlerLogger/<계열>/*.txt} 만 읽고 나머지 파일은 무시한다.
 * 줄마다 장비 폴더 아래 상대 경로와 파일 안 바이트 오프셋을 기억해 두었다가, 부하를 걸 때 장비 ID 만 가상 차량으로 바꿔
 * Fluent Bit 이 보냈을 법한 source 를 다시 만든다. 그래서 1대분 로그로 수천 대를 흉내 낼 수 있다.
 *
 * <p>줄 순서는 고정 시드로 섞는다. 파일 순서대로 보내면 한동안 긴 줄만, 한동안 짧은 줄만 나가서
 * 같은 계단 안에서도 메시지 크기가 출렁인다. 섞어 두면 어느 구간을 잘라도 크기 분포가 전체와 같고, 실행마다 순서가 같다.
 *
 * <p>읽기는 처음 쓰일 때 한다. 부하 테스트를 하지 않는 실행에서는 앱 기동이 느려지지 않고, 폴더가 없어도 앱은 뜬다.
 */
@Component
public class LoadTestCorpus {

    private static final long SHUFFLE_SEED = 42L;

    /**
     * 원문 한 줄과 그 줄이 있던 자리.
     *
     * @param relativePath 장비 폴더 아래 경로 (예: {@code UILogger/APP_20260105_09.log}). 같은 파일의 줄은 같은 String 을 공유한다
     * @param offset       파일 안에서 줄이 시작하는 바이트 위치. Fluent Bit 의 Offset_Key 와 같은 역할
     * @param raw          줄바꿈(LF)을 뗀 원문. CR 은 Fluent Bit 처럼 남겨 두고 LogNormalizer 가 지우게 한다
     */
    public record Line(String relativePath, long offset, String raw) {

        /** 이 줄을 장비 {@code deviceId} 의 에이전트가 읽었다면 붙였을 source (fluent-bit.conf 의 마운트 경로 기준). */
        public String sourceFor(String deviceId) {
            return "/var/log/devices/" + deviceId + "/" + relativePath;
        }
    }

    /** 입력의 규모. 결과 문서에 "무엇으로 쟀는가"를 적을 때 쓴다. */
    public record Summary(String dir, int files, int lines, long rawBytes, double avgRawBytes) {
    }

    private final Path dir;
    private volatile List<Line> lines;
    private volatile Summary summary;

    public LoadTestCorpus(@Value("${app.loadtest.corpus-dir}") String dir) {
        this.dir = Path.of(dir);
    }

    public List<Line> lines() {
        List<Line> loaded = lines;
        if (loaded == null) {
            synchronized (this) {
                if (lines == null) {
                    load();
                }
                loaded = lines;
            }
        }
        return loaded;
    }

    public Summary summary() {
        lines();
        return summary;
    }

    private void load() {
        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("부하 테스트 입력 폴더가 없다: " + dir.toAbsolutePath()
                    + " (app.loadtest.corpus-dir. IntelliJ 실행이면 Working directory 가 프로젝트 루트인지 확인)");
        }
        List<Line> result = new ArrayList<>();
        int files = 0;
        long rawBytes = 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path file : walk.filter(Files::isRegularFile).sorted().toList()) {
                String rel = relativeLogPath(dir.relativize(file));
                if (rel != null) {
                    files++;
                    rawBytes += readLines(file, rel, result);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (result.isEmpty()) {
            throw new IllegalStateException("부하 테스트 입력 폴더에 로그 파일이 없다: " + dir.toAbsolutePath()
                    + " (<장비ID>/UILogger/*.log 또는 <장비ID>/OHTHandlerLogger/<계열>/*.txt)");
        }
        Collections.shuffle(result, new Random(SHUFFLE_SEED));
        summary = new Summary(dir.toAbsolutePath().toString(), files, result.size(), rawBytes,
                (double) rawBytes / result.size());
        lines = List.copyOf(result);
    }

    /**
     * {@code <장비ID>/UILogger/x.log} 또는 {@code <장비ID>/OHTHandlerLogger/<계열>/x.txt} 이면 장비 폴더 아래 경로를,
     * 아니면 null 을 돌려준다. fluent-bit.conf 의 Path 패턴과 같은 규칙이다.
     */
    static String relativeLogPath(Path rel) {
        int n = rel.getNameCount();
        String file = rel.getFileName().toString();
        if (n == 3 && rel.getName(1).toString().equals("UILogger") && file.endsWith(".log")) {
            return rel.subpath(1, 3).toString().replace('\\', '/');
        }
        if (n == 4 && rel.getName(1).toString().equals("OHTHandlerLogger") && file.endsWith(".txt")) {
            return rel.subpath(1, 4).toString().replace('\\', '/');
        }
        return null;
    }

    /** 파일을 LF 로 잘라 줄과 시작 오프셋을 out 에 더하고, 읽은 원문 바이트 수(줄바꿈 제외)를 돌려준다. */
    static long readLines(Path file, String rel, List<Line> out) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long rawBytes = 0;
        int start = 0;
        for (int i = 0; i <= bytes.length; i++) {
            if (i == bytes.length || bytes[i] == '\n') {
                int end = i;
                // 빈 줄(CR 만 있는 줄 포함)은 Fluent Bit 도 건너뛴다 (fluent-bit.conf 의 Skip_Empty_Lines On)
                boolean empty = end == start || (end - start == 1 && bytes[start] == '\r');
                if (!empty) {
                    out.add(new Line(rel, start, new String(bytes, start, end - start, StandardCharsets.UTF_8)));
                    rawBytes += end - start;
                }
                start = i + 1;
            }
        }
        return rawBytes;
    }
}
