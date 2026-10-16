package com.practice.kafka.loadtest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 부하 테스트 API. 보통은 직접 부르지 않고 {@code scripts/loadtest/app.ps1} 이 토픽을 새로 만든 뒤 부른다.
 *
 * <ul>
 *   <li>POST /api/loadtest/start?preset=baseline : 계단식 부하를 백그라운드로 시작 (202)</li>
 *   <li>GET  /api/loadtest/status                : 끝난 계단까지의 결과</li>
 *   <li>POST /api/loadtest/stop                  : 지금 계단을 끝으로 멈춤</li>
 *   <li>POST /api/loadtest/payload               : 브로커 단독 층(perf-test)이 보낼 메시지 파일 생성</li>
 *   <li>GET  /api/loadtest/corpus                : 입력(실제 로그)의 파일 수, 줄 수, 평균 크기</li>
 *   <li>POST /api/loadtest/fluentbit-batches     : 장비 → 수집 서버 구간(k6)이 보낼 요청 본문 파일 생성</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/loadtest")
public class LoadTestController {

    private final LoadTestRunner runner;
    private final PerfTestPayload payload;
    private final LoadTestCorpus corpus;
    private final FluentBitBatches batches;

    public LoadTestController(LoadTestRunner runner, PerfTestPayload payload, LoadTestCorpus corpus,
                              FluentBitBatches batches) {
        this.runner = runner;
        this.payload = payload;
        this.corpus = corpus;
        this.batches = batches;
    }

    @PostMapping("/start")
    public ResponseEntity<LoadTestStatus> start(@RequestParam(defaultValue = "baseline") String preset,
                                                @RequestParam(defaultValue = "100000") int startRate,
                                                @RequestParam(defaultValue = "50000") int stepRate,
                                                @RequestParam(defaultValue = "1000000") int maxRate,
                                                @RequestParam(defaultValue = "30") int stepSeconds,
                                                @RequestParam(defaultValue = "5") int warmupSeconds,
                                                @RequestParam(defaultValue = "4") int threads,
                                                @RequestParam(defaultValue = "1") int producers,
                                                @RequestParam(defaultValue = "2") int failuresToStop,
                                                @RequestParam(defaultValue = "0.95") double minAchievedRatio,
                                                @RequestParam(defaultValue = "1000") long maxP99Ms) {
        LoadTestPlan plan = new LoadTestPlan(ProducerPreset.of(preset), startRate, stepRate, maxRate,
                stepSeconds, warmupSeconds, threads, producers, failuresToStop, minAchievedRatio, maxP99Ms);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(runner.start(plan));
    }

    @GetMapping("/status")
    public LoadTestStatus status() {
        return runner.status();
    }

    @PostMapping("/stop")
    public LoadTestStatus stop() {
        runner.stop();
        return runner.status();
    }

    /** vehicles 는 메시지 안 device_id 의 가짓수. perf-test 는 키 없이 보내므로 크기만 같으면 되고, 기본값은 10만 줄/s 몫이다. */
    @PostMapping("/payload")
    public PerfTestPayload.Written payload(@RequestParam(defaultValue = "1667") int vehicles) {
        return payload.write(vehicles);
    }

    @GetMapping("/corpus")
    public LoadTestCorpus.Summary corpus() {
        return corpus.summary();
    }

    /**
     * 장비 → 수집 서버 구간(k6)이 보낼 요청 본문 파일 생성. mode 는 per-file(지금 Fluent Bit 설정) 또는 per-device.
     * devices 는 본문 안 장비 ID 의 가짓수다. 실제 부하의 대수는 k6 의 요청 속도로 정하므로 넉넉하면 된다.
     */
    @PostMapping("/fluentbit-batches")
    public FluentBitBatches.Written fluentBitBatches(@RequestParam(defaultValue = "per-file") String mode,
                                                     @RequestParam(defaultValue = "20000") int devices) {
        return batches.write(FluentBitBatches.Mode.of(mode), devices);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail conflict(IllegalStateException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
