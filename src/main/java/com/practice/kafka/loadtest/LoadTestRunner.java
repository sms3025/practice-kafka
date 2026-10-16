package com.practice.kafka.loadtest;

import com.practice.kafka.log.LogEvent;
import com.practice.kafka.log.LogNormalizer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * 실제 로그를 정규화해 Kafka 로 보내며, 목표 처리량을 계단식으로 올려 어디까지 버티는지 잰다 (앱 프로듀서 경유 층).
 *
 * <p>수집 서버가 실제로 하는 일과 같은 일을 한다. 원문 한 줄을 {@link LogNormalizer} 로 {@link LogEvent} 로 바꾸고,
 * 앱과 같은 직렬화기로 JSON 을 만들어 device_id 를 키로 보낸다. 다른 점은 HTTP 를 거치지 않는다는 것 하나다.
 * 그래서 이 층의 숫자와 브로커 단독 층(kafka-producer-perf-test)의 숫자 차이가 "우리 코드 + 호스트에서 docker 로 들어가는 길"의 몫이 된다.
 *
 * <p>프로듀서는 앱의 {@link ProducerFactory} 를 복사해 preset 의 값만 덮어쓴 것을 실행마다 새로 만든다.
 * 앱의 KafkaTemplate 은 건드리지 않으므로 부하 테스트 중에도 원래 수집 경로의 설정은 그대로다.
 *
 * <p>속도 조절은 "지금까지 보냈어야 할 건수"를 매 1ms 계산해 모자란 만큼 보내는 방식이다. 프로듀서가 밀려
 * send() 가 버퍼 자리를 기다리며 막히면 그만큼 덜 보내게 되고, 그것이 곧 달성률 하락으로 드러난다.
 */
@Component
public class LoadTestRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadTestRunner.class);

    /** 보내는 스레드가 한 번 쉬는 시간. Windows 에서는 타이머 해상도 때문에 실제로는 1ms 보다 길 수 있다. 그만큼 몰아서 보낸다. */
    private static final long PACING_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private final ProducerFactory<String, LogEvent> producerFactory;
    private final KafkaAdmin kafkaAdmin;
    private final LogNormalizer normalizer;
    private final LoadTestCorpus corpus;
    private final String topic;
    private final int linesPerVehicle;

    private final ExecutorService runExecutor =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("loadtest-run").daemon().factory());
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private volatile Run current;

    public LoadTestRunner(ProducerFactory<String, LogEvent> producerFactory, KafkaAdmin kafkaAdmin,
                          LogNormalizer normalizer, LoadTestCorpus corpus,
                          @Value("${app.loadtest.topic}") String topic,
                          @Value("${app.loadtest.lines-per-vehicle}") int linesPerVehicle) {
        this.producerFactory = producerFactory;
        this.kafkaAdmin = kafkaAdmin;
        this.normalizer = normalizer;
        this.corpus = corpus;
        this.topic = topic;
        this.linesPerVehicle = linesPerVehicle;
    }

    /** 실행을 백그라운드에서 시작하고 바로 돌아온다. 진행은 {@link #status()} 로 본다. */
    public synchronized LoadTestStatus start(LoadTestPlan plan) {
        if (current != null && current.state.equals("RUNNING")) {
            throw new IllegalStateException("이미 부하 테스트가 돌고 있다. POST /api/loadtest/stop 으로 멈춘 뒤 다시 시작한다");
        }
        requireTopic();
        corpus.lines();   // 입력이 없으면 여기서 바로 실패를 알린다
        stopRequested.set(false);
        Run run = new Run(plan);
        current = run;
        runExecutor.execute(() -> execute(run));
        return run.snapshot();
    }

    public void stop() {
        stopRequested.set(true);
    }

    public LoadTestStatus status() {
        Run run = current;
        return run == null
                ? new LoadTestStatus("IDLE", null, null, List.of(), null, null, 0, null)
                : run.snapshot();
    }

    private void requireTopic() {
        try {
            kafkaAdmin.describeTopics(topic);
        } catch (RuntimeException e) {
            throw new IllegalStateException("토픽 " + topic + " 이 없다. scripts/loadtest/app.ps1 이 실행마다 지우고 새로 만든다."
                    + " API 를 직접 부를 때는 먼저 만든다 (docs/result/benchmark-kafka-throughput.md 실행 절차)", e);
        }
    }

    private void execute(Run run) {
        LoadTestPlan plan = run.plan;
        List<ProducerFactory<String, LogEvent>> factories = new ArrayList<>();
        List<KafkaTemplate<String, LogEvent>> templates = new ArrayList<>();
        for (int i = 0; i < plan.producers(); i++) {
            Map<String, Object> overrides = new HashMap<>(plan.preset().overrides());
            // 앱의 프로듀서와 구별되는 이름. Actuator 지표와 브로커 쪽에서 이 실행의 연결을 알아볼 수 있다.
            overrides.put(ProducerConfig.CLIENT_ID_CONFIG, "loadtest-" + plan.preset().key() + "-" + i);
            // 프로듀서 지표의 평균을 최근 30초(5초 × 6칸)로 계산하게 한다. 기본값(30초 × 2칸)이면 앞 계단의 값이 섞인다.
            // 측정용 설정이라 처리량에는 영향이 없다.
            overrides.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 5000);
            overrides.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, 6);
            ProducerFactory<String, LogEvent> factory = producerFactory.copyWithConfigurationOverride(overrides);
            factories.add(factory);
            templates.add(new KafkaTemplate<>(factory));
        }
        ExecutorService senders = Executors.newFixedThreadPool(plan.threads(),
                Thread.ofPlatform().name("loadtest-sender-", 0).factory());
        try {
            // 첫 계단이 메타데이터 조회와 연결 수립을 떠안지 않게 미리 해 둔다.
            templates.forEach(t -> t.partitionsFor(topic));
            int consecutiveFailures = 0;
            for (int rate = plan.startRate(); rate <= plan.maxRate() && !stopRequested.get(); rate += plan.stepRate()) {
                run.currentTarget = rate;
                StepResult step = runStep(templates, senders, plan, rate);
                run.steps.add(step);
                log.info("[loadtest {} t{} p{}] 목표 {}/s ({}대) → 실제 {}/s ({}%), {} MB/s, p50 {}ms p99 {}ms max {}ms,"
                                + " 오류 {}, 앱 CPU {}코어, 배치 {}B, 버퍼 대기 {} → {}",
                        plan.preset().key(), plan.threads(), plan.producers(),
                        step.targetRate(), step.vehicles(), Math.round(step.achievedRate()),
                        Math.round(step.achievedRatio() * 1000) / 10.0, Math.round(step.payloadMBps() * 10) / 10.0,
                        step.p50Ms(), step.p99Ms(), step.maxMs(), step.errors(),
                        Math.round(step.appCpuCores() * 10) / 10.0,
                        step.batchSizeAvg() == null ? "-" : Math.round(step.batchSizeAvg()),
                        step.bufferPoolWaitRatio() == null ? "-" : Math.round(step.bufferPoolWaitRatio() * 1000) / 1000.0,
                        step.passed() ? "버팀" : "못 버팀");
                consecutiveFailures = step.passed() ? 0 : consecutiveFailures + 1;
                if (consecutiveFailures >= plan.failuresToStop()) {
                    break;
                }
            }
            run.finish(stopRequested.get() ? "STOPPED" : "FINISHED", null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            run.finish("STOPPED", "interrupted");
        } catch (RuntimeException e) {
            log.error("부하 테스트 실패", e);
            run.finish("FAILED", rootMessage(e));
        } finally {
            senders.shutdownNow();
            factories.forEach(ProducerFactory::reset);   // 이 실행의 프로듀서를 닫는다
        }
    }

    private StepResult runStep(List<KafkaTemplate<String, LogEvent>> templates, ExecutorService senders,
                               LoadTestPlan plan, int target) throws InterruptedException {
        // 목표 처리량을 1대 peak 로 나눈 만큼 가상 차량을 둔다. 키 가짓수가 실제 차량 수와 같아져 파티션 분산도 실제와 같다.
        int vehicles = Math.max(1, (int) Math.round((double) target / linesPerVehicle));
        String[] vehicleIds = new String[vehicles];
        for (int i = 0; i < vehicles; i++) {
            vehicleIds[i] = "OHT-%04d".formatted(i + 1);
        }
        StepStats stats = new StepStats();

        long start = System.nanoTime();
        long measureStart = start + TimeUnit.SECONDS.toNanos(plan.warmupSeconds());
        long end = start + TimeUnit.SECONDS.toNanos(plan.stepSeconds());
        double ratePerThread = (double) target / plan.threads();
        CountDownLatch done = new CountDownLatch(plan.threads());
        for (int t = 0; t < plan.threads(); t++) {
            int threadIndex = t;
            KafkaTemplate<String, LogEvent> template = templates.get(t % templates.size());
            senders.execute(() -> {
                try {
                    sendLoop(template, threadIndex, plan.threads(), ratePerThread, vehicleIds,
                            start, measureStart, end, stats);
                } finally {
                    done.countDown();
                }
            });
        }

        // 집계 구간이 시작하는 순간의 벽시계와 CPU 시간을 잡는다.
        LockSupport.parkNanos(Math.max(0, measureStart - System.nanoTime()));
        long measureStartEpochMs = System.currentTimeMillis();
        long cpuStart = processCpuNanos();
        done.await();
        long measureEnd = System.nanoTime();
        long measureEndEpochMs = System.currentTimeMillis();
        long cpuEnd = processCpuNanos();

        // 프로듀서 지표는 버퍼를 비우기 전에 읽는다. flush 뒤에 읽으면 부하가 없는 순간의 표본이 섞인다.
        // 프로듀서가 여럿이면 각자의 값을 평균한다.
        Double batchSize = metric(templates, "batch-size-avg");
        Double recordsPerRequest = metric(templates, "records-per-request-avg");
        Double compressionRate = metric(templates, "compression-rate-avg");
        Double bufferWait = metric(templates, "bufferpool-wait-ratio");
        Double queueTime = metric(templates, "record-queue-time-avg");

        // 남은 레코드를 모두 보내고 ack 를 기다린다. 다음 계단이 이 계단의 꼬리를 떠안지 않게 한다.
        templates.forEach(KafkaTemplate::flush);
        awaitCompletion(stats);

        double seconds = (measureEnd - measureStart) / 1e9;
        double achieved = stats.measuredAcked.sum() / seconds;
        double ratio = achieved / target;
        long p99 = stats.latency.percentileMs(0.99);
        long errors = stats.errors.sum();
        double cpuCores = cpuStart < 0 ? -1 : (cpuEnd - cpuStart) / (double) (measureEnd - measureStart);
        return new StepResult(target, vehicles, achieved, ratio,
                stats.measuredBytes.sum() / seconds / (1024 * 1024),
                stats.latency.percentileMs(0.50), p99, stats.latency.maxMs(),
                errors, stats.firstError.get(), cpuCores,
                batchSize, recordsPerRequest, compressionRate, bufferWait, queueTime,
                plan.passes(ratio, p99, errors), measureStartEpochMs, measureEndEpochMs);
    }

    private void sendLoop(KafkaTemplate<String, LogEvent> template, int threadIndex, int threads, double ratePerThread,
                          String[] vehicleIds, long start, long measureStart, long end, StepStats stats) {
        List<LoadTestCorpus.Line> lines = corpus.lines();
        int size = lines.size();
        int cursor = (int) ((long) size * threadIndex / threads);   // 스레드마다 입력의 다른 지점에서 시작한다
        long vehicleSeq = threadIndex;
        long sent = 0;
        long now = System.nanoTime();
        while (now < end && !stopRequested.get()) {
            long due = (long) (ratePerThread * (now - start) / 1e9);
            while (sent < due) {
                LoadTestCorpus.Line line = lines.get(cursor);
                cursor = cursor + 1 == size ? 0 : cursor + 1;
                String vehicle = vehicleIds[(int) (vehicleSeq % vehicleIds.length)];
                vehicleSeq += threads;
                send(template, line, vehicle, measureStart, stats);
                sent++;
                // 밀려서 몰아 보내는 중에도 계단 끝을 넘기지 않게 가끔 시계를 본다.
                if ((sent & 0xFF) == 0 && System.nanoTime() >= end) {
                    break;
                }
            }
            LockSupport.parkNanos(PACING_NANOS);
            now = System.nanoTime();
        }
    }

    private void send(KafkaTemplate<String, LogEvent> template, LoadTestCorpus.Line line, String vehicle,
                      long measureStart, StepStats stats) {
        // 수집 서버가 Fluent Bit 레코드 한 건마다 하는 일과 같다 (FluentBitIngestController).
        LogEvent event = normalizer.normalize(line.sourceFor(vehicle), line.offset(), line.raw(), Instant.now());
        // 지연은 send() 를 부르기 직전부터 잰다. 직렬화와 버퍼 자리를 기다리며 막힌 시간까지 포함한다 (perf-test 와 같은 기준).
        long sentNanos = System.nanoTime();
        boolean measured = sentNanos >= measureStart;
        stats.sent.increment();
        try {
            template.send(topic, event.deviceId(), event)
                    .whenComplete((result, ex) -> stats.onComplete(measured, sentNanos, result, ex));
        } catch (RuntimeException e) {
            // send() 자체가 던지는 경우 (max.block.ms 동안 버퍼 자리나 메타데이터를 얻지 못함 등)
            stats.onComplete(measured, sentNanos, null, e);
        }
    }

    private static void awaitCompletion(StepStats stats) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(150);   // delivery.timeout.ms(120초) + 여유
        while (stats.completed.sum() < stats.sent.sum()) {
            if (System.nanoTime() > deadline) {
                stats.errors.increment();
                stats.firstError.compareAndSet(null, "ack 를 기다리다 시간 초과");
                return;
            }
            Thread.sleep(10);
        }
    }

    /** 프로듀서 내부 지표 하나를 프로듀서들에 걸쳐 평균한다. 아직 표본이 없으면(NaN) 빼고, 하나도 없으면 null. */
    private static Double metric(List<KafkaTemplate<String, LogEvent>> templates, String name) {
        double sum = 0;
        int count = 0;
        for (KafkaTemplate<String, LogEvent> template : templates) {
            for (Map.Entry<MetricName, ? extends Metric> e : template.metrics().entrySet()) {
                MetricName n = e.getKey();
                if (n.group().equals("producer-metrics") && n.name().equals(name)
                        && e.getValue().metricValue() instanceof Double d && Double.isFinite(d)) {
                    sum += d;
                    count++;
                }
            }
        }
        return count == 0 ? null : sum / count;
    }

    /** 이 JVM 이 지금까지 쓴 CPU 시간(ns). 구할 수 없으면 -1. */
    private static long processCpuNanos() {
        return ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os
                ? os.getProcessCpuTime()
                : -1;
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }

    /** 계단 하나 동안 콜백들이 함께 더하는 숫자들. */
    private static final class StepStats {
        final LongAdder sent = new LongAdder();            // send() 를 부른 건수 (워밍업 포함)
        final LongAdder completed = new LongAdder();       // 결과가 정해진 건수 (성공 + 실패)
        final LongAdder measuredAcked = new LongAdder();   // 집계 구간에 보낸 것 중 ack 를 받은 건수
        final LongAdder measuredBytes = new LongAdder();
        final LongAdder errors = new LongAdder();
        final AtomicReference<String> firstError = new AtomicReference<>();
        final LatencyHistogram latency = new LatencyHistogram();

        void onComplete(boolean measured, long sentNanos, SendResult<String, LogEvent> result, Throwable ex) {
            long now = System.nanoTime();
            if (ex != null) {
                errors.increment();
                firstError.compareAndSet(null, rootMessage(ex));
            } else if (measured) {
                RecordMetadata md = result.getRecordMetadata();
                measuredAcked.increment();
                measuredBytes.add(Math.max(0, md.serializedKeySize()) + Math.max(0, md.serializedValueSize()));
                latency.recordNanos(now - sentNanos);
            }
            completed.increment();
        }
    }

    /** 실행 한 번의 상태. 상태 조회는 다른 스레드(HTTP)에서 하므로 필드를 volatile 이나 동시성 컬렉션으로 둔다. */
    private static final class Run {
        final LoadTestPlan plan;
        final long startedEpochMs = System.currentTimeMillis();
        final List<StepResult> steps = new CopyOnWriteArrayList<>();
        volatile String state = "RUNNING";
        volatile Integer currentTarget;
        volatile String error;
        volatile Long finishedEpochMs;

        Run(LoadTestPlan plan) {
            this.plan = plan;
        }

        void finish(String finalState, String errorMessage) {
            error = errorMessage;
            currentTarget = null;
            finishedEpochMs = System.currentTimeMillis();
            state = finalState;
        }

        LoadTestStatus snapshot() {
            Integer maxPassed = steps.stream().filter(StepResult::passed)
                    .map(StepResult::targetRate).max(Integer::compare).orElse(null);
            return new LoadTestStatus(state, plan, currentTarget, List.copyOf(steps), maxPassed,
                    error, startedEpochMs, finishedEpochMs);
        }
    }
}
