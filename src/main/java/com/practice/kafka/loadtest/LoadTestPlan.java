package com.practice.kafka.loadtest;

/**
 * 계단식 부하 테스트 한 번의 설계.
 *
 * <p>startRate 줄/s 에서 시작해 계단마다 stepRate 씩 올린다. 계단 하나는 stepSeconds 동안 돌고, 앞 warmupSeconds 는
 * 연결 수립·JIT·버퍼 채우기가 섞이므로 집계에서 뺀다. 아래 세 조건을 모두 만족한 계단을 "버텼다"고 본다.
 * <ol>
 *   <li>실제 처리량(브로커 ack 를 받은 줄 수 ÷ 집계 시간)이 목표의 minAchievedRatio 이상</li>
 *   <li>전송에서 ack 까지 p99 지연이 maxP99Ms 이하</li>
 *   <li>전송 오류 0건</li>
 * </ol>
 * 못 버틴 계단이 failuresToStop 번 연속되면 멈춘다. 한 번은 우연(GC, 다른 프로그램)일 수 있으므로 기본값은 2다.
 *
 * @param threads   동시에 보내는 스레드 수. 수집 서버에서 여러 HTTP 요청이 동시에 send() 하는 상황을 흉내 낸다
 * @param producers 프로듀서(KafkaProducer) 개수. 기본 1은 앱의 KafkaTemplate 과 같이 프로듀서 하나를 모든 스레드가 함께 쓴다.
 *                  프로듀서마다 브로커 연결을 따로 가지므로, 연결 하나의 대역폭이 한계인지 볼 때 늘린다. 스레드 i 는 프로듀서 i % producers 를 쓴다
 */
public record LoadTestPlan(
        ProducerPreset preset,
        int startRate,
        int stepRate,
        int maxRate,
        int stepSeconds,
        int warmupSeconds,
        int threads,
        int producers,
        int failuresToStop,
        double minAchievedRatio,
        long maxP99Ms
) {

    public LoadTestPlan {
        if (startRate <= 0 || stepRate <= 0 || maxRate < startRate) {
            throw new IllegalArgumentException("startRate > 0, stepRate > 0, maxRate >= startRate 여야 한다");
        }
        if (warmupSeconds < 0 || stepSeconds <= warmupSeconds) {
            throw new IllegalArgumentException("stepSeconds 는 warmupSeconds 보다 길어야 한다");
        }
        if (threads <= 0 || failuresToStop <= 0) {
            throw new IllegalArgumentException("threads, failuresToStop 은 1 이상이어야 한다");
        }
        if (producers <= 0 || producers > threads) {
            throw new IllegalArgumentException("producers 는 1 이상 threads 이하여야 한다 (스레드가 없는 프로듀서는 놀게 된다)");
        }
    }

    boolean passes(double achievedRatio, long p99Ms, long errors) {
        return achievedRatio >= minAchievedRatio && p99Ms <= maxP99Ms && errors == 0;
    }
}
