package com.practice.kafka.loadtest;

/**
 * 계단 하나의 측정 결과. 모든 값은 워밍업을 뺀 집계 구간의 것이다.
 *
 * @param targetRate          목표 처리량(줄/s)
 * @param vehicles            목표를 차량 1대 peak(app.loadtest.lines-per-vehicle)로 나눈 가상 차량 대수. 메시지 키의 가짓수이기도 하다
 * @param achievedRate        브로커 ack 를 받은 줄 수 ÷ 집계 시간
 * @param achievedRatio       achievedRate ÷ targetRate
 * @param payloadMBps         직렬화된 키+값 바이트 기준 MB/s (압축 전). 1MB = 1,048,576바이트로 perf-test 와 같다
 * @param p50Ms               전송에서 ack 까지 지연의 가운데 값
 * @param p99Ms               같은 지연의 99번째 백분위
 * @param maxMs               같은 지연의 최댓값
 * @param errors              계단 전체(워밍업 포함)의 전송 실패 건수
 * @param firstError          첫 실패의 예외 메시지. 없으면 null
 * @param appCpuCores         집계 구간 동안 앱 JVM 이 쓴 CPU. 1.0 = 코어 하나를 꽉 쓴 것
 * @param batchSizeAvg        프로듀서 지표 batch-size-avg. 파티션별 배치 하나의 평균 바이트(압축 후)
 * @param recordsPerRequest   프로듀서 지표 records-per-request-avg. 요청 하나에 실린 레코드 수
 * @param compressionRate     프로듀서 지표 compression-rate-avg. 압축 후 ÷ 압축 전. 1이면 압축 안 함
 * @param bufferPoolWaitRatio 프로듀서 지표 bufferpool-wait-ratio. send() 가 버퍼 자리를 기다리며 막힌 시간의 비율. 0보다 크면 프로듀서가 밀리고 있다
 * @param recordQueueTimeMs   프로듀서 지표 record-queue-time-avg. 레코드가 배치에 담겨 전송되기까지 버퍼에서 기다린 평균 시간
 * @param passed              LoadTestPlan 의 세 조건을 모두 만족했는가
 * @param measureStartEpochMs 집계 구간 시작 시각. 스크립트가 docker stats 표본을 이 구간으로 잘라 브로커 CPU 를 구한다
 * @param measureEndEpochMs   집계 구간 끝 시각
 */
public record StepResult(
        int targetRate,
        int vehicles,
        double achievedRate,
        double achievedRatio,
        double payloadMBps,
        long p50Ms,
        long p99Ms,
        long maxMs,
        long errors,
        String firstError,
        double appCpuCores,
        Double batchSizeAvg,
        Double recordsPerRequest,
        Double compressionRate,
        Double bufferPoolWaitRatio,
        Double recordQueueTimeMs,
        boolean passed,
        long measureStartEpochMs,
        long measureEndEpochMs
) {
}
