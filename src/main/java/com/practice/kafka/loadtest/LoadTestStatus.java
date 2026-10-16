package com.practice.kafka.loadtest;

import java.util.List;

/**
 * {@code GET /api/loadtest/status} 의 응답. 실행 중에도 끝난 계단까지의 결과를 볼 수 있다.
 *
 * @param state         RUNNING, FINISHED(조건대로 끝남), STOPPED(stop 요청), FAILED(예외)
 * @param currentTarget 지금 돌고 있는 계단의 목표 처리량. 끝났으면 null
 * @param maxPassedRate 버틴 계단 중 가장 높은 목표 처리량. 하나도 못 버텼으면 null
 */
public record LoadTestStatus(
        String state,
        LoadTestPlan plan,
        Integer currentTarget,
        List<StepResult> steps,
        Integer maxPassedRate,
        String error,
        long startedEpochMs,
        Long finishedEpochMs
) {
}
