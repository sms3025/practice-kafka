# =============================================================================
# 앱 프로듀서 경유 층 부하 테스트. 떠 있는 앱의 POST /api/loadtest/start 를 부르고, 계단이 끝날 때마다 결과를 받는다.
#
# 앱(LoadTestRunner)은 실제 로그를 LogNormalizer 로 정규화하고 앱과 같은 직렬화기로 device_id 키를 붙여 보낸다.
# 호스트(Windows)의 앱에서 localhost:9092 로 나가므로 Docker Desktop 의 포트 전달(com.docker.backend)을 거친다.
# 이 스크립트는 앱이 모르는 두 숫자를 옆에서 잰다: 브로커 컨테이너 CPU(docker stats)와 포트 전달 프로세스 CPU.
#
# 준비: docker compose 가 떠 있고, 앱이 떠 있다 (IntelliJ 또는 gradlew.bat bootRun).
# 실행: .\scripts\loadtest\app.ps1 -Preset baseline      (batch, batch-lz4 도 같은 방식)
# 중간에 멈추려면 Ctrl+C. 앱 쪽 실행도 함께 멈춘다.
# =============================================================================
param(
    [ValidateSet('baseline', 'batch', 'batch-lz4')][string]$Preset = 'baseline',
    [int]$StartRate = 100000,
    [int]$StepRate = 50000,
    [int]$MaxRate = 1000000,
    [int]$StepSeconds = 30,
    [int]$WarmupSeconds = 5,
    [int]$FailuresToStop = 2,
    [int]$Threads = 4,
    # 프로듀서(브로커 연결) 개수. 기본 1은 앱의 KafkaTemplate 과 같다. 스레드 i 는 프로듀서 i % Producers 를 쓴다.
    [int]$Producers = 1,
    [int]$Partitions = 3,
    [string]$Topic = 'loadtest-logs',
    [string]$App = 'http://localhost:8080',
    [switch]$KeepTopic
)
. "$PSScriptRoot\common.ps1"

try { Invoke-RestMethod "$App/actuator/health" -TimeoutSec 5 | Out-Null }
catch { throw "앱($App)이 응답하지 않는다. IntelliJ 또는 gradlew.bat bootRun 으로 먼저 띄운다" }
New-Item -ItemType Directory -Force $ResultDir | Out-Null

function Get-BackendCpuSeconds {
    # Docker Desktop 이 호스트 쪽에서 포트(9092)를 받아 VM 안으로 넘기는 프로세스. 프로세스가 여럿이라 합한다.
    (Get-Process com.docker.backend -ErrorAction SilentlyContinue | Measure-Object CPU -Sum).Sum
}

Reset-LoadTopic $Topic $Partitions
$sampler = Start-CpuSampler
$hostSamples = [System.Collections.Generic.List[object]]::new()
$query = "preset=$Preset&startRate=$StartRate&stepRate=$StepRate&maxRate=$MaxRate&stepSeconds=$StepSeconds" +
         "&warmupSeconds=$WarmupSeconds&threads=$Threads&producers=$Producers&failuresToStop=$FailuresToStop"
$status = $null
$finished = $false
try {
    Get-Json "$App/api/loadtest/start?$query" Post | Out-Null
    Write-Host "앱 프로듀서 경유 / $Preset : 스레드 $Threads, 프로듀서 $Producers, 계단 $StepSeconds 초 (워밍업 $WarmupSeconds 초). 앱 콘솔에도 계단마다 한 줄씩 찍힌다"
    $printed = 0
    do {
        Start-Sleep -Seconds 1
        $hostSamples.Add([pscustomobject]@{ T = Get-EpochMs; Cpu = Get-BackendCpuSeconds })
        $status = Get-Json "$App/api/loadtest/status"
        while ($printed -lt $status.steps.Count) {
            $s = $status.steps[$printed]
            Write-Host ("  {0:N0} 줄/s → 실제 {1:N0} ({2:N1}%), p99 {3}ms, 오류 {4} → {5}" -f $s.targetRate, $s.achievedRate,
                ($s.achievedRatio * 100), $s.p99Ms, $s.errors, $(if ($s.passed) { '버팀' } else { '못 버팀' }))
            $printed++
        }
    } while ($status.state -eq 'RUNNING')
    $finished = $true
} finally {
    if (-not $finished) {
        # Ctrl+C 로 스크립트를 멈추면 앱 쪽 실행도 멈춘다.
        try { Invoke-RestMethod -Method Post "$App/api/loadtest/stop" | Out-Null } catch { }
    }
    $samples = @(Stop-CpuSampler $sampler)
}
if ($status.state -eq 'FAILED') { throw "앱 쪽 부하 테스트 실패: $($status.error)" }

function Get-BackendCores([long]$FromMs, [long]$ToMs) {
    # 집계 구간 안쪽의 첫 표본과 마지막 표본 사이에 늘어난 CPU 초 ÷ 지난 초 = 평균 코어 수
    $in = @($hostSamples | Where-Object { $_.T -ge $FromMs -and $_.T -le $ToMs })
    if ($in.Count -lt 2) { return $null }
    $a = $in[0]; $b = $in[$in.Count - 1]
    [math]::Round(($b.Cpu - $a.Cpu) / (($b.T - $a.T) / 1000), 2)
}

$rows = foreach ($s in $status.steps) {
    [pscustomobject][ordered]@{
        '목표'        = $s.targetRate
        '대수'        = $s.vehicles
        '실제'        = [long]$s.achievedRate
        '달성%'       = [math]::Round($s.achievedRatio * 100, 1)
        'MB/s'        = [math]::Round($s.payloadMBps, 1)
        'p50'         = $s.p50Ms
        'p99'         = $s.p99Ms
        'max'         = $s.maxMs
        '오류'        = $s.errors
        '브로커CPU'   = Get-AvgCores $samples 'kafka' $s.measureStartEpochMs $s.measureEndEpochMs
        '보내는쪽CPU' = [math]::Round($s.appCpuCores, 2)
        '포트전달CPU' = Get-BackendCores $s.measureStartEpochMs $s.measureEndEpochMs
        '배치B'       = if ($null -eq $s.batchSizeAvg) { $null } else { [long]$s.batchSizeAvg }
        '요청당건수'  = if ($null -eq $s.recordsPerRequest) { $null } else { [math]::Round($s.recordsPerRequest, 1) }
        '압축률'      = if ($null -eq $s.compressionRate) { $null } else { [math]::Round($s.compressionRate, 3) }
        '버퍼대기'    = if ($null -eq $s.bufferPoolWaitRatio) { $null } else { [math]::Round($s.bufferPoolWaitRatio, 3) }
        '대기ms'      = if ($null -eq $s.recordQueueTimeMs) { $null } else { [math]::Round($s.recordQueueTimeMs, 1) }
        '판정'        = if ($s.passed) { '버팀' } else { '못 버팀' }
    }
}
$firstError = @($status.steps | Where-Object { $_.firstError } | Select-Object -First 1)
if ($firstError.Count -gt 0) { Write-Host "첫 전송 오류: $($firstError[0].firstError)" }
Write-Host ("토픽 디스크 사용량: {0:N0} MB" -f (Get-TopicDiskMB $Topic))
if (-not $KeepTopic) { Remove-LoadTopic $Topic }

$csv = Join-Path $ResultDir ("app-{0}-t{1}-p{2}-{3}.csv" -f $Preset, $Threads, $Producers, (Get-Date -Format 'yyyyMMdd-HHmmss'))
Show-Result $rows "앱 프로듀서 경유 t$Threads p$Producers" $Preset $csv
