# =============================================================================
# 브로커 단독 층 부하 테스트. Kafka 내장 도구 kafka-producer-perf-test 를 브로커와 같은 docker 네트워크에서 돌린다.
#
# 앱도, 호스트(Windows)에서 docker 로 들어가는 포트 전달도 거치지 않는다. 메시지는 앱이 만든 payload.jsonl
# (실제 로그를 정규화한 JSON, 앱이 보내는 것과 같은 바이트)을 쓰므로, 이 층의 한계가 "이 PC 의 Kafka 자체"의 한계에 가깝다.
#
# 계단마다 perf-test 를 새로 띄운다: 목표 R 줄/s 로 R x StepSeconds 건을 보내고, 앞 R x WarmupSeconds 건은
# --warmup-records 로 집계에서 뺀다. 판정 기준은 앱 층과 같다 (달성률 95% 이상, p99 1초 이하, 오류 0).
#
# perf-test 는 보내는 스레드가 하나다. 그 스레드가 먼저 꽉 차면 브로커가 아니라 perf-test 의 한계를 재게 된다.
# -Producers N 을 주면 perf-test 컨테이너 N 개가 목표를 N 등분해 동시에 보낸다. N 을 늘려도 천장이 그대로면 브로커 쪽 한계다.
#
# 준비: 앱을 띄우고 Invoke-RestMethod -Method Post http://localhost:8080/api/loadtest/payload 로 payload.jsonl 을 만든다.
# 실행: .\scripts\loadtest\broker.ps1 -Preset baseline      (batch, batch-lz4 도 같은 방식. -Producers 2 등)
# =============================================================================
param(
    [ValidateSet('baseline', 'batch', 'batch-lz4')][string]$Preset = 'baseline',
    [int]$StartRate = 100000,
    [int]$StepRate = 50000,
    [int]$MaxRate = 1000000,
    [int]$StepSeconds = 30,
    [int]$WarmupSeconds = 5,
    [int]$FailuresToStop = 2,
    [int]$Producers = 1,
    [int]$Partitions = 3,
    [string]$Topic = 'loadtest-logs',
    [switch]$KeepTopic
)
. "$PSScriptRoot\common.ps1"

$payload = Join-Path $LoadDir 'payload.jsonl'
if (-not (Test-Path $payload)) {
    throw "$payload 가 없다. 앱을 띄우고 Invoke-RestMethod -Method Post http://localhost:8080/api/loadtest/payload 로 먼저 만든다"
}
New-Item -ItemType Directory -Force $ResultDir | Out-Null

# 앱 층과 같은 설정을 perf-test 에 넘긴다. client.id 는 구별용, metrics.* 는 지표 평균을 최근 30초로 잡는 측정용 설정이다.
$props = [ordered]@{}
foreach ($k in $BaseProducerProps.Keys) { $props[$k] = $BaseProducerProps[$k] }
foreach ($k in $Presets[$Preset].Keys) { $props[$k] = $Presets[$Preset][$k] }
$props['client.id'] = "perf-$Preset"
$props['metrics.sample.window.ms'] = '5000'
$props['metrics.num.samples'] = '6'
$propArgs = @($props.Keys | ForEach-Object { "$_=$($props[$_])" })
Write-Host "브로커 단독 / $Preset / perf-test $Producers 개 : $($propArgs -join ' ')"

function Read-PerfOutput([string]$Text) {
    # 워밍업을 뺀 집계 줄: "N steady state records sent, X records/sec (Y MB/sec), A ms avg latency, M ms max latency, P50 ms 50th, ..."
    $m = [regex]::Match($Text, '(\d+) steady state records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency\s*, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th')
    if (-not $m.Success) { return $null }
    # --print-metrics 가 찍는 프로듀서 지표 한 줄. 예: producer-metrics:batch-size-avg:{client-id=perf-batch} : 130512.000
    # perf-test 는 계단마다 새로 뜨므로 이 값은 그 계단 하나(워밍업 포함)의 것이다.
    $metric = {
        param($name)
        $mm = [regex]::Match($Text, "producer-metrics:$([regex]::Escape($name)):\{client-id=[^}]*\}\s*:\s*([\d.]+)")
        if ($mm.Success) { [double]::Parse($mm.Groups[1].Value, $Invariant) } else { $null }
    }
    [pscustomobject]@{
        SteadyRecords = [double]$m.Groups[1].Value
        Rate          = [double]::Parse($m.Groups[2].Value, $Invariant)
        MBps          = [double]::Parse($m.Groups[3].Value, $Invariant)
        Max           = [double]::Parse($m.Groups[5].Value, $Invariant)
        P50           = [long]$m.Groups[6].Value
        P99           = [long]$m.Groups[8].Value
        Errors        = [long](& $metric 'record-error-total')
        Batch         = & $metric 'batch-size-avg'
        PerRequest    = & $metric 'records-per-request-avg'
        Compression   = & $metric 'compression-rate-avg'
        BufferWait    = & $metric 'bufferpool-wait-ratio'
        QueueMs       = & $metric 'record-queue-time-avg'
    }
}

function Get-ClientCores($Samples, [long]$FromMs, [long]$ToMs) {
    # perf-test 컨테이너가 여럿이면 같은 시각 표본끼리 더한 뒤 평균을 낸다.
    $s = @($Samples | Where-Object { $_.Name -like 'kafka-perf*' -and $_.T -ge $FromMs -and $_.T -le $ToMs })
    if ($s.Count -eq 0) { return $null }
    $perT = $s | Group-Object T | ForEach-Object { ($_.Group | Measure-Object Cores -Sum).Sum }
    [math]::Round(($perT | Measure-Object -Average).Average, 2)
}

function Get-Avg($xs) { $v = @($xs | Where-Object { $null -ne $_ }); if ($v.Count -eq 0) { $null } else { ($v | Measure-Object -Average).Average } }
function Get-Max($xs) { $v = @($xs | Where-Object { $null -ne $_ }); if ($v.Count -eq 0) { $null } else { ($v | Measure-Object -Maximum).Maximum } }

Reset-LoadTopic $Topic $Partitions
$sampler = Start-CpuSampler
$rows = [System.Collections.Generic.List[object]]::new()
try {
    $failures = 0
    for ($rate = $StartRate; $rate -le $MaxRate; $rate += $StepRate) {
        $each = [long][math]::Ceiling($rate / $Producers)
        $num = $each * $StepSeconds
        $warm = $each * $WarmupSeconds
        Write-Host ("계단 {0:N0} 줄/s: perf-test {1}개 x {2:N0}건 (각자 워밍업 {3:N0}건 제외)" -f $rate, $Producers, $num, $warm)
        # 컨테이너마다 백그라운드 작업 하나. KAFKA_HEAP_OPTS: perf-test 기본 힙(512MB)으로는 payload 파일 전체를 올리기에 빠듯하다.
        $jobs = for ($i = 1; $i -le $Producers; $i++) {
            $name = if ($Producers -eq 1) { 'kafka-perf' } else { "kafka-perf-$i" }
            Start-Job -ArgumentList $name, $Network, $LoadDir, $KafkaImage, $Topic, $num, $each, $warm, $propArgs -ScriptBlock {
                param($name, $network, $loadDir, $image, $topic, $num, $rate, $warm, $propArgs)
                & docker run --rm --name $name --network $network -e KAFKA_HEAP_OPTS=-Xmx2g `
                    -v "${loadDir}:/loadtest:ro" $image /opt/kafka/bin/kafka-producer-perf-test.sh `
                    --bootstrap-server kafka:19092 --topic $topic --num-records $num --throughput $rate `
                    --payload-file /loadtest/payload.jsonl --warmup-records $warm --reporting-interval 5000 `
                    --print-metrics --command-property @propArgs 2>&1 | ForEach-Object { "$_" }
                "EXITCODE=$LASTEXITCODE"
            }
        }
        $outputs = @($jobs | Wait-Job | ForEach-Object { , @(Receive-Job $_); Remove-Job $_ })
        $end = Get-EpochMs
        $parsed = foreach ($lines in $outputs) {
            $p = Read-PerfOutput ($lines -join ' ')
            if ($null -eq $p -or ($lines -notcontains 'EXITCODE=0')) {
                $lines | Select-Object -Last 20 | ForEach-Object { Write-Host $_ }
                throw "perf-test 실패"
            }
            $p
        }
        $achieved = ($parsed | Measure-Object Rate -Sum).Sum
        # 여럿이면 지연은 가장 나쁜 프로듀서의 값을 쓴다 (백분위는 더하거나 평균할 수 없다).
        $p99 = (Get-Max $parsed.P99)
        $errors = ($parsed | Measure-Object Errors -Sum).Sum
        # perf-test 는 집계 구간의 시각을 알려 주지 않는다. 끝난 시각에서 JVM 종료·컨테이너 정리(약 1.5초)를 빼고,
        # 집계 건수 ÷ 처리량만큼 거슬러 올라간 구간을 CPU 집계 구간으로 어림한다.
        $toMs = $end - 1500
        $fromMs = $toMs - [long](1000 * (Get-Max ($parsed | ForEach-Object { $_.SteadyRecords / [math]::Max($_.Rate, 1) })))
        $ratio = $achieved / $rate
        $passed = ($ratio -ge 0.95) -and ($p99 -le 1000) -and ($errors -eq 0)
        $comp = Get-Avg $parsed.Compression
        $row = [pscustomobject][ordered]@{
            '목표'        = $rate
            '대수'        = [long][math]::Round($rate / 60)
            '실제'        = [long]$achieved
            '달성%'       = [math]::Round($ratio * 100, 1)
            'MB/s'        = [math]::Round(($parsed | Measure-Object MBps -Sum).Sum, 1)
            'p50'         = Get-Max $parsed.P50
            'p99'         = $p99
            'max'         = [long](Get-Max $parsed.Max)
            '오류'        = $errors
            '브로커CPU'   = $null   # 실행이 끝난 뒤 docker stats 표본으로 채운다
            '보내는쪽CPU' = $null
            '배치B'       = [long](Get-Avg $parsed.Batch)
            '요청당건수'  = [math]::Round([double](Get-Avg $parsed.PerRequest), 1)
            '압축률'      = if ($null -eq $comp) { $null } else { [math]::Round($comp, 3) }
            '버퍼대기'    = [math]::Round([double](Get-Max $parsed.BufferWait), 3)
            '대기ms'      = [math]::Round([double](Get-Max $parsed.QueueMs), 1)
            '판정'        = if ($passed) { '버팀' } else { '못 버팀' }
            'fromMs'      = $fromMs
            'toMs'        = $toMs
        }
        $rows.Add($row)
        Write-Host ("  → 실제 {0:N0} 줄/s ({1}%), p99 {2}ms, 오류 {3} → {4}" -f $row.'실제', $row.'달성%', $p99, $errors, $row.'판정')
        if ($passed) { $failures = 0 } else { $failures++ }
        if ($failures -ge $FailuresToStop) { break }
    }
} finally {
    # Ctrl+C 로 멈췄을 때 남은 perf-test 작업을 정리한다. 컨테이너는 --rm 이라 끝나면 저절로 지워진다.
    Get-Job | Where-Object { $_.Id -ne $sampler.Id -and $_.Command -like '*kafka-producer-perf-test*' } | Remove-Job -Force
    $samples = @(Stop-CpuSampler $sampler)
}

# docker stats 표본을 계단마다 잘라 브로커와 perf-test 컨테이너의 평균 CPU 를 채운다.
foreach ($row in $rows) {
    $row.'브로커CPU' = Get-AvgCores $samples 'kafka' $row.fromMs $row.toMs
    $row.'보내는쪽CPU' = Get-ClientCores $samples $row.fromMs $row.toMs
}
Write-Host ("토픽 디스크 사용량: {0:N0} MB" -f (Get-TopicDiskMB $Topic))
if (-not $KeepTopic) { Remove-LoadTopic $Topic }

$suffix = if ($Producers -eq 1) { '' } else { "-x$Producers" }
$csv = Join-Path $ResultDir ("broker-{0}{1}-{2}.csv" -f $Preset, $suffix, (Get-Date -Format 'yyyyMMdd-HHmmss'))
Show-Result ($rows | Select-Object * -ExcludeProperty fromMs, toMs) "브로커 단독 x$Producers" $Preset $csv
