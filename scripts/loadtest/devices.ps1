# =============================================================================
# 장비 → 수집 서버 구간(2-1) 부하 테스트. 장비 N대가 Fluent Bit 처럼 보내는 HTTP 요청을 vegeta 로 흉내 내고, 대수를 계단식으로 올린다.
#
# 부하 발생기는 vegeta(Go)다. 처음에 k6 를 썼으나 초당 4,336건에 k6 자신이 CPU 코어 9개를 써서(요청마다 자바스크립트가
# 3.7KB 본문을 다룬다) 수천 대를 흉내 낼 수 없었다. vegeta 는 같은 부하에 코어 0.8개를 쓴다(2026-10-16 실측).
# vegeta 는 정해진 속도로 새 요청을 시작한다(열린 모델). 장비는 서버를 기다려 주지 않고 1초마다 새 묶음을 만들기 때문이다.
#
# 준비 (docs/result/benchmark-transport.md 실행 절차)
#   docker compose -f docker/docker-compose.yml -f docker/docker-compose.loadtest.yml up -d
#   Invoke-RestMethod -Method Post "http://localhost:18080/api/loadtest/fluentbit-batches?mode=per-file" |
#       ConvertTo-Json | Set-Content build\loadtest\fluentbit-per-file.meta.json            (본문 파일, 대외비)
#
# 실행
#   .\scripts\loadtest\devices.ps1                         지금 Fluent Bit 설정(파일별 요청), 압축 없음, 실제 경로(Kafka ack 까지)
#   .\scripts\loadtest\devices.ps1 -Gzip                   compress gzip 을 켠 에이전트
#   .\scripts\loadtest\devices.ps1 -Mode per-device        장비당 1초에 요청 1건 (Tag 를 하나로 묶은 설정)
#   .\scripts\loadtest\devices.ps1 -Endpoint receive       HTTP 수신 + JSON 읽기까지만 (Kafka 로 보내지 않음)
#   .\scripts\loadtest\devices.ps1 -Endpoint normalize     위 + 정규화까지
#   .\scripts\loadtest\devices.ps1 -WithSink               적재 컨슈머가 함께 돌 때 계단마다 lag 과 수집→저장 지연도 잰다
#
# 판정: 성공한 요청 속도가 목표의 95% 이상, 실패 0, p99 1초 이하. 두 계단 연속 못 버티면 멈춘다.
# =============================================================================
param(
    [ValidateSet('per-file', 'per-device')][string]$Mode = 'per-file',
    [switch]$Gzip,
    [ValidateSet('full', 'normalize', 'receive')][string]$Endpoint = 'full',
    [int]$StartDevices = 1000,
    [int]$StepDevices = 1000,
    [int]$MaxDevices = 30000,
    [int]$StepSeconds = 30,
    [int]$WarmupSeconds = 15,
    [int]$FailuresToStop = 2,
    # 동시에 응답을 기다릴 수 있는 요청 수의 상한. 서버가 느려져 이만큼이 모두 묶이면 vegeta 는 목표 속도를 못 낸다(달성률 하락)
    [int]$MaxWorkers = 2000,
    [switch]$WithSink,
    [int]$Partitions = 12,
    [string]$Topic = 'loadtest-logs',
    [string]$Sink = 'http://localhost:18081',
    [switch]$KeepTopic
)
. "$PSScriptRoot\common.ps1"

$VegetaImage = 'peterevans/vegeta:latest'
$metaFile = Join-Path $LoadDir "fluentbit-$Mode.meta.json"
if (-not (Test-Path $metaFile)) {
    throw "$metaFile 가 없다. 이 파일 머리말의 준비 명령으로 본문 파일을 먼저 만든다"
}
$meta = Get-Content $metaFile -Raw | ConvertFrom-Json
New-Item -ItemType Directory -Force $ResultDir | Out-Null
$path = switch ($Endpoint) {
    'full'      { '/ingest/fluentbit' }
    'normalize' { '/loadtest/ingest/normalize' }
    'receive'   { '/loadtest/ingest/receive' }
}
$label = "$Mode / $(if ($Gzip) { 'gzip' } else { '압축 없음' }) / $Endpoint$(if ($WithSink) { ' / +sink' })"

function New-VegetaTargets {
    # 요청 본문 파일(한 줄 = 요청 하나)을 vegeta 의 JSON 대상 형식으로 바꾼다. 본문은 base64 로 들어간다.
    # gzip 이면 본문을 미리 압축하고 Content-Encoding 헤더를 붙인다. 압축은 장비(Fluent Bit)가 하는 일이라 부하 발생기가 매번 할 필요가 없다.
    $name = "targets-$Mode$(if ($Gzip) { '-gzip' })-$Endpoint.jsonl"
    $dst = Join-Path $LoadDir $name
    $src = Join-Path $LoadDir "fluentbit-$Mode.jsonl"
    if ((Test-Path $dst) -and (Get-Item $dst).LastWriteTime -gt (Get-Item $src).LastWriteTime) { return $name }
    $header = if ($Gzip) { '{"Content-Type":["application/json"],"Content-Encoding":["gzip"]}' } else { '{"Content-Type":["application/json"]}' }
    $reader = New-Object IO.StreamReader($src, [Text.Encoding]::UTF8)
    $writer = New-Object IO.StreamWriter($dst, $false, (New-Object Text.UTF8Encoding $false))
    try {
        while ($null -ne ($line = $reader.ReadLine())) {
            if ($line.Length -eq 0) { continue }
            $bytes = [Text.Encoding]::UTF8.GetBytes($line)
            if ($Gzip) {
                $buf = New-Object IO.MemoryStream
                $gz = New-Object IO.Compression.GZipStream($buf, [IO.Compression.CompressionLevel]::Optimal)
                $gz.Write($bytes, 0, $bytes.Length)
                $gz.Close()
                $bytes = $buf.ToArray()
            }
            $writer.WriteLine('{"method":"POST","url":"http://ingest:8080' + $path + '","header":' + $header +
                ',"body":"' + [Convert]::ToBase64String($bytes) + '"}')
        }
    } finally {
        $reader.Close()
        $writer.Close()
    }
    $name
}

function Invoke-Vegeta([int]$Rate, [int]$Seconds, [string]$Targets, [string]$Report) {
    # attack 결과를 바로 report 로 넘겨 JSON 요약(지연 백분위, 성공률, 상태 코드)만 남긴다.
    Invoke-Docker run --rm --name vegeta --network $Network -v "${LoadDir}:/loadtest" --entrypoint sh $VegetaImage -c (
        "vegeta attack -format=json -targets=/loadtest/$Targets -rate=$Rate/1s -duration=${Seconds}s " +
        "-timeout=10s -max-workers=$MaxWorkers | vegeta report -type=json > /loadtest/results/$Report")
}

$targets = New-VegetaTargets
Write-Host ("장비 부하: {0}. 장비 1대 = 1초에 요청 {1:N2}건, {2:N1}줄 (요청 1건 {3:N1}줄, {4:N1}KB, gzip 이면 {5:N0}%)" -f $label,
    $meta.requestsPerDeviceSecond, $meta.linesPerDeviceSecond, $meta.linesPerRequest, ($meta.avgBodyBytes / 1024), ($meta.gzipRatio * 100))
if (-not $WithSink) {
    $running = Invoke-Docker ps --filter name=^sink$ --filter status=running --format '{{.Names}}'
    if ($running.Lines -contains 'sink') { Write-Host "주의: sink 가 떠 있다. 수집 구간만 재려면 docker stop sink 를 먼저 한다 (같은 CPU 를 쓴다)" }
}
if ($Endpoint -eq 'full' -and -not $WithSink) { Reset-LoadTopic $Topic $Partitions }

$sampler = Start-CpuSampler
$rows = [System.Collections.Generic.List[object]]::new()
try {
    # 첫 계단 전에 서버의 JIT 와 연결을 데운다. 이 실행은 기록하지 않는다.
    Write-Host "워밍업 $WarmupSeconds 초"
    Invoke-Vegeta ([int]($StartDevices * $meta.requestsPerDeviceSecond)) $WarmupSeconds $targets 'vegeta-warmup.json' | Out-Null

    $failures = 0
    for ($devices = $StartDevices; $devices -le $MaxDevices; $devices += $StepDevices) {
        $rate = [int][math]::Round($devices * $meta.requestsPerDeviceSecond)
        $reportName = "vegeta-step-$devices.json"
        $reportPath = Join-Path $ResultDir $reportName
        Remove-Item $reportPath -ErrorAction SilentlyContinue
        $from = Get-EpochMs
        $r = Invoke-Vegeta $rate $StepSeconds $targets $reportName
        $to = Get-EpochMs
        if (-not (Test-Path $reportPath) -or (Get-Item $reportPath).Length -eq 0) {
            $r.Lines | Select-Object -Last 20 | ForEach-Object { Write-Host $_ }
            throw "vegeta 실패 (종료 코드 $($r.ExitCode))"
        }
        $v = Get-Content $reportPath -Raw | ConvertFrom-Json
        $requests = [double]$v.requests
        $success = [double]$v.success
        # 집계 구간 안에 성공한 요청 수 ÷ 계단 길이. 서버가 밀리면 vegeta 가 일꾼 상한에 걸려 요청을 덜 시작하거나, 실패가 섞인다.
        $achieved = $requests * $success / $StepSeconds
        $p99 = [double]$v.latencies.'99th' / 1e6
        $ratio = $achieved / $rate
        $passed = ($ratio -ge 0.95) -and ($success -eq 1) -and ($p99 -le 1000)
        $errors = @($v.errors)
        $row = [pscustomobject][ordered]@{
            '대수'        = $devices
            '목표req/s'   = $rate
            '실제req/s'   = [long]$achieved
            '달성%'       = [math]::Round($ratio * 100, 1)
            '줄/s'        = [long]($achieved * $meta.linesPerRequest)
            '전송MB/s'    = [math]::Round([double]$v.bytes_out.total / $StepSeconds / 1MB, 1)
            'p50'         = [math]::Round([double]$v.latencies.'50th' / 1e6, 2)
            'p99'         = [math]::Round($p99, 2)
            'max'         = [math]::Round([double]$v.latencies.max / 1e6, 1)
            '실패%'       = [math]::Round((1 - $success) * 100, 3)
            '수집서버CPU' = $null
            '브로커CPU'   = $null
            '발생기CPU'   = $null
            'lag'         = $null
            '저장지연p50' = $null
            '저장지연p99' = $null
            '판정'        = if ($passed) { '버팀' } else { '못 버팀' }
            'fromMs'      = $from
            'toMs'        = $to
        }
        if ($WithSink) {
            # 계단이 끝난 순간 아직 적재되지 않은 메시지 수와, 이 계단 동안 적재된 행의 "수집 서버가 받은 뒤 저장되기까지" 지연(ms)
            $row.'lag' = (Get-Json "$Sink/api/sink/stats").lag
            $q = Invoke-ClickHouse ("SELECT quantile(0.5)(dateDiff('millisecond', collected_at, inserted_at)), " +
                "quantile(0.99)(dateDiff('millisecond', collected_at, inserted_at)) FROM logs.app_logs " +
                "WHERE inserted_at BETWEEN fromUnixTimestamp64Milli($from) AND fromUnixTimestamp64Milli($to)")
            $parts = "$q" -split '\s+'
            $row.'저장지연p50' = $parts[0]
            $row.'저장지연p99' = $parts[1]
        }
        $rows.Add($row)
        $firstError = if ($errors.Count -gt 0) { " / $($errors[0])" } else { '' }
        Write-Host ("  {0:N0}대 ({1:N0} req/s) → 실제 {2:N0} req/s ({3}%), p50 {4}ms p99 {5}ms, 실패 {6}%{7} → {8}" -f $devices, $rate,
            $row.'실제req/s', $row.'달성%', $row.'p50', $row.'p99', $row.'실패%', $firstError, $row.'판정')
        if ($passed) { $failures = 0 } else { $failures++ }
        if ($failures -ge $FailuresToStop) { break }
    }
} finally {
    $samples = @(Stop-CpuSampler $sampler)
}

# docker stats 표본을 계단마다 잘라 수집 서버, 브로커, 부하 발생기의 평균 CPU 를 채운다. 앞뒤 3초(컨테이너 기동·정리)는 뺀다.
foreach ($row in $rows) {
    $row.'수집서버CPU' = Get-AvgCores $samples 'ingest' ($row.fromMs + 3000) ($row.toMs - 3000)
    $row.'브로커CPU' = Get-AvgCores $samples 'kafka' ($row.fromMs + 3000) ($row.toMs - 3000)
    $row.'발생기CPU' = Get-AvgCores $samples 'vegeta' ($row.fromMs + 3000) ($row.toMs - 3000)
}
if ($Endpoint -eq 'full' -and -not $WithSink) {
    Write-Host ("토픽 디스크 사용량: {0:N0} MB" -f (Get-TopicDiskMB $Topic))
    if (-not $KeepTopic) { Remove-LoadTopic $Topic }
}

$name = "devices-{0}{1}-{2}{3}-{4}.csv" -f $Mode, $(if ($Gzip) { '-gzip' } else { '' }), $Endpoint,
    $(if ($WithSink) { '-sink' } else { '' }), (Get-Date -Format 'yyyyMMdd-HHmmss')
$passedRows = @($rows | Where-Object { $_.'판정' -eq '버팀' })
Show-Table ($rows | Select-Object * -ExcludeProperty fromMs, toMs) (Join-Path $ResultDir $name)
if ($passedRows.Count -gt 0) {
    $best = ($passedRows | Measure-Object '대수' -Maximum).Maximum
    Write-Host ("[장비 → 수집 서버 / {0}] 버틴 최대 계단: {1:N0}대 (초당 요청 {2:N0}건, 줄 {3:N0})" -f $label, $best,
        ($best * $meta.requestsPerDeviceSecond), ($best * $meta.linesPerDeviceSecond))
} else {
    Write-Host "[장비 → 수집 서버 / $label] 첫 계단부터 못 버텼다"
}
