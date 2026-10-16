# =============================================================================
# 적재 컨슈머 구간(2-3) 부하 테스트. Kafka 에 메시지를 미리 쌓아 두고, 컨슈머 수를 바꿔 가며 그것을 ClickHouse 에 다 넣는 데
# 걸리는 시간을 잰다. 생산 속도와 상관없이 "컨슈머가 poll() 해서 적재할 수 있는 최대 속도"가 나온다.
#
# 순서 (컨슈머 수마다)
#   1. sink 를 멈추고 ClickHouse 테이블을 비우고, 컨슈머 그룹의 오프셋을 처음으로 되돌린다
#   2. SINK_CONCURRENCY 를 바꿔 sink 컨테이너를 새로 만든다 → 바로 읽기 시작한다
#   3. lag 가 0 이 되고 적재한 행이 쌓인 메시지 수에 닿을 때까지 2초마다 /api/sink/stats 를 본다
#
# 메시지는 ingest 컨테이너의 LoadTestRunner 로 채운다(실제 로그를 정규화한 JSON, 가상 차량 키). 같은 줄이 여러 번 들어가도
# 차량 ID 가 달라 멱등키 (device_id, source, offset) 는 대부분 겹치지 않는다. 그래서 ReplacingMergeTree 의 중복 제거가 결과를 흔들지 않는다.
#
# 준비: docker compose -f docker/docker-compose.yml -f docker/docker-compose.loadtest.yml up -d
# 실행: .\scripts\loadtest\sink.ps1                        (1,000만 건을 채우고 컨슈머 1, 3, 6, 12 로 잰다)
#       .\scripts\loadtest\sink.ps1 -SkipFill -Concurrency 12   (이미 쌓인 토픽으로 다시)
# =============================================================================
param(
    [int[]]$Concurrency = @(1, 3, 6, 12),
    [long]$Messages = 10000000,
    [int]$FillRate = 200000,
    [switch]$SkipFill,
    [int]$TimeoutMinutes = 20,
    [int]$Partitions = 12,
    [string]$Topic = 'loadtest-logs',
    [string]$Group = 'clickhouse-sink',
    [string]$Ingest = 'http://localhost:18080',
    [string]$Sink = 'http://localhost:18081'
)
. "$PSScriptRoot\common.ps1"
New-Item -ItemType Directory -Force $ResultDir | Out-Null

function Stop-Sink { Invoke-Docker stop sink | Out-Null }

function Start-Sink([int]$Threads) {
    # 컨슈머 스레드 수는 기동 인자라 컨테이너를 다시 만든다. compose 파일의 ${SINK_CONCURRENCY} 가 이 값을 받는다.
    $env:SINK_CONCURRENCY = "$Threads"
    $r = Invoke-Docker @ComposeArgs up -d --force-recreate --no-deps sink
    if ($r.ExitCode -ne 0) { throw "sink 기동 실패: $($r.Lines -join ' ')" }
    $deadline = (Get-Date).AddSeconds(90)
    while ($true) {
        try { Invoke-RestMethod "$Sink/actuator/health" -TimeoutSec 2 | Out-Null; return } catch { }
        if ((Get-Date) -gt $deadline) { throw "sink 가 90초 안에 뜨지 않았다 (docker logs sink)" }
        Start-Sleep -Milliseconds 500
    }
}

if (-not $SkipFill) {
    Stop-Sink
    Reset-LoadTopic $Topic $Partitions
    $seconds = [int][math]::Ceiling($Messages / $FillRate)
    Write-Host ("메시지 채우기: {0:N0}건 목표, 초당 {1:N0}건으로 {2}초 (ingest 의 LoadTestRunner, preset batch)" -f $Messages, $FillRate, $seconds)
    $q = "preset=batch&startRate=$FillRate&stepRate=1&maxRate=$FillRate&stepSeconds=$seconds&warmupSeconds=0&threads=8&failuresToStop=1"
    Get-Json "$Ingest/api/loadtest/start?$q" Post | Out-Null
    do { Start-Sleep -Seconds 2; $st = Get-Json "$Ingest/api/loadtest/status" } while ($st.state -eq 'RUNNING')
    if ($st.state -ne 'FINISHED') { throw "채우기 실패: $($st.state) $($st.error)" }
}
$total = Get-TopicEndOffsetSum $Topic
Write-Host ("토픽 {0} 에 쌓인 메시지: {1:N0}건, 디스크 {2:N0} MB" -f $Topic, $total, (Get-TopicDiskMB $Topic))
if ($total -le 0) { throw "토픽이 비어 있다" }

$sampler = Start-CpuSampler
$rows = [System.Collections.Generic.List[object]]::new()
try {
    foreach ($c in $Concurrency) {
        Stop-Sink
        Invoke-ClickHouse "TRUNCATE TABLE logs.app_logs" | Out-Null
        $reset = Invoke-Docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 `
            --group $Group --reset-offsets --to-earliest --topic $Topic --execute
        if ($reset.ExitCode -ne 0) { throw "오프셋 초기화 실패: $($reset.Lines -join ' ')" }
        $startMs = Get-EpochMs
        Write-Host ("컨슈머 {0}개: {1:N0}건 적재 시작" -f $c, $total)
        Start-Sink $c
        $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
        $s = $null
        while ($true) {
            Start-Sleep -Seconds 2
            $s = Get-Json "$Sink/api/sink/stats"
            if ($s.lag -eq 0 -and $s.rows -ge $total) { break }
            if ((Get-Date) -gt $deadline) { Write-Host "  시간 초과 (lag $($s.lag))"; break }
        }
        $endMs = Get-EpochMs
        # ClickHouse 쪽: 남은 활성 part 수, 이 실행 동안 새로 생긴 part 와 병합 횟수, 테이블 행 수
        Invoke-ClickHouse "SYSTEM FLUSH LOGS" | Out-Null
        $activeParts = [long]("$(Invoke-ClickHouse "SELECT count() FROM system.parts WHERE database = 'logs' AND table = 'app_logs' AND active")")
        $events = Invoke-ClickHouse ("SELECT event_type, count() FROM system.part_log WHERE database = 'logs' AND table = 'app_logs' " +
            "AND event_time >= toDateTime($([math]::Floor($startMs / 1000))) GROUP BY event_type")
        $newParts = 0; $merges = 0
        foreach ($e in $events) {
            $p = "$e" -split '\s+'
            if ($p[0] -eq 'NewPart') { $newParts = [long]$p[1] }
            if ($p[0] -eq 'MergeParts') { $merges = [long]$p[1] }
        }
        $tableRows = [long]("$(Invoke-ClickHouse "SELECT count() FROM logs.app_logs")")
        $row = [pscustomobject][ordered]@{
            '컨슈머'      = $c
            '적재행'      = [long]$s.rows
            '적재행/s'    = [long]$s.rowsPerSecond
            '대수환산'    = [long]($s.rowsPerSecond / 60.3)
            '걸린초'      = [math]::Round(([long]$s.lastInsertEndEpochMs - [long]$s.firstInsertStartEpochMs) / 1000, 1)
            'INSERT수'    = [long]$s.inserts
            '행/INSERT'   = [long]$s.rowsPerInsert
            'INSERTp50ms' = [long]$s.insertP50Ms
            'INSERTp99ms' = [long]$s.insertP99Ms
            '서버ms/INSERT' = [math]::Round([double]$s.serverMsPerInsert, 1)
            'sinkCPU'     = $null
            'ClickHouseCPU' = $null
            '새part'      = $newParts
            '병합'        = $merges
            '남은part'    = $activeParts
            '테이블행'    = $tableRows
            '실패'        = [long]$s.failures
            'fromMs'      = [long]$s.firstInsertStartEpochMs
            'toMs'        = [long]$s.lastInsertEndEpochMs
        }
        $rows.Add($row)
        Write-Host ("  → {0:N0}행/s (약 {1:N0}대 분량), INSERT {2:N0}회 x {3:N0}행, INSERT p99 {4}ms, 새 part {5}, 병합 {6}" -f
            $row.'적재행/s', $row.'대수환산', $row.'INSERT수', $row.'행/INSERT', $row.'INSERTp99ms', $newParts, $merges)
    }
} finally {
    $samples = @(Stop-CpuSampler $sampler)
}

foreach ($row in $rows) {
    $row.'sinkCPU' = Get-AvgCores $samples 'sink' $row.fromMs $row.toMs
    $row.'ClickHouseCPU' = Get-AvgCores $samples 'clickhouse' $row.fromMs $row.toMs
}
Show-Table ($rows | Select-Object * -ExcludeProperty fromMs, toMs) (Join-Path $ResultDir ("sink-{0}.csv" -f (Get-Date -Format 'yyyyMMdd-HHmmss')))
Write-Host "토픽은 남겨 둔다(-SkipFill 로 다시 쓸 수 있다). 지우려면: docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $Topic"
