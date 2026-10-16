# =============================================================================
# 부하 테스트 공통 부분. broker.ps1 과 app.ps1 이 맨 앞에서 불러 쓴다:  . "$PSScriptRoot\common.ps1"
#
# Windows PowerShell 5.1 과 PowerShell 7 에서 같은 문법으로 돈다. 5.1 은 BOM 없는 UTF-8 파일을 ANSI(CP949)로 읽어
# 한글이 깨지므로 이 폴더의 .ps1 은 UTF-8 BOM 으로 저장한다.
# =============================================================================

$Root       = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
# payload.jsonl(실제 로그로 만든 메시지, 대외비)과 결과 CSV 를 둔다. gitignore 된 build/ 아래라 커밋되지 않는다.
$LoadDir    = Join-Path $Root 'build\loadtest'
$ResultDir  = Join-Path $LoadDir 'results'
# docker-compose.yml 의 브로커와 같은 버전. perf-test 는 이 이미지에 들어 있는 Kafka 내장 도구다.
$KafkaImage = 'apache/kafka:4.2.2'
# compose 프로젝트 이름(practice-kafka) + _default. perf-test 컨테이너를 브로커와 같은 네트워크에 붙인다.
$Network    = 'practice-kafka_default'

# 프로듀서 설정 세 가지. src/main/java/com/practice/kafka/loadtest/ProducerPreset.java 와 이름·값이 같아야 한다.
# 기준값은 application.properties 의 producer 설정을 옮긴 것이다. 앱은 그 파일을 읽고, perf-test 는 이 표를 읽는다.
$BaseProducerProps = [ordered]@{ 'acks' = 'all'; 'retries' = '3'; 'batch.size' = '16384'; 'linger.ms' = '5' }
$Presets = @{
    'baseline'  = [ordered]@{}
    'batch'     = [ordered]@{ 'batch.size' = '131072'; 'linger.ms' = '20' }
    'batch-lz4' = [ordered]@{ 'batch.size' = '131072'; 'linger.ms' = '20'; 'compression.type' = 'lz4' }
}

$Invariant = [Globalization.CultureInfo]::InvariantCulture

function Get-EpochMs { [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() }

function Get-Json([string]$Url, [string]$Method = 'Get') {
    # 호스트에서 컨테이너의 제어 API 를 부르는 길(Docker Desktop 포트 전달)은 부하가 가장 높을 때 연결이 한 번씩 끊긴다
    # (2026-10-16, "원격 호스트에 의해 강제로 끊겼습니다"). 한 번 끊겼다고 실행이 끝난 것으로 보지 않도록 몇 번 다시 묻는다.
    for ($i = 1; ; $i++) {
        try { return Invoke-RestMethod -Method $Method $Url -TimeoutSec 15 -ErrorAction Stop }
        catch {
            if ($i -ge 10) { throw }
            Start-Sleep -Seconds 1
        }
    }
}

function Invoke-Docker {
    # docker 와 Kafka CLI 는 경고를 stderr 로 내보낸다. PowerShell 5.1 은 2>&1 로 받은 stderr 줄을 오류로 취급하므로
    # 이 함수 안에서만 오류 처리 기준을 낮추고, 성공 여부는 종료 코드로 판단한다.
    $ErrorActionPreference = 'Continue'
    $lines = & docker @args 2>&1 | ForEach-Object { "$_" }
    [pscustomobject]@{ ExitCode = $LASTEXITCODE; Lines = @($lines) }
}

function Invoke-KafkaTopics {
    Invoke-Docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 @args
}

function Reset-LoadTopic([string]$Topic, [int]$Partitions) {
    # 실행마다 토픽을 지우고 새로 만든다. 앞 실행의 데이터가 디스크와 페이지 캐시에 남아 있으면 조건이 달라진다.
    Write-Host "토픽 $Topic 초기화 (파티션 $Partitions)"
    Invoke-KafkaTopics --delete --if-exists --topic $Topic | Out-Null
    $deadline = (Get-Date).AddSeconds(60)
    while ($true) {
        # 보존 크기(파티션당 1GB)와 세그먼트 크기(256MB)는 디스크가 끝없이 커지지 않게 하는 안전장치다.
        # 다만 브로커가 보존 규칙을 검사하는 주기(log.retention.check.interval.ms)가 기본 5분이라 실행 중에는 거의 지워지지 않는다.
        # docker-compose.loadtest.yml 을 겹쳐 띄우면 이 주기가 30초가 된다. 그래도 실행이 끝나면 토픽을 지운다(-KeepTopic 이 없을 때).
        $r = Invoke-KafkaTopics --create --topic $Topic --partitions $Partitions --replication-factor 1 `
            --config retention.bytes=1073741824 --config segment.bytes=268435456
        if ($r.ExitCode -eq 0) { return }
        # 지운 직후에는 "marked for deletion" 으로 거절될 수 있다. 정리가 끝날 때까지 다시 시도한다.
        if ((Get-Date) -gt $deadline) { throw "토픽 생성 실패: $($r.Lines -join ' ')" }
        Start-Sleep -Seconds 2
    }
}

function Remove-LoadTopic([string]$Topic) {
    Invoke-KafkaTopics --delete --if-exists --topic $Topic | Out-Null
    Write-Host "토픽 $Topic 삭제 (남기려면 -KeepTopic)"
}

function Get-TopicDiskMB([string]$Topic) {
    # 지운 토픽의 폴더는 이름 끝에 -delete 가 붙은 채 60초쯤 남는다(file.delete.delay.ms). 그것은 세지 않는다.
    $r = Invoke-Docker exec kafka sh -c "for d in /var/lib/kafka/data/$Topic-*; do case `$d in *-delete) ;; *) du -sm `$d ;; esac; done 2>/dev/null"
    ($r.Lines | ForEach-Object { [int](($_ -split '\s+')[0]) } | Measure-Object -Sum).Sum
}

function Start-CpuSampler {
    # docker stats 를 계속 돌려 컨테이너별 CPU 를 표본으로 모은다. 한 번에 1~2초 걸리므로 표본도 그 간격이다.
    # CPU% 는 코어 하나를 100% 로 센다 (350% = 코어 3.5개). 나중에 계단의 집계 구간으로 잘라 평균을 낸다.
    Start-Job -ScriptBlock {
        while ($true) {
            $lines = docker stats --no-stream --format '{{.Name}},{{.CPUPerc}}' 2>$null
            # 표본은 직전 약 1초의 평균이므로 시각을 0.5초 당겨 적는다.
            $t = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - 500
            foreach ($l in $lines) { "$t,$l" }
        }
    }
}

function Stop-CpuSampler($Job) {
    Stop-Job $Job
    $raw = Receive-Job $Job
    Remove-Job $Job
    foreach ($l in $raw) {
        $p = "$l" -split ','
        if ($p.Count -eq 3 -and $p[2] -match '^[\d.]+%$') {
            [pscustomobject]@{ T = [long]$p[0]; Name = $p[1]; Cores = [double]::Parse($p[2].TrimEnd('%'), $Invariant) / 100 }
        }
    }
}

function Get-AvgCores($Samples, [string]$Name, [long]$FromMs, [long]$ToMs) {
    $s = @($Samples | Where-Object { $_.Name -eq $Name -and $_.T -ge $FromMs -and $_.T -le $ToMs })
    if ($s.Count -eq 0) { return $null }
    [math]::Round(($s | Measure-Object Cores -Average).Average, 2)
}

function Format-Rate($v) { if ($null -eq $v) { '-' } else { '{0:N0}' -f $v } }

# 부하 테스트 compose 를 겹친 명령. sink 컨테이너를 다시 만들 때 쓴다.
$ComposeArgs = @('compose', '-f', (Join-Path $Root 'docker\docker-compose.yml'), '-f', (Join-Path $Root 'docker\docker-compose.loadtest.yml'))

function Invoke-ClickHouse([string]$Sql) {
    # clickhouse 컨테이너 안의 clickhouse-client 로 질의한다. 결과는 탭으로 구분된 줄들이다.
    $r = Invoke-Docker exec clickhouse clickhouse-client --user app --password app --query $Sql
    if ($r.ExitCode -ne 0) { throw "ClickHouse 질의 실패: $Sql`n$($r.Lines -join "`n")" }
    $r.Lines
}

function Get-TopicEndOffsetSum([string]$Topic) {
    # 파티션마다 끝 오프셋(= 지금까지 쓰인 메시지 수, 토픽을 새로 만든 뒤라면)의 합
    $r = Invoke-Docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topic
    ($r.Lines | Where-Object { $_ -match '^[^:]+:\d+:\d+$' } | ForEach-Object { [long](($_ -split ':')[2]) } | Measure-Object -Sum).Sum
}

function Show-Table($Rows, [string]$Csv) {
    # 칸이 많아 기본 화면 폭에서는 잘리므로 넓게 찍는다. 모든 칸은 CSV 에도 남는다.
    $Rows | Format-Table -Property * -AutoSize | Out-String -Width 400 | Write-Host
    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $Csv
    Write-Host "결과 CSV: $Csv"
}

function Show-Result($Rows, [string]$Layer, [string]$Preset, [string]$Csv) {
    # 칸이 많아 기본 화면 폭에서는 잘리므로 넓게 찍는다. 모든 칸은 CSV 에도 남는다.
    $Rows | Format-Table -Property * -AutoSize | Out-String -Width 400 | Write-Host
    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $Csv
    $passed = @($Rows | Where-Object { $_.'판정' -eq '버팀' })
    if ($passed.Count -gt 0) {
        $best = ($passed | Measure-Object '목표' -Maximum).Maximum
        Write-Host ("[{0} / {1}] 버틴 최대 계단: {2:N0} 줄/s (약 {3:N0}대 분량)" -f $Layer, $Preset, $best, ($best / 60))
    } else {
        Write-Host "[$Layer / $Preset] 첫 계단부터 못 버텼다"
    }
    Write-Host "결과 CSV: $Csv"
}
