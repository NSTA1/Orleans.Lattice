<#
.SYNOPSIS
    Builds, starts the benchmark host, runs bombardier against every
    Lattice CRUD endpoint, and prints a summary table.

.DESCRIPTION
    The host process is always terminated on exit, even when an error occurs.
    Requires bombardier (https://github.com/codesenberg/bombardier) on PATH.

.PARAMETER Duration
    Seconds each bombardier scenario runs (default 15).

.PARAMETER Concurrency
    Number of concurrent connections bombardier uses (default 64).

.PARAMETER KeysKeyCount
    Number of keys to seed for the KEYS benchmark (default 100).
#>
[CmdletBinding()]
param(
    [int]$Duration     = 15,
    [int]$Concurrency  = 64,
    [int]$KeysKeyCount = 100
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── Paths ────────────────────────────────────────────────────────────────
$repoRoot    = Split-Path -Parent $PSScriptRoot          # benchmark/.. → repo root
$hostProject = Join-Path $PSScriptRoot 'host' 'Orleans.Lattice.Benchmark.Host.csproj'
$baseUrl     = 'http://localhost:5000'
$historyFile = Join-Path $PSScriptRoot 'results.json'

# ── Helpers ──────────────────────────────────────────────────────────────
function Write-Banner([string]$text) {
    $rule = '─' * 60
    Write-Host "`n$rule" -ForegroundColor Cyan
    Write-Host "  $text" -ForegroundColor Cyan
    Write-Host "$rule"   -ForegroundColor Cyan
}

function Write-Step([string]$text) {
    Write-Host "▸ $text" -ForegroundColor Yellow
}

function Assert-Tool([string]$name) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        Write-Host "✗ '$name' not found on PATH. Please install it first." -ForegroundColor Red
        exit 1
    }
}

function Parse-BombardierOutput([string[]]$lines) {
    $rps     = ($lines | Select-String 'Reqs/sec'  | ForEach-Object { if ($_ -match '[\d.]+') { $Matches[0] } }) -as [double]
    $latAvg  = ($lines | Select-String 'Latency'   | ForEach-Object { if ($_ -match 'Latency\s+([\d.]+\S+)') { $Matches[1] } })
    $codes   = ($lines | Select-String '1xx -')
    $tput    = ($lines | Select-String 'Throughput' | ForEach-Object { if ($_ -match 'Throughput:\s+(\S+)') { $Matches[1] } })
    $others  = ($lines | Select-String 'others -'   | ForEach-Object { if ($_ -match 'others - (\d+)') { [int]$Matches[1] } })

    $xx2 = 0; $xx4 = 0; $xx5 = 0; $oth = 0
    if ($codes) {
        $m = [regex]::Match($codes, '2xx - (\d+)')
        if ($m.Success) { $xx2 = [int]$m.Groups[1].Value }
        $m = [regex]::Match($codes, '4xx - (\d+)')
        if ($m.Success) { $xx4 = [int]$m.Groups[1].Value }
        $m = [regex]::Match($codes, '5xx - (\d+)')
        if ($m.Success) { $xx5 = [int]$m.Groups[1].Value }
    }
    if ($others) { $oth = $others }

    [PSCustomObject]@{
        RPS        = $rps
        LatencyAvg = $latAvg
        Throughput = $tput
        '2xx'      = $xx2
        '4xx'      = $xx4
        '5xx'      = $xx5
        'other'    = $oth
    }
}

function Format-Bytes([double]$bytes) {
    if     ($bytes -ge 1GB) { '{0:N1} GB' -f ($bytes / 1GB) }
    elseif ($bytes -ge 1MB) { '{0:N1} MB' -f ($bytes / 1MB) }
    elseif ($bytes -ge 1KB) { '{0:N1} KB' -f ($bytes / 1KB) }
    else                    { '{0:N0} B'  -f $bytes }
}

function Load-PreviousRun {
    if (Test-Path $historyFile) {
        $json = Get-Content $historyFile -Raw | ConvertFrom-Json
        if ($json -and $json.Results) { return $json }
    }
    return $null
}

function Format-Delta([double]$current, [double]$previous) {
    if ($previous -eq 0) { return 'n/a' }
    $pct = (($current - $previous) / $previous) * 100
    $sign = if ($pct -ge 0) { '+' } else { '' }
    return "${sign}{0:N1}%" -f $pct
}

function Show-Comparison($currentResults, $previousRun) {
    $prevMap = @{}
    foreach ($r in $previousRun.Results) { $prevMap[$r.Scenario] = $r }

    $rows = @()
    foreach ($r in $currentResults) {
        $prev = $prevMap[$r.Scenario]
        if (-not $prev) {
            $rows += [PSCustomObject]@{
                Scenario   = $r.Scenario
                RPS        = $r.RPS
                'ΔRPS'     = 'new'
                LatencyAvg = $r.LatencyAvg
                'ΔLatency' = 'new'
            }
            continue
        }
        $rows += [PSCustomObject]@{
            Scenario   = $r.Scenario
            RPS        = $r.RPS
            'ΔRPS'     = Format-Delta $r.RPS $prev.RPS
            LatencyAvg = $r.LatencyAvg
            'PrevRPS'  = $prev.RPS
            'PrevLat'  = $prev.LatencyAvg
        }
    }

    $ts = $previousRun.Timestamp
    Write-Banner "Comparison with previous run ($ts)"
    $rows | Format-Table -AutoSize -Property Scenario, RPS, PrevRPS, 'ΔRPS', LatencyAvg, PrevLat
}

# Starts a background job that samples CPU% and working-set every
# $intervalMs milliseconds. CPU% is derived from TotalProcessorTime
# deltas — no Windows PerformanceCounter dependency.
# Each sample is emitted to the output pipeline immediately so that
# Stop-Job / Receive-Job can collect them.
function Start-ResourceSampler([int]$processId, [int]$intervalMs = 500) {
    Start-Job -ArgumentList $processId, $intervalMs -ScriptBlock {
        param($targetPid, $interval)
        $coreCount = [Environment]::ProcessorCount
        try {
            $prev     = Get-Process -Id $targetPid -ErrorAction Stop
            $prevTime = $prev.TotalProcessorTime
            $prevTs   = [System.Diagnostics.Stopwatch]::GetTimestamp()
        } catch { return }
        Start-Sleep -Milliseconds $interval
        while ($true) {
            try {
                $proc    = Get-Process -Id $targetPid -ErrorAction Stop
                $nowTime = $proc.TotalProcessorTime
                $nowTs   = [System.Diagnostics.Stopwatch]::GetTimestamp()

                $cpuDelta  = ($nowTime - $prevTime).TotalMilliseconds
                $wallDelta = ($nowTs - $prevTs) / [System.Diagnostics.Stopwatch]::Frequency * 1000
                $cpuPct    = if ($wallDelta -gt 0) { $cpuDelta / $wallDelta * 100 / $coreCount } else { 0 }

                # Emit directly to the pipeline so Receive-Job can collect it
                [PSCustomObject]@{
                    CpuPct     = [Math]::Round($cpuPct, 1)
                    WorkingSet = $proc.WorkingSet64
                }

                $prevTime = $nowTime
                $prevTs   = $nowTs
            } catch { break }   # process exited
            Start-Sleep -Milliseconds $interval
        }
    }
}

function Get-ResourceSummary($job) {
    Stop-Job $job -ErrorAction SilentlyContinue
    $samples = @(Receive-Job $job)
    Remove-Job $job -Force -ErrorAction SilentlyContinue

    if ($samples.Count -eq 0) {
        return [PSCustomObject]@{ AvgCpu = 'n/a'; PeakCpu = 'n/a'; AvgMem = 'n/a'; PeakMem = 'n/a' }
    }

    $avgCpu  = [Math]::Round(($samples | Measure-Object -Property CpuPct     -Average).Average, 1)
    $peakCpu = [Math]::Round(($samples | Measure-Object -Property CpuPct     -Maximum).Maximum, 1)
    $avgMem  = ($samples | Measure-Object -Property WorkingSet -Average).Average
    $peakMem = ($samples | Measure-Object -Property WorkingSet -Maximum).Maximum

    [PSCustomObject]@{
        AvgCpu  = "$avgCpu%"
        PeakCpu = "$peakCpu%"
        AvgMem  = Format-Bytes $avgMem
        PeakMem = Format-Bytes $peakMem
    }
}

# ── Pre-flight checks ───────────────────────────────────────────────────
Write-Banner 'Pre-flight checks'
Assert-Tool 'dotnet'
Assert-Tool 'bombardier'
Write-Step 'All tools found.'

# ── Build ────────────────────────────────────────────────────────────────
Write-Banner 'Building host (Release)'
dotnet build $hostProject -c Release --nologo -v q
if ($LASTEXITCODE -ne 0) { Write-Host '✗ Build failed.' -ForegroundColor Red; exit 1 }
Write-Step 'Build succeeded.'

# ── Start host ───────────────────────────────────────────────────────────
Write-Banner 'Starting host'
# Run the compiled DLL directly (not via 'dotnet run') so the process we
# monitor IS the application — 'dotnet run' spawns a child process whose
# CPU/memory would be invisible to our sampler.
$hostDll = Join-Path $PSScriptRoot 'host' 'bin' 'Release' 'net10.0' 'Orleans.Lattice.Benchmark.Host.dll'
$hostProc = Start-Process -FilePath 'dotnet' `
    -ArgumentList $hostDll,"--urls",$baseUrl `
    -PassThru -WindowStyle Hidden

$hostPid = $hostProc.Id
Write-Step "Host PID: $hostPid"

# Ensure host is always killed on exit
$cleanup = {
    if (-not $hostProc.HasExited) {
        Write-Host "`n▸ Stopping host (PID $hostPid)..." -ForegroundColor Yellow
        Stop-Process -Id $hostPid -Force -ErrorAction SilentlyContinue
    }
}
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action $cleanup | Out-Null

try {
    # Wait for host to be ready
    $maxWait = 30
    $ready   = $false
    for ($i = 0; $i -lt $maxWait; $i++) {
        Start-Sleep -Seconds 1
        try {
            $null = Invoke-WebRequest -Uri "$baseUrl/lattice/bench/keys/health" -Method GET `
                        -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
            $ready = $true; break
        } catch {
            # 404 is fine — it means the server is up
            if ($_.Exception.Response.StatusCode.value__ -ge 400) { $ready = $true; break }
        }
    }
    if (-not $ready) { throw 'Host did not start within 30 s.' }
    Write-Step 'Host is ready.'

    # ── Define scenarios ─────────────────────────────────────────────────
    # Scenarios run in order. SET populates the tree, then GET and DELETE
    # reuse those keys (counter reset only — no re-seeding). KEYS uses a
    # separate tree with a small seed so responses stay within timeout.
    #
    # Setup runs before bombardier; it can be $null or a script block.
    $scenarios = @(
        @{
            Name = 'SET'; Method = 'POST'; Path = '/lattice/bench/bench/set'
            Setup = $null
        },
        @{
            Name = 'GET'; Method = 'GET'; Path = '/lattice/bench/bench/get'
            Setup = { curl.exe -s -X POST "$baseUrl/lattice/bench/bench/reset" | Out-Null }
        },
        @{
            Name = 'DELETE'; Method = 'DELETE'; Path = '/lattice/bench/bench/delete'
            Setup = { curl.exe -s -X POST "$baseUrl/lattice/bench/bench/reset" | Out-Null }
        },
        @{
            Name = 'KEYS'; Method = 'GET'; Path = '/lattice/keys-bench/keys'
            Setup = {
                Write-Step "Seeding $KeysKeyCount keys into 'keys-bench' tree..."
                curl.exe -s -X POST "$baseUrl/lattice/keys-bench/seed?count=$KeysKeyCount" | Out-Null
            }
        }
    )

    $results = @()

    foreach ($s in $scenarios) {
        Write-Banner "$($s.Name) ($($s.Method) $($s.Path))"

        if ($s.Setup) { & $s.Setup }

        # Start sampling host CPU / memory in the background
        $sampler = Start-ResourceSampler -processId $hostPid

        $bmArgs = @('-c', $Concurrency, '-d', "${Duration}s", '-m', $s.Method, '-p', 'r')
        $bmArgs += "$baseUrl$($s.Path)"

        $output = & bombardier @bmArgs 2>&1 | ForEach-Object { $_.ToString() }
        $output | ForEach-Object { Write-Host $_ }

        # Collect resource samples
        $res = Get-ResourceSummary $sampler

        $parsed = Parse-BombardierOutput $output
        $results += [PSCustomObject]@{
            Scenario   = $s.Name
            RPS        = $parsed.RPS
            LatencyAvg = $parsed.LatencyAvg
            Throughput = $parsed.Throughput
            AvgCpu     = $res.AvgCpu
            PeakCpu    = $res.PeakCpu
            AvgMem     = $res.AvgMem
            PeakMem    = $res.PeakMem
            '2xx'      = $parsed.'2xx'
            '4xx'      = $parsed.'4xx'
            '5xx'      = $parsed.'5xx'
            'other'    = $parsed.'other'
        }
    }

    # ── Summary ──────────────────────────────────────────────────────────
    Write-Banner 'Results Summary'
    $results | Format-Table -AutoSize -Property Scenario, RPS, LatencyAvg, Throughput, AvgCpu, PeakCpu, AvgMem, PeakMem, '2xx', '4xx', '5xx', 'other'

    # ── Comparison with previous run ─────────────────────────────────────
    $previousRun = Load-PreviousRun
    if ($previousRun) {
        Show-Comparison $results $previousRun
    } else {
        Write-Host "`nNo previous run found. This run will become the baseline." -ForegroundColor DarkGray
    }

    # ── Retain / discard ─────────────────────────────────────────────────
    $choice = Read-Host "`nSave this run to history? [Y/n]"
    if ($choice -eq '' -or $choice -match '^[Yy]') {
        $run = [PSCustomObject]@{
            Timestamp   = (Get-Date -Format 'o')
            Duration    = $Duration
            Concurrency = $Concurrency
            Results     = $results
        }
        $run | ConvertTo-Json -Depth 4 | Set-Content $historyFile -Encoding UTF8
        Write-Step "Results saved to $historyFile"
    } else {
        Write-Step 'Results discarded.'
    }

} finally {
    & $cleanup
}
