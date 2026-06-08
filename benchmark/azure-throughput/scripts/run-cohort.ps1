#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Run one cohort on the wedge VM: restart silo, start producer with the
	given vehicle/tickHz/duration, wait for the producer to exit, then pull
	the merged journal back to benchmark/.run/vm/.

.PARAMETER Vehicles
	BENCH_VEHICLE_COUNT (default 4000).
.PARAMETER TickHz
	BENCH_TICK_HZ (default 5).
.PARAMETER DurationSec
	BENCH_DURATION_SEC (default 45).
.PARAMETER ExtraSiloEnv
	Hashtable of extra env vars for the silo (e.g. @{ BENCH_TREE_ID = 'foo' }).
	Applied via a runtime drop-in; cleared between cohorts.

.EXAMPLE
	./run-cohort.ps1 -Vehicles 4000 -TickHz 5 -DurationSec 45
.EXAMPLE
	./run-cohort.ps1 -Vehicles 25000 -TickHz 5 -DurationSec 60 -ExtraSiloEnv @{ BENCH_TREE_ID = 'r25k-001' }
#>
[CmdletBinding()]
param(
	[int] $Vehicles = 4000,
	[int] $TickHz = 5,
	[int] $DurationSec = 45,
	[hashtable] $ExtraSiloEnv = @{},
	[string] $ParametersFile,
	[string] $NamePrefix
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $here '../../..')

# Dot-source the verdict-computation helpers. Kept in a separate module
# so the regression test under scripts/Test-CohortVerdict.ps1 can
# exercise them in isolation without provisioning an Azure VM.
. (Join-Path $here '_run-cohort-helpers.ps1')

if (-not $ParametersFile) {
	$local = Join-Path $here 'parameters.local.ps1'
	$default = Join-Path $here 'parameters.ps1'
	$ParametersFile = if (Test-Path $local) { $local } else { $default }
}
$p = & $ParametersFile
if ($NamePrefix) { $p.NamePrefix = $NamePrefix; $p.ResourceGroup = "rg-$NamePrefix" }

az account set --subscription $p.SubscriptionId | Out-Null

$pipName = "$($p.NamePrefix)-pip"
$fqdn = (& az network public-ip show -g $p.ResourceGroup -n $pipName --query dnsSettings.fqdn -o tsv).Trim()
$sshTarget = "$($p.AdminUsername)@$fqdn"
$sshOpts = @(
	'-o','StrictHostKeyChecking=accept-new',
	'-o','ServerAliveInterval=15',
	'-o','ServerAliveCountMax=3',
	'-o','ConnectTimeout=10'
)

# Hard PowerShell-side timeout wrapper. The server-side `timeout` only
# helps if the remote sh actually runs - a stalled ssh handshake or a
# silently-dropped TCP session can leave ssh.exe blocked indefinitely
# with no signal that ever gets through. Wrap every ssh call in a Job
# we can Stop-Job if it overruns its budget.
function _SshExec {
	param(
		[Parameter(Mandatory)] [string] $Cmd,
		[int] $TimeoutSec = 30
	)
	$serverCmd = "timeout $TimeoutSec sh -c `"$($Cmd -replace '"','\"')`""
	$job = Start-Job -ArgumentList @($sshOpts, $sshTarget, $serverCmd) -ScriptBlock {
		param($opts, $target, $cmd)
		& ssh @opts $target $cmd 2>&1
		$LASTEXITCODE
	}
	$wallCap = $TimeoutSec + 15
	if (-not (Wait-Job -Job $job -Timeout $wallCap)) {
		Stop-Job -Job $job -ErrorAction SilentlyContinue
		Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
		throw "ssh '$Cmd' hung past ${wallCap}s wall budget (server-side timeout=${TimeoutSec}s)."
	}
	$out = @(Receive-Job -Job $job)
	$exit = if ($out.Count -gt 0) { [int]$out[-1] } else { -1 }
	$body = if ($out.Count -gt 1) { ($out[0..($out.Count - 2)] | Out-String).Trim() } else { '' }
	Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
	return @{ ExitCode = $exit; Output = $body }
}

function Invoke-Ssh([string]$cmd, [int]$TimeoutSec = 30) {
	$r = _SshExec -Cmd $cmd -TimeoutSec $TimeoutSec
	if ($r.Output) { Write-Output $r.Output }
	if ($r.ExitCode -ne 0) { throw "ssh '$cmd' failed (exit $($r.ExitCode))." }
}
function Invoke-SshQuery([string]$cmd, [int]$TimeoutSec = 15) {
	$r = _SshExec -Cmd $cmd -TimeoutSec $TimeoutSec
	return $r.Output
}

# Hard PowerShell-side timeout wrapper for scp. Same rationale as _SshExec:
# scp inherits ssh's TCP-stream behaviour and a half-open / stalled stream
# (e.g. ssh session opened, destination file created, but no data ever
# arrives) does NOT honour ConnectTimeout / ServerAliveInterval reliably in
# practice. Without this wrapper an empty .tmp file gets created locally and
# the scp process parks indefinitely, hanging the whole cohort loop. Job +
# wall-clock cap lets us abort cleanly and surface a typed failure instead.
function _ScpExec {
	param(
		[Parameter(Mandatory)] [string] $Source,
		[Parameter(Mandatory)] [string] $Dest,
		[int] $TimeoutSec = 60
	)
	$job = Start-Job -ArgumentList @($sshOpts, $Source, $Dest) -ScriptBlock {
		param($opts, $src, $dst)
		& scp @opts $src $dst 2>&1
		$LASTEXITCODE
	}
	$wallCap = $TimeoutSec + 15
	if (-not (Wait-Job -Job $job -Timeout $wallCap)) {
		Stop-Job -Job $job -ErrorAction SilentlyContinue
		Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
		throw "scp '$Source' -> '$Dest' hung past ${wallCap}s wall budget."
	}
	$out = @(Receive-Job -Job $job)
	$exit = if ($out.Count -gt 0) { [int]$out[-1] } else { -1 }
	Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
	return $exit
}
$cleanup = { }
try {

$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmssZ')
$cohortName = "v${Vehicles}-h${TickHz}-${DurationSec}s-$stamp"
$logDir = Join-Path $repoRoot 'benchmark/.run/azure-throughput'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$siloLog = Join-Path $logDir "silo-$cohortName.log"
$prodLog = Join-Path $logDir "producer-$cohortName.log"

Write-Host "Cohort       : $cohortName" -ForegroundColor Cyan
Write-Host "Silo log     : $siloLog"
Write-Host "Producer log : $prodLog"

# Build the silo drop-in (overrides + a stable tree id per cohort so logs are scoped).
$treeId = if ($ExtraSiloEnv.ContainsKey('BENCH_TREE_ID')) { $ExtraSiloEnv['BENCH_TREE_ID'] } else { "cohort-$cohortName" }
$siloEnvLines = @("[Service]","Environment=BENCH_TREE_ID=$treeId")
foreach ($k in $ExtraSiloEnv.Keys) {
	if ($k -eq 'BENCH_TREE_ID') { continue }
	$siloEnvLines += "Environment=$k=$($ExtraSiloEnv[$k])"
}
$prodEnvLines = @(
	"[Service]",
	"Environment=BENCH_VEHICLE_COUNT=$Vehicles",
	"Environment=BENCH_TICK_HZ=$TickHz",
	"Environment=BENCH_DURATION_SEC=$DurationSec"
)

function Push-DropIn([string]$unit, [string[]]$lines) {
	$content = ($lines -join "`n") + "`n"
	$tmp = New-TemporaryFile
	try {
		[System.IO.File]::WriteAllText($tmp.FullName, $content, [System.Text.UTF8Encoding]::new($false))
		& scp @sshOpts $tmp.FullName "${sshTarget}:/tmp/$unit.override.conf" | Out-Null
		if ($LASTEXITCODE -ne 0) { throw "scp drop-in for $unit failed." }
	} finally { Remove-Item $tmp.FullName -Force -ErrorAction SilentlyContinue }
	Invoke-Ssh "sudo mkdir -p /etc/systemd/system/$unit.d && sudo install -m 0644 /tmp/$unit.override.conf /etc/systemd/system/$unit.d/cohort.conf"
}

Write-Host 'Writing cohort drop-in env...' -ForegroundColor Cyan
Push-DropIn 'lattice-silo.service' $siloEnvLines
Push-DropIn 'lattice-producer.service' $prodEnvLines
Invoke-Ssh 'sudo systemctl daemon-reload'

# Hard-reset both units up front so a wedged previous cohort can't bleed
# into this one. We do this even though `restart` below would imply a
# stop - because the silo's SIGTERM handler awaits its drain before
# exiting, a wedged drain from a previous cohort can make the restart
# hang for minutes. Using `stop` first (capped at 30s via the SSH wrapper)
# bounds that, and `kill -KILL` as a backstop guarantees forward progress.
Write-Host 'Stopping any leftover producer...' -ForegroundColor Cyan
_SshExec -Cmd 'sudo systemctl stop lattice-producer 2>/dev/null; true' -TimeoutSec 20 | Out-Null
Write-Host 'Stopping any leftover silo (cap 30s, then SIGKILL)...' -ForegroundColor Cyan
_SshExec -Cmd 'sudo systemctl stop lattice-silo 2>/dev/null; true' -TimeoutSec 35 | Out-Null
$siloState = Invoke-SshQuery 'systemctl is-active lattice-silo 2>/dev/null'
if ($siloState -eq 'active' -or $siloState -eq 'activating' -or $siloState -eq 'deactivating') {
	Write-Host "  silo state '$siloState' after stop; sending SIGKILL." -ForegroundColor Yellow
	_SshExec -Cmd 'sudo systemctl kill -s SIGKILL lattice-silo; sudo systemctl reset-failed lattice-silo' -TimeoutSec 15 | Out-Null
	Start-Sleep -Seconds 2
}
Write-Host 'Starting silo with cohort config...' -ForegroundColor Cyan
Invoke-Ssh 'sudo systemctl start lattice-silo'
$cursorStamp = (Invoke-SshQuery 'date -u --iso-8601=seconds')
Write-Host "  silo started at $cursorStamp UTC" -ForegroundColor Green

# Capture VM-level baseline: CPU model, total RAM, kernel.
$vmInfo = Invoke-SshQuery 'printf "%d vCPU / %d MiB / %s" $(nproc) $(grep MemTotal /proc/meminfo | awk ''{print int($2/1024)}'') $(uname -r)'

Start-Sleep -Seconds 3
Invoke-Ssh 'sudo systemctl start lattice-producer'
Write-Host "Producer running (~$($DurationSec)s of offered load; expect exit within ~$($DurationSec + 30)s)..." -ForegroundColor Cyan

# Background per-second sampler of silo PID's RSS + CPU%. Runs on the VM
# until the producer is no longer active; output goes to a tmp file we
# pull back at the end.
$samplerCmd = @'
#!/usr/bin/env bash
set -euo pipefail
OUT=/tmp/cohort-sampler.csv
echo 'ts_unix,silo_pid,silo_rss_kib,silo_cpu_pct,sys_cpu_pct,sys_mem_used_kib,sys_mem_total_kib' > "$OUT"
prev_busy=0; prev_total=0
while systemctl is-active --quiet lattice-producer; do
  pid=$(systemctl show -p MainPID --value lattice-silo)
  if [ "$pid" = "0" ] || [ -z "$pid" ]; then sleep 1; continue; fi
  read -r _ user nice sys idle iowait irq sirq steal _ < /proc/stat
  total=$((user+nice+sys+idle+iowait+irq+sirq+steal))
  busy=$((total - idle - iowait))
  if [ "$prev_total" -ne 0 ]; then
	dt=$((total - prev_total)); db=$((busy - prev_busy))
	sys_cpu=$(awk -v db="$db" -v dt="$dt" 'BEGIN{ if(dt>0) printf "%.1f", 100.0*db/dt; else print "0" }')
  else
	sys_cpu="0"
  fi
  prev_total=$total; prev_busy=$busy
  rss=$(awk '/^VmRSS:/ {print $2}' /proc/$pid/status 2>/dev/null || echo 0)
  scpu=$(top -b -n1 -p $pid 2>/dev/null | awk -v pid=$pid '$1==pid {print $9}' | head -1)
  scpu=${scpu:-0}
  mt=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
  ma=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  mu=$((mt - ma))
  printf '%s,%s,%s,%s,%s,%s,%s\n' "$(date +%s)" "$pid" "$rss" "$scpu" "$sys_cpu" "$mu" "$mt" >> "$OUT"
  sleep 1
done
'@
# Write to a temp file, scp it, then launch via nohup. Avoids any quoting
# round-trip headache with embedded $ and quotes.
$tmpSampler = New-TemporaryFile
try {
	[System.IO.File]::WriteAllText($tmpSampler.FullName, ($samplerCmd -replace "`r`n","`n"))
	$exit = _ScpExec -Source $tmpSampler.FullName -Dest "${sshTarget}:/tmp/cohort-sampler.sh" -TimeoutSec 30
	if ($exit -ne 0) { throw "scp of sampler script failed (exit $exit)." }
} finally { Remove-Item $tmpSampler.FullName -Force -ErrorAction SilentlyContinue }
Invoke-Ssh 'rm -f /tmp/cohort-sampler.csv; chmod +x /tmp/cohort-sampler.sh; nohup bash /tmp/cohort-sampler.sh >/tmp/cohort-sampler.out 2>&1 &'

# Poll for producer exit.
$deadline = (Get-Date).AddSeconds($DurationSec + 30)
while ((Get-Date) -lt $deadline) {
	$state = Invoke-SshQuery 'systemctl is-active lattice-producer 2>/dev/null'
	if ($state -ne 'active' -and $state -ne 'activating') { break }
	Start-Sleep -Seconds 2
}
$finalState = Invoke-SshQuery 'systemctl is-active lattice-producer 2>/dev/null'
Write-Host "Producer final state: $finalState" -ForegroundColor Yellow

# Stop the silo so its listener exits, channel completes, drain loop drains,
# and the dispatcher emits its FINAL line. The silo does NOT complete the
# ingest channel on producer disconnect (it's listener-driven for the
# multi-producer case), so the cohort harness has to drive the stop. The
# silo's SIGTERM handler awaits the drain before exiting, so FINAL is
# guaranteed before the unit deactivates.
Write-Host 'Stopping silo to trigger drain + FINAL...' -ForegroundColor Cyan
# Don't throw on a long drain - large in-flight calls (e.g. with BENCH_RESPONSE_TIMEOUT_SEC=180)
# can take a while to settle. Use the bounded helper so we always make forward progress.
_SshExec -Cmd 'sudo systemctl stop lattice-silo 2>/dev/null; true' -TimeoutSec 120 | Out-Null

# Poll for FINAL line in the journal. Should appear within seconds of the
# stop above (after the in-flight flushes drain).
Write-Host 'Waiting for silo FINAL (flush drain)...' -ForegroundColor Cyan
$finalDeadline = (Get-Date).AddSeconds(60)
$sawFinal = $false
while ((Get-Date) -lt $finalDeadline) {
	$hit = Invoke-SshQuery "sudo -n journalctl -u lattice-silo --since '$cursorStamp' --no-pager --output=cat | grep -E 'FINAL (ops|written)=' | head -1"
	if ($hit) { $sawFinal = $true; Write-Host "  $hit" -ForegroundColor Green; break }
	Start-Sleep -Seconds 2
}
if (-not $sawFinal) { Write-Host '  no FINAL line seen within 60s; silo may be wedged.' -ForegroundColor Yellow }

# Pull merged journals + sampler back. The previous attempts to redirect
# inside a sudo subshell over ssh hit quoting + permission edge cases;
# simplest reliable path is to capture journalctl's stdout into a local
# file via ssh's own stream (which is read fully before ssh exits, so
# no half-read pipe stalls).
#
# Every fetch is routed through a job-wrapped wall-clock budget. Empirically
# the producer-log fetch path can stall after creating the local .tmp file
# but before sending a single byte and would hang the whole cohort loop
# indefinitely. The bounded Save-Remote / _ScpExec helpers convert that
# stall into a typed failure without taking down the cohort, and the silo-
# log fetch (which lands the data needed for the §27.1 steady-state mean)
# runs first so a producer-log stall does not lose the cohort sample.
Write-Host 'Extracting journals + sampler...' -ForegroundColor Cyan
$samplerLog = Join-Path $logDir "sampler-$cohortName.csv"

function Save-Remote([string]$remoteCmd, [string]$localPath, [int]$TimeoutSec = 60) {
	$tmp = "$localPath.tmp"
	# Clear any prior aborted-fetch tmp so a stale zero-byte file doesn't
	# masquerade as a successful pull.
	Remove-Item -Path $tmp -Force -ErrorAction SilentlyContinue
	$r = _SshExec -Cmd $remoteCmd -TimeoutSec $TimeoutSec
	if ($r.ExitCode -ne 0) {
		Remove-Item -Path $tmp -Force -ErrorAction SilentlyContinue
		throw "ssh '$remoteCmd' failed (exit $($r.ExitCode))."
	}
	[System.IO.File]::WriteAllText($tmp, $r.Output)
	Move-Item -Force $tmp $localPath
}

# Silo log first: the [silo] t= per-second samples and [phaseA] instruments
# in this file are the cohort sample (per the §27.1 methodology in
# benchmark/azure-throughput/throughput.md). If anything else stalls we still
# want this file on disk.
try { Save-Remote "sudo -n journalctl -u lattice-silo --since $cursorStamp --no-pager --output=cat" $siloLog -TimeoutSec 60 }
catch { Write-Host "  silo journal fetch failed: $_" -ForegroundColor Yellow }

# Producer log: useful for offered-rate sanity-checks but not required for
# the cohort sample. A failure here must not abort the cohort.
try { Save-Remote "sudo -n journalctl -u lattice-producer --since $cursorStamp --no-pager --output=cat" $prodLog -TimeoutSec 60 }
catch { Write-Host "  producer journal fetch failed: $_" -ForegroundColor Yellow }

# Sampler CSV: VM-level CPU/RSS samples used by the headline summary. Same
# best-effort posture as the producer log.
try {
	$samplerExit = _ScpExec -Source "${sshTarget}:/tmp/cohort-sampler.csv" -Dest $samplerLog -TimeoutSec 60
	if ($samplerExit -ne 0) { Write-Host "  sampler fetch returned exit $samplerExit" -ForegroundColor Yellow }
} catch { Write-Host "  sampler fetch failed: $_" -ForegroundColor Yellow }

Invoke-Ssh 'sudo systemctl stop lattice-producer 2>/dev/null || true'

# Parse summaries.
$prodSummary = Select-String -Path $prodLog -Pattern '\[producer\] DONE' -SimpleMatch | Select-Object -First 1
$siloFinal   = Select-String -Path $siloLog -Pattern 'FINAL (ops|written)=' | Select-Object -First 1
$watchdog    = @(Select-String -Path $siloLog -Pattern '\[stall-watchdog\]' -SimpleMatch).Count
$walSlot     = @(Select-String -Path $siloLog -Pattern '\[wal-slot\]' -SimpleMatch).Count
$walAppend   = @(Select-String -Path $siloLog -Pattern '\[wal-append\]' -SimpleMatch).Count
$siloTail    = @(Select-String -Path $siloLog -Pattern '^\[silo\] t=') | Select-Object -Last 1

# Section 27.1: the runner-printed FINAL `active avg` is corrupted by
# drain-tail behaviour when the silo wedges (denominator inflated by 28+ s
# of dead time during the post-producer drain stall). The honest cohort
# sample is the mean of `[silo] t=` per-second rate samples over
# `t in [15s, last-non-zero-rate]`: the `t >= 15s` filter trims the warmup
# ramp, the `rate > 0` filter trims the post-producer drain. We compute
# both numbers here and emit the steady-state mean as the primary metric;
# the FINAL `active avg` is shown only as a secondary diagnostic.
$siloPerSec = @()
$failedSamples = 0
foreach ($m in (Select-String -Path $siloLog -Pattern '^\[silo\] t=')) {
	# Accept both the new (ops=, ops/sec=) and legacy (written=, Entries
	# written per second=) per-second formats so cohort logs captured before
	# the silo log-token rename can still be parsed without re-provisioning.
	if ($m.Line -match 't=\s*([\d.]+)s\s+(?:ops|written)=\s*([\d,]+)\s+(?:ops/sec|Entries written per second)=\s*([\d,]+)\s+inFlight=\s*(\d+)') {
		$sample = [pscustomobject]@{
			t        = [double]$matches[1]
			written  = [long](($matches[2]) -replace ',','')
			rate     = [long](($matches[3]) -replace ',','')
			inFlight = [int]$matches[4]
		}
		$siloPerSec += $sample
		if ($m.Line -match 'failed=\s*([\d,]+)' -and [long](($matches[1]) -replace ',','') -gt 0) {
			$failedSamples++
		}
	}
}
$steady = $siloPerSec | Where-Object { $_.t -ge 15 -and $_.rate -gt 0 }
$steadyMean = 0
$steadyCount = @($steady).Count
if ($steadyCount -gt 0) {
	$steadyMean = [int](($steady | Measure-Object -Property rate -Sum).Sum / $steadyCount)
}
$inFlightMax = 0
$inFlightMedian = 0
if ($steadyCount -gt 0) {
	$inFlightMax = ($steady | Measure-Object -Property inFlight -Maximum).Maximum
	$sortedIf = @($steady | Sort-Object inFlight)
	$inFlightMedian = $sortedIf[[int]($sortedIf.Count / 2)].inFlight
}

# Post-producer drain-tail length: number of trailing rate=0 samples. A
# long zero-rate tail with `inFlight>0` (or with `inFlight=0` but no
# progress) is the drain-wedge phenotype - the silo did not surface its
# stuck batches to the SIGTERM-driven drain inside the configured
# `WalFlushTimeout`/`WalAppendDispatchTimeout` budget. Independent of the
# `stall-watchdog` instrument, which has historically not fired on this
# wedge family.
$drainTailSamples = 0
for ($i = @($siloPerSec).Count - 1; $i -ge 0; $i--) {
	if ($siloPerSec[$i].rate -eq 0) { $drainTailSamples++ } else { break }
}

# Exception lines in the silo journal during this cohort's window
# include cross-cohort residual-grain noise: the silo runs for the
# lifetime of performance-report.ps1 (not per cohort), so wedged WAL
# grains from prior cohorts' trees continue to throw against their
# saturation-residual storage rows under the current cohort's wall
# clock. Filtering by the current cohort's tree id before counting
# yields an accurate per-cohort exception tally for the verdict.
# The raw count is preserved as a diagnostic so the operator can still
# see total log noise when triaging.
$exceptionCounts = Get-CohortExceptionCount -LogPath $siloLog -CurrentTreeId $treeId
$exceptionCount  = [int]$exceptionCounts.Filtered
$exceptionRaw    = [int]$exceptionCounts.Raw
$exceptionCrossCohort = [int]$exceptionCounts.Excluded

# Pull written / elapsed / active out of the FINAL line. Tolerates the
# older single-avg FINAL line (active== reported as N/A in that case).
$writtenFinal = 0; $elapsedFinal = 0.0; $activeFinal = 0.0
$failedFinal  = 0; $avgTotalFinal = 0.0; $avgActiveFinal = 0.0
if ($siloFinal) {
	$line = $siloFinal.Line
	if ($line -match '(?:ops|written)=([\d,]+)')                { $writtenFinal  = [long]($matches[1] -replace ',','') }
	if ($line -match 'failed=([\d,]+)')                          { $failedFinal   = [long]($matches[1] -replace ',','') }
	if ($line -match 'elapsed=([\d.]+)s')                        { $elapsedFinal  = [double]$matches[1] }
	if ($line -match 'active=([\d.]+)s')                         { $activeFinal   = [double]$matches[1] }
	if ($line -match '\(avg\)=([\d,]+)')                         { $avgTotalFinal = [double](($matches[1]) -replace ',','') }
	if ($line -match '\(active avg\)=([\d,]+)')                  { $avgActiveFinal = [double](($matches[1]) -replace ',','') }
}

# Sampler stats: peak silo RSS (GiB), avg + max silo CPU%, avg + max system CPU%.
$samples = @()
if (Test-Path $samplerLog) {
	$imported = @(Import-Csv $samplerLog -ErrorAction SilentlyContinue)
	if ($imported) { $samples = $imported }
}
function _AggMax($prop) { if (@($samples).Count -gt 0) { ($samples | Measure-Object -Property $prop -Maximum).Maximum } else { 0 } }
function _AggAvg($prop) { if (@($samples).Count -gt 0) { [math]::Round(($samples | Measure-Object -Property $prop -Average).Average, 1) } else { 0 } }
$peakRssKib   = [int](_AggMax 'silo_rss_kib')
$peakRssGiB   = [math]::Round($peakRssKib / 1024 / 1024, 2)
$avgSiloCpu   = _AggAvg 'silo_cpu_pct'
$maxSiloCpu   = _AggMax 'silo_cpu_pct'
$avgSysCpu    = _AggAvg 'sys_cpu_pct'
$maxSysCpu    = _AggMax 'sys_cpu_pct'
$memTotalKib  = if (@($samples).Count -gt 0) { [int](($samples | Select-Object -Last 1).sys_mem_total_kib) } else { 0 }
$memTotalGiB  = [math]::Round($memTotalKib / 1024 / 1024, 1)

# Verdict computation. Pre-section-31 the runner declared HEALTHY based
# solely on (watchdog=0 AND walSlot=0 AND walAppend=0). Empirically that
# misses three independent failure modes that all leave those counters at
# zero (cohort 2026-06-05 d8-ceiling-discovery): missing FINAL, non-zero
# `failed=N` from per-second samples or FINAL, and a long post-producer
# zero-rate drain tail. Verdict now considers each independently and
# returns the most-severe state.
#
# State precedence (worst-first):
#   WEDGE     - drain-tail >= drainWedgeThreshold OR no FINAL emitted at all
#   FAILED    - any per-second `failed=N>0` sample OR FINAL failed>0
#   DEGRADED  - watchdog/wal-slot/wal-append OR exception lines OR
#               brief drain-tail OR FINAL missing the activeAvg field
#   HEALTHY   - none of the above; FINAL emitted, no failures, clean drain
$drainWedgeThreshold = 10  # samples (~10 s of trailing rate=0 post-producer)
$verdictReasons = @()
$verdictState   = 'HEALTHY'
function _DowngradeVerdict([string]$target) {
	$order = @{ 'HEALTHY'=0; 'DEGRADED'=1; 'FAILED'=2; 'WEDGE'=3 }
	if ($order[$target] -gt $order[$script:verdictState]) { $script:verdictState = $target }
}
if (-not $sawFinal -or -not $siloFinal) {
	_DowngradeVerdict 'WEDGE'
	$verdictReasons += 'no FINAL emitted'
}
if ($drainTailSamples -ge $drainWedgeThreshold) {
	_DowngradeVerdict 'WEDGE'
	$verdictReasons += "$drainTailSamples-sample drain tail"
}
if ($failedFinal -gt 0 -or $failedSamples -gt 0) {
	_DowngradeVerdict 'FAILED'
	if ($failedFinal -gt 0) { $verdictReasons += "FINAL failed=$failedFinal" }
	elseif ($failedSamples -gt 0) { $verdictReasons += "$failedSamples per-second sample(s) carried failed>0" }
}
if ($watchdog -gt 0 -or $walSlot -gt 0 -or $walAppend -gt 0) {
	_DowngradeVerdict 'DEGRADED'
	$verdictReasons += "watchdog=$watchdog wal-slot=$walSlot wal-append=$walAppend"
}
if ($exceptionCount -gt 0) {
	_DowngradeVerdict 'DEGRADED'
	$verdictReasons += "$exceptionCount exception line(s)"
}

Write-Host ''
Write-Host '=== Cohort complete ===' -ForegroundColor Green
Write-Host ("Host         : {0}" -f $vmInfo)
Write-Host ("Cohort       : {0}" -f $cohortName)
Write-Host ("Producer     : {0}" -f $finalState)
if ($prodSummary) { Write-Host ("  {0}" -f $prodSummary.Line) }
if ($siloFinal)   { Write-Host ("Silo FINAL   : {0}" -f $siloFinal.Line.Trim()) }
elseif ($siloTail) { Write-Host ("Silo last    : {0}" -f $siloTail.Line.Trim()) }

# Steady-state mean (section 27.1) is the primary throughput metric. Always
# print if we have at least one usable sample; this is what cohort A/B
# comparisons should use. FINAL `(active avg)` is shown as a secondary
# diagnostic when it differs materially (and is suppressed for WEDGE
# verdicts where it is provably wrong).
if ($steadyCount -gt 0) {
	$inFlightLabel = if ($steadyCount -gt 0) { " inFlight med/max={0}/{1}" -f $inFlightMedian, $inFlightMax } else { '' }
	Write-Host ("Steady mean  : {0:N0} e/s (n={1} samples, t>=15s, rate>0){2}" -f $steadyMean, $steadyCount, $inFlightLabel)
} else {
	Write-Host 'Steady mean  : -- no nonzero per-second samples --' -ForegroundColor Yellow
}
if ($writtenFinal -gt 0) {
	$activeAvgIsTrustworthy = ($verdictState -ne 'WEDGE')
	$activeAvgSuffix = if ($activeAvgIsTrustworthy) { '' } else { ' (drain-inflated; ignore)' }
	Write-Host ("FINAL active : {0:N0} entries in {1:0.0}s active = {2:N0}/s{3}{4}" -f `
		$writtenFinal, $activeFinal, $avgActiveFinal, $(if ($failedFinal -gt 0) { " (failed=$failedFinal)" } else { '' }), $activeAvgSuffix)
} else {
	Write-Host 'FINAL active : -- no FINAL emitted --' -ForegroundColor Yellow
}
Write-Host ("Drain tail   : {0} trailing rate=0 sample(s) post-producer" -f $drainTailSamples)
Write-Host ("Silo CPU     : avg {0}% / peak {1}% (of one vCPU)" -f $avgSiloCpu, $maxSiloCpu)
Write-Host ("System CPU   : avg {0}% / peak {1}%" -f $avgSysCpu, $maxSysCpu)
Write-Host ("Silo RSS peak: {0} GiB (of {1} GiB)" -f $peakRssGiB, $memTotalGiB)
$exceptionsDisplay = if ($exceptionCrossCohort -gt 0) {
	"{0} (raw={1}; cross-cohort={2})" -f $exceptionCount, $exceptionRaw, $exceptionCrossCohort
} else {
	"$exceptionCount"
}
Write-Host ("Diagnostics  : stall-watchdog={0}  wal-slot={1}  wal-append={2}  exceptions={3}  failed-samples={4}" -f $watchdog, $walSlot, $walAppend, $exceptionsDisplay, $failedSamples)
$verdictColor = switch ($verdictState) {
	'HEALTHY'  { 'Green' }
	'DEGRADED' { 'Yellow' }
	'FAILED'   { 'Red' }
	'WEDGE'    { 'Red' }
	default    { 'Yellow' }
}
$verdictDetail = if ($verdictReasons.Count -gt 0) { ' (' + ($verdictReasons -join '; ') + ')' } else { '' }
Write-Host ("Verdict      : {0}{1}" -f $verdictState, $verdictDetail) -ForegroundColor $verdictColor
Write-Host "Logs         : $siloLog"
Write-Host "             : $prodLog"
Write-Host "             : $samplerLog"
} finally { & $cleanup }
