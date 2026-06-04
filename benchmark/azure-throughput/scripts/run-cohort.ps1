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
	& scp @sshOpts $tmpSampler.FullName "${sshTarget}:/tmp/cohort-sampler.sh" | Out-Null
	if ($LASTEXITCODE -ne 0) { throw 'scp of sampler script failed.' }
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
	$hit = Invoke-SshQuery "sudo -n journalctl -u lattice-silo --since '$cursorStamp' --no-pager --output=cat | grep -F 'FINAL written=' | head -1"
	if ($hit) { $sawFinal = $true; Write-Host "  $hit" -ForegroundColor Green; break }
	Start-Sleep -Seconds 2
}
if (-not $sawFinal) { Write-Host '  no FINAL line seen within 60s; silo may be wedged.' -ForegroundColor Yellow }

# Pull merged journals + sampler back. The previous attempts to redirect
# inside a sudo subshell over ssh hit quoting + permission edge cases;
# simplest reliable path is to capture journalctl's stdout into a local
# file via ssh's own stream (which is read fully before ssh exits, so
# no half-read pipe stalls).
Write-Host 'Extracting journals + sampler...' -ForegroundColor Cyan
$samplerLog = Join-Path $logDir "sampler-$cohortName.csv"

function Save-Remote([string]$remoteCmd, [string]$localPath) {
	$tmp = "$localPath.tmp"
	& ssh @sshOpts $sshTarget $remoteCmd *> $tmp
	if ($LASTEXITCODE -ne 0) { throw "ssh '$remoteCmd' failed (exit $LASTEXITCODE)." }
	Move-Item -Force $tmp $localPath
}

Save-Remote "sudo -n journalctl -u lattice-silo --since $cursorStamp --no-pager --output=cat" $siloLog
Save-Remote "sudo -n journalctl -u lattice-producer --since $cursorStamp --no-pager --output=cat" $prodLog
& scp @sshOpts "${sshTarget}:/tmp/cohort-sampler.csv" $samplerLog 2>$null | Out-Null

Invoke-Ssh 'sudo systemctl stop lattice-producer 2>/dev/null || true'

# Parse summaries.
$prodSummary = Select-String -Path $prodLog -Pattern '\[producer\] DONE' -SimpleMatch | Select-Object -First 1
$siloFinal   = Select-String -Path $siloLog -Pattern 'FINAL written=' -SimpleMatch | Select-Object -First 1
$watchdog    = @(Select-String -Path $siloLog -Pattern '\[stall-watchdog\]' -SimpleMatch).Count
$walSlot     = @(Select-String -Path $siloLog -Pattern '\[wal-slot\]' -SimpleMatch).Count
$walAppend   = @(Select-String -Path $siloLog -Pattern '\[wal-append\]' -SimpleMatch).Count
$siloTail    = @(Select-String -Path $siloLog -Pattern '^\[silo\] t=') | Select-Object -Last 1

# Pull written / elapsed / active out of the FINAL line. Tolerates the
# older single-avg FINAL line (active== reported as N/A in that case).
$writtenFinal = 0; $elapsedFinal = 0.0; $activeFinal = 0.0
$failedFinal  = 0; $avgTotalFinal = 0.0; $avgActiveFinal = 0.0
if ($siloFinal) {
	$line = $siloFinal.Line
	if ($line -match 'written=([\d,]+)')                         { $writtenFinal  = [long]($matches[1] -replace ',','') }
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

Write-Host ''
Write-Host '=== Cohort complete ===' -ForegroundColor Green
Write-Host ("Host         : {0}" -f $vmInfo)
Write-Host ("Cohort       : {0}" -f $cohortName)
Write-Host ("Producer     : {0}" -f $finalState)
if ($prodSummary) { Write-Host ("  {0}" -f $prodSummary.Line) }
if ($siloFinal)   { Write-Host ("Silo FINAL   : {0}" -f $siloFinal.Line.Trim()) }
elseif ($siloTail) { Write-Host ("Silo last    : {0}" -f $siloTail.Line.Trim()) }
if ($writtenFinal -gt 0) {
	Write-Host ("Throughput   : {0:N0} entries in {1:0.0}s active = {2:N0}/s{3}" -f `
		$writtenFinal, $activeFinal, $avgActiveFinal, $(if ($failedFinal -gt 0) { " (failed=$failedFinal)" } else { '' }))
} else {
	Write-Host 'Throughput   : -- no final output: unknown --' -ForegroundColor Yellow
}
Write-Host ("Silo CPU     : avg {0}% / peak {1}% (of one vCPU)" -f $avgSiloCpu, $maxSiloCpu)
Write-Host ("System CPU   : avg {0}% / peak {1}%" -f $avgSysCpu, $maxSysCpu)
Write-Host ("Silo RSS peak: {0} GiB (of {1} GiB)" -f $peakRssGiB, $memTotalGiB)
Write-Host ("Diagnostics  : stall-watchdog={0}  wal-slot={1}  wal-append={2}" -f $watchdog, $walSlot, $walAppend)
$wedgeVerdict = if ($watchdog -eq 0 -and $walSlot -eq 0 -and $walAppend -eq 0) { 'HEALTHY' } else { 'WEDGE SIGNAL' }
Write-Host ("Verdict      : {0}" -f $wedgeVerdict) -ForegroundColor $(if ($wedgeVerdict -eq 'HEALTHY') {'Green'} else {'Red'})
Write-Host "Logs         : $siloLog"
Write-Host "             : $prodLog"
Write-Host "             : $samplerLog"
} finally { & $cleanup }
