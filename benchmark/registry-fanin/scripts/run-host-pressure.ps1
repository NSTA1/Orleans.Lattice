<#
.SYNOPSIS
	Cold-start the rig at a FIXED tree count while a throwaway burner container
	contends for the host, to separate 'the registry saturates because there are
	many trees' from 'the registry saturates because the silo is starved'.

.DESCRIPTION
	This arm exists because four K=80 cold starts disagreed with each other, and
	the variable that predicted the disagreement was not K.

	Offered registry load was constant across all four runs to within 10%
	(1021-1132 calls per 30 s, measured, not assumed). Three produced zero
	timeouts. One produced 28, of which 26 were on
	latticeregistry/_lattice_trees. The distinguishing feature of that one run
	was recorded only incidentally, in an end-of-run host snapshot: a
	NEIGHBOURING container was burning 5.96 CPU cores and sitting at 11.89 GiB
	against a 12 GiB cap, while the rig's own silo was idle at 0.27 cores. The
	other three runs saw that neighbour at 1.51, 1.85 and 0.56 cores.

	So the rig reproduced a registry storm once in four attempts at constant K
	and constant offered load, and the one attempt that stormed is the one where
	something else was hammering the host. That is not a tree-count scaling law.
	It is consistent with the silo being descheduled or I/O-starved for long
	enough that a 30 s deadline expires on whatever activation happens to be
	busiest - and the registry is the busiest single activation in the system by
	construction, being a singleton every tree talks to.

	This script tests that directly, by holding K at the LIVE estate's actual
	tree count (20) and manufacturing the host pressure instead. If a storm
	appears at K=20 under pressure, tree count is not the driver and the
	'(trees) x (per-tree services)' law is not what breaks the registry.

.NOTES
	The burner is a throwaway container with its own name and its own writable
	layer. It never touches the protected container, the protected volume, or
	the rig's volume. It is removed in a finally block so an interrupted run
	cannot leave the host under load.

	CPU and I/O are separately controllable because the storm run's neighbour
	was at a peak in BOTH, and the host was NOT CPU-saturated overall at the
	time (about 6.3 of 16 cores), which makes disk contention at least as
	plausible a mechanism as CPU starvation. Running one knob at a time is how
	they are told apart.
#>
[CmdletBinding()]
param(
	[int] $Trees = 20,
	[ValidateRange(0, 32)]
	[int] $BurnerCpu = 8,
	[ValidateRange(0, 16)]
	[int] $BurnerIoWriters = 4,
	[ValidateRange(60, 3600)]
	[int] $WindowSeconds = 420,
	[ValidateRange(10, 300)]
	[int] $SampleSeconds = 30,
	[string] $Label = '',
	[switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$config = Get-FanInConfig -ScriptRoot $PSScriptRoot
$null = Assert-FanInIsolation -Config $config

if (-not $Label) { $Label = "pressure-K$Trees-cpu$BurnerCpu-io$BurnerIoWriters" }

$burnerName = 'fanin-burner'
$protectedNames = @('repocontextcontainer-repocontext-1', 'repocontextcontainer-embedder-1')
if ($protectedNames -contains $burnerName) {
	throw "burner name '$burnerName' collides with a protected container"
}

function Stop-Burner {
	$existing = @(docker ps -aq --filter "name=^/$burnerName$" 2>$null | Where-Object { $_ })
	if ($existing.Count -gt 0) {
		Write-Host "Removing burner $burnerName" -ForegroundColor DarkGray
		$null = docker rm -f $burnerName 2>&1
	}
}

# A burner left running would poison every subsequent run on this host, so it is
# removed before it is started as well as after.
Stop-Burner

$rigScript = Join-Path $PSScriptRoot 'run-birth-curve.ps1'
if (-not (Test-Path $rigScript)) { throw "missing $rigScript" }

try {
	if ($BurnerCpu -gt 0 -or $BurnerIoWriters -gt 0) {
		# busybox is tiny and already present on any machine that has pulled a
		# base image; the loops are plain shell so nothing needs installing.
		#   - CPU: N spinners on /dev/null.
		#   - I/O: N writers each cycling a 256 MiB file inside the container's
		#     own writable layer, with a sync so the writes actually reach the
		#     same disk queue the silo's WAL uses rather than sitting in page
		#     cache.
		$burnScript = @'
set -e
i=0
while [ $i -lt __CPU__ ]; do
  (while :; do :; done) &
  i=$((i+1))
done
j=0
while [ $j -lt __IO__ ]; do
  (while :; do dd if=/dev/zero of=/burn.$j bs=1M count=256 2>/dev/null; sync; rm -f /burn.$j; done) &
  j=$((j+1))
done
wait
'@
		# A literal here-string, then substitution. An expandable here-string
		# would interpolate $i and $j as PowerShell variables - and backslash is
		# not an escape character in PowerShell, so the obvious \$i does not
		# prevent it and the script dies before the burner ever starts.
		#
		# The variable is NOT called $script: that is a scope qualifier, and
		# under StrictMode it does not behave as an ordinary variable.
		# CR must be stripped. A PowerShell here-string carries CRLF line
		# endings, and busybox sh reads the trailing CR as part of the token -
		# 'set -e' becomes 'set -e\r' and the shell dies with
		# "set: line 0: illegal option -" before a single spinner starts. The
		# container then exits immediately, which presents as "burner failed to
		# start" with no obvious cause.
		$burnScript = $burnScript.Replace('__CPU__', "$BurnerCpu").Replace('__IO__', "$BurnerIoWriters").Replace("`r`n", "`n").Replace("`r", "`n")

		Write-Host "Starting burner: $BurnerCpu cpu spinners, $BurnerIoWriters io writers" -ForegroundColor Yellow
		# docker's output is kept rather than discarded, so a failure to start
		# is diagnosable instead of surfacing only as the guard below.
		$runOutput = @(docker run -d --name $burnerName busybox sh -c $burnScript 2>&1 | ForEach-Object { "$_" })
		Start-Sleep -Seconds 10

		$check = @(docker ps -q --filter "name=^/$burnerName$" 2>$null | Where-Object { $_ })
		if ($check.Count -eq 0) {
			$why = ($runOutput -join '; ')
			$burnerLogs = @(docker logs $burnerName 2>&1 | ForEach-Object { "$_" }) -join '; '
			throw "burner failed to start; refusing to run an unpressured measurement under a pressure label. docker run: $why. logs: $burnerLogs"
		}

		$stat = docker stats --no-stream --format '{{.CPUPerc}}' $burnerName 2>&1
		Write-Host "  burner cpu: $stat" -ForegroundColor Yellow
	}

	$curveArgs = @(
		'-NoProfile', '-File', $rigScript,
		'-Trees', $Trees,
		'-WindowSeconds', $WindowSeconds,
		'-SampleSeconds', $SampleSeconds,
		'-Label', $Label
	)
	if ($SkipBuild) { $curveArgs += '-SkipBuild' }

	& pwsh @curveArgs
	if ($LASTEXITCODE -ne 0) { throw "birth curve exited $LASTEXITCODE" }
}
finally {
	Stop-Burner
}

Write-Host ''
Write-Host "Pressure arm complete: $Label" -ForegroundColor Green
Write-Host 'Compare TimeoutsTotal against the unpressured run at the same K.' -ForegroundColor Green
