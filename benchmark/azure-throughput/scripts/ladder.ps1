#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Throughput rung sweep. Thin loop over run-cohort.ps1.

.DESCRIPTION
	For each (Vehicles, TickHz) rung, runs one cohort via run-cohort.ps1,
	takes the LatticeBench.CohortResult object that script returns on its
	success stream, and appends a row to .ladder-results.csv in this folder.
	A rung whose cohort threw records verdict ERROR and the sweep continues.

	The harness pins BENCH_RESPONSE_TIMEOUT_SEC=180 by default so a saturated
	rung does NOT collapse into the silo's own 30s grain-RPC deadline (see
	throughput.md s.25 and wedge-plan.md s.23, both in this folder, for why
	this matters).

.PARAMETER Rungs
	Array of "vehicles:tickHz" pairs. Default is a 5-rung sweep covering
	1k -> 50k vehicles at 5 Hz.

.PARAMETER DurationSec
	Producer seconds per rung (default 30).

.PARAMETER CooldownSec
	Seconds between rungs (default 5).

.PARAMETER NamePrefix
	Override the VM prefix in parameters.local.ps1 (e.g. 'lat01').

.PARAMETER ParametersFile
	Explicit path to a parameters .ps1 file (overrides the default discovery).

.PARAMETER ResponseTimeoutSec
	BENCH_RESPONSE_TIMEOUT_SEC (default 180). Drop to 30 to reproduce the
	historical "grain-RPC deadline fires" failure mode for diagnostics.

.PARAMETER ExtraSiloEnv
	Extra silo env vars forwarded to every cohort (merged with the
	ResponseTimeoutSec setting).

.PARAMETER DegradeThresholdPct
	If non-zero, stop the sweep once a rung's steady-state throughput drops
	below (1 - threshold/100) of the best observed throughput so far, or once
	a rung yields no usable throughput at all after one that did.

.PARAMETER ResultsCsv
	Output CSV path. Default: scripts/.ladder-results.csv (gitignored).

.EXAMPLE
	./ladder.ps1 -NamePrefix lat01

.EXAMPLE
	./ladder.ps1 -NamePrefix lat01 -Rungs '4000:5','6000:5','8000:5','10000:5','12000:5' -DurationSec 30 -DegradeThresholdPct 15
#>
[CmdletBinding()]
param(
	[string[]] $Rungs = @('1000:5','5000:5','10000:5','20000:5','50000:5'),
	[int] $DurationSec = 30,
	[int] $CooldownSec = 5,
	[string] $NamePrefix,
	[string] $ParametersFile,
	[int] $ResponseTimeoutSec = 180,
	[hashtable] $ExtraSiloEnv = @{},
	[int] $DegradeThresholdPct = 0,
	[string] $ResultsCsv
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$cohortScript = Join-Path $here 'run-cohort.ps1'
if (-not (Test-Path $cohortScript)) { throw "missing $cohortScript." }
if (-not $ResultsCsv) { $ResultsCsv = Join-Path $here '.ladder-results.csv' }

$envHash = @{} + $ExtraSiloEnv
$envHash['BENCH_RESPONSE_TIMEOUT_SEC'] = "$ResponseTimeoutSec"

Write-Host "[ladder] rungs=$($Rungs.Count) duration=${DurationSec}s cooldown=${CooldownSec}s responseTimeout=${ResponseTimeoutSec}s" -ForegroundColor Cyan
Write-Host "[ladder] results -> $ResultsCsv" -ForegroundColor DarkGray

# Fresh CSV header per sweep so the file is self-describing.
"vehicles,tickHz,durationSec,written,failed,activeSec,steadyMean,activeAvg,drainTailSamples,totalElapsedSec,siloCpuPeakPct,siloCpuAvgPct,sysCpuPeakPct,siloRssGiB,verdict,timestampUtc" `
	| Out-File -FilePath $ResultsCsv -Encoding utf8

$bestThroughput = 0
$bestRung = ''

for ($i = 0; $i -lt $Rungs.Count; $i++) {
	$rung = $Rungs[$i]
	$parts = $rung -split ':'
	if ($parts.Count -ne 2) { throw "bad rung '$rung'; expected 'vehicles:tickHz'." }
	$vehicles = [int]$parts[0]
	$tickHz = [int]$parts[1]

	Write-Host ''
	Write-Host "===== [ladder] rung $($i + 1)/$($Rungs.Count): vehicles=$vehicles tickHz=$tickHz =====" -ForegroundColor Magenta

	$cohortArgs = @{
		Vehicles      = $vehicles
		TickHz        = $tickHz
		DurationSec   = $DurationSec
		ExtraSiloEnv  = $envHash
	}
	if ($NamePrefix)     { $cohortArgs.NamePrefix = $NamePrefix }
	if ($ParametersFile) { $cohortArgs.ParametersFile = $ParametersFile }

	# run-cohort.ps1 prints its human summary with Write-Host (which reaches
	# the console but cannot be captured) and returns one
	# LatticeBench.CohortResult object on the success stream. Take that
	# object; anything else on the success stream is remote command output
	# and is echoed so the console still shows it. Before this, the ladder
	# regex-scraped captured text that never held the summary, so every rung
	# recorded 0 / UNKNOWN and -DegradeThresholdPct could never fire.
	$result = $null
	try {
		& $cohortScript @cohortArgs | ForEach-Object {
			if ($_ -is [psobject] -and $_.PSObject.TypeNames -contains 'LatticeBench.CohortResult') { $result = $_ }
			else { Write-Host $_ }
		}
	} catch {
		# One rung's ssh/scp failure must not discard the rungs already run;
		# record it and move on.
		Write-Host "[ladder] rung ${vehicles}:${tickHz} threw: $_" -ForegroundColor Red
	}

	$tsUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
	if ($null -eq $result) {
		"${vehicles},${tickHz},${DurationSec},0,0,0,0,0,0,0,0,0,0,0,ERROR,${tsUtc}" | Add-Content -Path $ResultsCsv -Encoding utf8
		Write-Host "[ladder] rung ${vehicles}:${tickHz} -> no cohort result (ERROR)" -ForegroundColor Red
		if ($i -lt $Rungs.Count - 1 -and $CooldownSec -gt 0) { Start-Sleep -Seconds $CooldownSec }
		continue
	}

	$inv = [System.Globalization.CultureInfo]::InvariantCulture
	$written    = $result.Written
	$failed     = $result.Failed
	$active     = ([double]$result.ActiveSec).ToString('0.0', $inv)
	$elapsed    = ([double]$result.ElapsedSec).ToString('0.0', $inv)
	$steadyMean = [int]$result.SteadyMean
	$activeAvg  = [int]$result.ActiveAvg
	$drainTail  = $result.DrainTailSamples
	$siloCpuPeak = ([double]$result.SiloCpuPeakPct).ToString($inv)
	$siloCpuAvg  = ([double]$result.SiloCpuAvgPct).ToString($inv)
	$sysCpuPeak  = ([double]$result.SysCpuPeakPct).ToString($inv)
	$rssGiB      = ([double]$result.SiloRssPeakGiB).ToString($inv)
	$verdict     = $result.Verdict

	# `throughput` is the per-rung "best honest sustained number" used for the
	# DegradeThresholdPct early-stop guard: the section 27.1 steady-state mean,
	# falling back to FINAL active-avg only when no per-second samples reached
	# the mid-window. A WEDGE rung's active-avg is drain-inflated, so it is
	# never used as a fallback.
	$throughput = if ($steadyMean -gt 0) { $steadyMean } elseif ($verdict -ne 'WEDGE') { $activeAvg } else { 0 }

	"${vehicles},${tickHz},${DurationSec},${written},${failed},${active},${steadyMean},${activeAvg},${drainTail},${elapsed},${siloCpuPeak},${siloCpuAvg},${sysCpuPeak},${rssGiB},${verdict},${tsUtc}" `
		| Add-Content -Path $ResultsCsv -Encoding utf8

	Write-Host "[ladder] rung ${vehicles}:${tickHz} -> steady=$steadyMean e/s, failed=$failed, drainTail=${drainTail}s, verdict=$verdict" -ForegroundColor Yellow

	if ($DegradeThresholdPct -gt 0 -and $throughput -gt 0) {
		if ($throughput -gt $bestThroughput) {
			$bestThroughput = $throughput
			$bestRung = "$vehicles`:$tickHz"
		}
		$floor = [int]($bestThroughput * (1 - $DegradeThresholdPct / 100.0))
		if ($throughput -lt $floor -and $i -gt 0) {
			Write-Host "[ladder] STOP: $throughput e/s is below $DegradeThresholdPct% floor of best ($bestThroughput at $bestRung)." -ForegroundColor Red
			break
		}
	} elseif ($DegradeThresholdPct -gt 0 -and $bestThroughput -gt 0) {
		# A rung that produced no usable throughput after one that did is past
		# the cliff by definition (typically a WEDGE).
		Write-Host "[ladder] STOP: rung produced no usable throughput (verdict=$verdict) after $bestThroughput e/s at $bestRung." -ForegroundColor Red
		break
	}

	if ($i -lt $Rungs.Count - 1 -and $CooldownSec -gt 0) {
		Start-Sleep -Seconds $CooldownSec
	}
}

Write-Host ''
Write-Host '=== Sweep complete ===' -ForegroundColor Green
Write-Host "Results CSV: $ResultsCsv"
Import-Csv $ResultsCsv | Format-Table -AutoSize
