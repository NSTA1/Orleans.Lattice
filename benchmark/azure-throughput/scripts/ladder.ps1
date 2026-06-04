#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Throughput rung sweep. Thin loop over run-cohort.ps1.

.DESCRIPTION
	For each (Vehicles, TickHz) rung, runs one cohort via run-cohort.ps1,
	then parses the saved silo log for the FINAL line and appends a row to
	.ladder-results.csv in this folder. Optionally trips an early-exit
	when active throughput drops more than -DegradeThresholdPct from the
	best observed so far (handy for "find the peak" sweeps).

	The harness pins BENCH_RESPONSE_TIMEOUT_SEC=180 by default so a saturated
	rung does NOT collapse into the silo's own 30s grain-RPC deadline (see
	repro/wedge-orleans/throughput.md and wedge-plan.md s.23 / s.25 for why
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
	If non-zero, stop the sweep once a rung's active throughput drops below
	(1 - threshold/100) of the best observed throughput so far.

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
"vehicles,tickHz,durationSec,written,failed,activeSec,activeAvg,totalElapsedSec,siloCpuPeakPct,siloCpuAvgPct,sysCpuPeakPct,siloRssGiB,verdict,timestampUtc" `
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

	# Capture cohort output for parsing while still tee'ing to console.
	$out = & $cohortScript @cohortArgs 2>&1 | Tee-Object -Variable cohortOut | Out-String

	# Pull the same summary fields run-cohort.ps1 already prints, so we don't
	# duplicate parsing logic in two places.
	$written  = if ($out -match 'written=([\d,]+)')                        { [int](($matches[1]) -replace ',','') } else { 0 }
	$failed   = if ($out -match 'failed=([\d,]+)')                         { [int](($matches[1]) -replace ',','') } else { 0 }
	$active   = if ($out -match 'active=([\d.]+)s')                        { [double]$matches[1] }                 else { 0 }
	$elapsed  = if ($out -match 'elapsed=([\d.]+)s')                       { [double]$matches[1] }                 else { 0 }
	$throughput = if ($out -match '\(active avg\)=([\d,]+)')               { [int](($matches[1]) -replace ',','') } else { 0 }
	$siloCpuPeak = if ($out -match 'Silo CPU\s+:\s+avg [\d.]+% / peak ([\d.]+)%') { [double]$matches[1] } else { 0 }
	$siloCpuAvg  = if ($out -match 'Silo CPU\s+:\s+avg ([\d.]+)%')         { [double]$matches[1] }                 else { 0 }
	$sysCpuPeak  = if ($out -match 'System CPU\s+:\s+avg [\d.]+% / peak ([\d.]+)%') { [double]$matches[1] } else { 0 }
	$rssGiB      = if ($out -match 'Silo RSS peak:\s+([\d.]+) GiB')        { [double]$matches[1] }                 else { 0 }
	$verdict     = if ($out -match 'Verdict\s+:\s+(\S+)')                  { $matches[1] }                          else { 'UNKNOWN' }
	$tsUtc       = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')

	"${vehicles},${tickHz},${DurationSec},${written},${failed},${active},${throughput},${elapsed},${siloCpuPeak},${siloCpuAvg},${sysCpuPeak},${rssGiB},${verdict},${tsUtc}" `
		| Add-Content -Path $ResultsCsv -Encoding utf8

	Write-Host "[ladder] rung $vehicles:$tickHz -> throughput=$throughput e/s, failed=$failed, verdict=$verdict" -ForegroundColor Yellow

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
	}

	if ($i -lt $Rungs.Count - 1 -and $CooldownSec -gt 0) {
		Start-Sleep -Seconds $CooldownSec
	}
}

Write-Host ''
Write-Host '=== Sweep complete ===' -ForegroundColor Green
Write-Host "Results CSV: $ResultsCsv"
Import-Csv $ResultsCsv | Format-Table -AutoSize
