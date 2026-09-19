#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Runs one measurement cell: populate an estate, cold-start under it, measure.

.DESCRIPTION
	One cell is (K trees) x (leaves per tree) x (one cold start) x (one window).
	Both axes are parameters here, so the same script serves the breadth arm
	(vary K at fixed depth) and the depth arm (vary depth at fixed K). Keeping
	them in one script is deliberate: two scripts would drift, and a difference
	in measurement procedure between the arms would be indistinguishable in the
	results from a difference in the thing being measured.

	Every cell records the estate it was actually taken at - tree count and
	implied leaf count - rather than the switch values requested, because the
	host contributes trees of its own and a populate can fall short.

.PARAMETER Trees
	Driver trees to create. The REPORTED tree count is this plus the host's own,
	which is read back from the registry rather than assumed.

.PARAMETER LeavesPerTree
	Populate depth target per tree.

.PARAMETER Runs
	Replicates for this cell. Defaults to the configured Replicates (3). One
	sample per cell cannot distinguish a real effect from the dispersion that is
	already known to be present: observed storm magnitude varied 103 vs 18 across
	two runs of the same configuration, a factor of 5.7.

.PARAMETER EnumeratePct
	Share of probe calls issued against the non-interleaved range scan.
#>

[CmdletBinding()]
param(
	[int] $Trees = 20,
	[int] $LeavesPerTree = 8,
	[int] $Runs,
	[int] $EnumeratePct = 0,
	[int] $WindowSeconds,
	[string] $Label,
	[string] $ResultsDir,
	[switch] $KeepEstate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$config = Get-FanInConfig -ScriptRoot $PSScriptRoot
$null = Assert-FanInIsolation -Config $config

if (-not $PSBoundParameters.ContainsKey('Runs')) { $Runs = [int] $config.Replicates }
if (-not $PSBoundParameters.ContainsKey('WindowSeconds')) { $WindowSeconds = [int] $config.MeasureWindowSec }
if (-not $Label) { $Label = "K$Trees-leaves$LeavesPerTree" }
if (-not $ResultsDir) { $ResultsDir = Join-Path (Split-Path -Parent $PSScriptRoot) 'results' }

if (-not (Test-Path $ResultsDir)) { $null = New-Item -ItemType Directory -Path $ResultsDir -Force }

$rig = Join-Path $PSScriptRoot 'rig.ps1'
$collect = Join-Path $PSScriptRoot 'collect-window.ps1'

function Invoke-Driver {
	param([string] $Args, [int] $TimeoutSec = 3600)

	& pwsh -NoProfile -File $rig driver -DriverArgs $Args
	if ($LASTEXITCODE -ne 0) { throw "driver failed: $Args" }
}

Write-Host "=== cell $Label : $Runs run(s), window ${WindowSeconds}s ===" -ForegroundColor Cyan

$cellResults = @()

for ($run = 1; $run -le $Runs; $run++) {
	Write-Host "--- run $run/$Runs ---" -ForegroundColor Yellow

	# 1. Fresh state. Every run starts from an empty volume so the estate under
	#    measurement is only what this cell built. Reusing a volume across runs
	#    would let depth accumulate silently and turn a breadth sweep into an
	#    uncontrolled depth sweep.
	& pwsh -NoProfile -File $rig reset | Out-Null
	& pwsh -NoProfile -File $rig up | Out-Null
	& pwsh -NoProfile -File $rig ready | Out-Null

	# 2. Build the estate.
	Invoke-Driver "create --trees $Trees"
	if ($LeavesPerTree -gt 0) {
		Invoke-Driver "populate --trees $Trees --leaves-per-tree $LeavesPerTree --value-bytes $($config.ValueBytes) --keys-per-leaf $($config.KeysPerLeaf) --batch-size $($config.PopulateBatchSize)"
	}

	# 3. Census the estate BEFORE the cold start, so the reported scale is what
	#    was actually built rather than what was asked for.
	$censusPath = Join-Path $ResultsDir "$Label-run$run-estate.json"
	Invoke-Driver "census --trees $Trees --output /reports/$Label-run$run-estate.json"

	# 4. THE COLD START. This is the event under measurement: the estate exists,
	#    the process does not, and the per-tree background services all come up
	#    at once and fan in onto the registry singleton.
	& pwsh -NoProfile -File $rig down | Out-Null
	& pwsh -NoProfile -File $rig up | Out-Null
	$readyJson = & pwsh -NoProfile -File $rig ready
	$ready = ($readyJson | Out-String) | ConvertFrom-Json
	$readyAtUtc = [datetime]::Parse(
		$ready.ReadyAtUtc,
		[cultureinfo]::InvariantCulture,
		[System.Globalization.DateTimeStyles]::AdjustToUniversal -bor [System.Globalization.DateTimeStyles]::AssumeUniversal)

	# Counters are re-baselined here by construction: the container was just
	# recreated, so every counter starts from zero in a new process lifetime and
	# nothing is carried across the boundary. collect-window records StartedAt
	# with every reading so that remains checkable rather than merely asserted.

	# 5. Hold offered load across the window when asked. The probe runs in the
	#    background for the whole window so the registry sees sustained load
	#    rather than a burst that has drained before the measurement is taken.
	$loadJob = $null
	if ($EnumeratePct -gt 0) {
		$loadArgs = "probe --trees $Trees --rate $($config.ContentionRate) --duration $WindowSeconds --enumerate-pct $EnumeratePct --output /reports/$Label-run$run-probe.json"
		$loadJob = Start-Job -ScriptBlock {
			param($rigPath, $a)
			& pwsh -NoProfile -File $rigPath driver -DriverArgs $a
		} -ArgumentList $rig, $loadArgs
	}

	# 6. Measure the window.
	Write-Host "measuring ${WindowSeconds}s from $($readyAtUtc.ToString('o'))" -ForegroundColor DarkGray
	Start-Sleep -Seconds $WindowSeconds

	if ($loadJob) {
		$null = Wait-Job $loadJob -Timeout 300
		Receive-Job $loadJob | Out-Null
		Remove-Job $loadJob -Force
	}

	$windowPath = Join-Path $ResultsDir "$Label-run$run-window.json"
	& pwsh -NoProfile -File $collect `
		-ReadyAtUtc $readyAtUtc `
		-WindowSeconds $WindowSeconds `
		-Label "$Label-run$run" `
		-OutputPath $windowPath | Out-Null

	$cellResults += $windowPath

	# 7. Teardown unless the estate is being reused deliberately.
	if (-not $KeepEstate) {
		Invoke-Driver "teardown --trees $Trees"
	}
}

Write-Host "cell $Label complete: $($cellResults.Count) window(s)" -ForegroundColor Green
$cellResults
