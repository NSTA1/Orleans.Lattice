#!/usr/bin/env pwsh
<#
.SYNOPSIS
	The arm that reaches the regime in which the fan-in bound actually binds,
	and the A/B control that proves the rig can tell the bounded path from the
	unbounded one (issue #3266).

.DESCRIPTION
	WHY THIS ARM EXISTS. The bound (RegistryFanInGate.GlobalMaxConcurrentReads,
	16) was merged behind this rig, the rig was run, and it came back green on
	three figures: an observed width of 1.9 to 3.2, an admission wait of
	~0.002 ms, and 0.7% of reads batched. Every one of those is consistent with
	a comfortable bound - and every one of them is also exactly what the
	apparatus emits when the gate is never entered at all. The run did not
	weakly test the bound. It did not test it. See "False greens - a green check
	that never exercised its property" in
	.github/instructions/testing.instructions.md.

	The governing principle: AN INSTRUMENT THAT CANNOT REACH THE FAILING REGIME
	YIELDS NO EVIDENCE, NOT WEAK EVIDENCE. A green from such an instrument is
	worse than a red, because it additionally asserts there is nothing to fix.

	WHY THE OTHER ARMS CANNOT REACH IT. Three compounding reasons, and only the
	third is fatal:

	  1. The probe verb never touches the gate. RegistryFanInGate hangs off the
	     silo's LatticeOptionsResolver and is internal; its only callers are
	     in-silo. The probe addresses ILatticeRegistry as an Orleans CLIENT, so
	     100% of probe traffic bypasses the gate. This arm calls
	     ILattice.GetRoutingAsync(forceRefresh: true), which resolves through
	     LatticeOptionsResolver and therefore through the gate.

	  2. A permit carries a BATCH. Pump() takes Math.Min(MaxBatchSize = 64,
	     arrivals) per permit, so a single instantaneous burst needs more than
	     16 x 64 = 960 waiting ids to exhaust the permits. With arrivals spread
	     in time the first 16 each take a permit of their own and 16 concurrent
	     distinct reads suffice - the ~960 figure applies only to one
	     simultaneous burst, which is what this arm issues.

	  3. DISPERSAL, and this is the fatal one. Background services are
	     reminder-birthed with a 60-second due time, so K trees yield only
	     ~K/60 distinct-tree resolutions per second. By Little's law the offered
	     fan-in is ~(K/60) x latency, or ~K/12000 at a few milliseconds. The
	     dispersal grows exactly as fast as the load does, so scaling the estate
	     is structurally the wrong axis. The arrival process is the lever.

	WHAT THE THREE ORIGINAL FIGURES ACTUALLY WERE. All three are floors, not
	small readings:

	  - The ~0.002 ms admission wait is the instrument's STRUCTURAL FLOOR.
	    GetEntryAsync enqueues and then calls Pump() synchronously on the
	    calling thread, so an arrival that finds a free permit is dequeued by
	    its own thread in the same stack frame. No workload can make that
	    smaller, and it is what a gate that never queued emits.
	  - 0.7% batched follows from the same fact: a batch of two requires two
	    distinct ids waiting at one instant. Below the bound a small non-zero
	    share still appears, because arrivals enqueue under the lock but pump
	    outside it, so two can couple in a window of microseconds. That
	    incidental coupling does not grow with offered load and is present on a
	    gate that never queued once - it is the floor, not weak coalescing.
	  - The 1.9-3.2 "gate width" was not a gate reading at all. It is
	    MeanGlobalFanInWidth, derived from RegistryCallCensus - concurrency
	    inside the registry grain body, summed over all callers including
	    Orleans clients. The permit count had no instrument whatever, so the
	    headline criterion was UNMEASURABLE rather than unmet. The three
	    orleans_lattice_registry_admission_* series this arm reads were added
	    for that reason.

	WHAT THIS SCRIPT PRODUCES. Two windows under identical offered load:

	  - the GATED arm, driving GetRoutingAsync through the gate;
	  - the UNGATED control (--fanout-ungated), addressing ILatticeRegistry
	    directly and reaching no gate.

	The comparison between them is the deliverable. A rig that reaches the
	saturated regime but produces the same numbers with and without the bound
	has measured the workload, not the bound, and is still not an instrument.

	Optionally a third WALK-BACK window re-runs the gated arm with the wave
	dispersed (-StarvedGapMillis), reproducing the original false green on
	demand so it can be read next to the saturated one rather than described.

.PARAMETER Width
	Distinct trees released from one barrier per wave. Must exceed the permit
	count by a wide margin to place the arm in the saturated regime rather than
	on the boundary between regimes.

.PARAMETER Waves
	Waves per run. The batched SHARE is a proportion over dispatches, so a
	handful of dispatches cannot produce a meaningful one however saturated the
	gate is.

.PARAMETER GapMillis
	Inter-wave gap for the gated arm. Zero is back-to-back.

.PARAMETER StarvedGapMillis
	When -WalkBack is set, the gap used for the third window. Large enough that
	waves do not overlap, which reproduces the dispersed regime.

.PARAMETER WalkBack
	Also run the dispersed gated window, so the false green is reproduced in
	the same results directory as the fixed reading.
#>

[CmdletBinding()]
param(
	[int] $Width,
	[int] $Waves,
	[int] $GapMillis,
	[int] $StarvedGapMillis,
	[int] $Trees = 256,
	[string] $Label,
	[string] $ResultsDir,
	[switch] $WalkBack,
	[switch] $KeepEstate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$config = Get-FanInConfig -ScriptRoot $PSScriptRoot
$null = Assert-FanInIsolation -Config $config

if (-not $PSBoundParameters.ContainsKey('Width')) { $Width = [int] $config.FanoutWidth }
if (-not $PSBoundParameters.ContainsKey('Waves')) { $Waves = [int] $config.FanoutWaves }
if (-not $PSBoundParameters.ContainsKey('GapMillis')) { $GapMillis = [int] $config.FanoutGapMillis }
if (-not $PSBoundParameters.ContainsKey('StarvedGapMillis')) { $StarvedGapMillis = [int] $config.FanoutStarvedGapMillis }
if (-not $Label) { $Label = "fanout-W$Width-N$Waves" }
if (-not $ResultsDir) { $ResultsDir = Join-Path (Split-Path -Parent $PSScriptRoot) 'results' }

if ($Width -le 16) {
	throw "Width $Width does not exceed the permit count. This arm exists to leave the regime in which the bound is invisible; running it below the bound reproduces the false green rather than testing anything."
}
if ($Trees -lt $Width) { $Trees = $Width }

if (-not (Test-Path $ResultsDir)) { $null = New-Item -ItemType Directory -Path $ResultsDir -Force }

$rig = Join-Path $PSScriptRoot 'rig.ps1'
$collect = Join-Path $PSScriptRoot 'collect-window.ps1'

function Invoke-Driver {
	param([string] $Arguments)

	& pwsh -NoProfile -File $rig driver -DriverArgs $Arguments
	if ($LASTEXITCODE -ne 0) { throw "driver failed: $Arguments" }
}

# One window = one fresh silo + one fan-out run + one collection. The silo is
# recreated for every window rather than reused, because the gate's instruments
# are process-lifetime counters: a reused process would carry the previous
# window's admission history into the next one, and the A/B comparison that is
# the whole point of this arm would be reading a sum of both arms.
function Invoke-FanoutWindow {
	param(
		[string] $WindowLabel,
		[int] $Gap,
		[switch] $Ungated
	)

	Write-Host "--- window $WindowLabel (gap ${Gap}ms, $(if ($Ungated) { 'UNGATED control' } else { 'gated' })) ---" -ForegroundColor Yellow

	& pwsh -NoProfile -File $rig reset | Out-Null
	& pwsh -NoProfile -File $rig up | Out-Null
	$readyJson = & pwsh -NoProfile -File $rig ready
	$ready = ($readyJson | Out-String) | ConvertFrom-Json
	$readyAtUtc = [datetime]::Parse(
		$ready.ReadyAtUtc,
		[cultureinfo]::InvariantCulture,
		[System.Globalization.DateTimeStyles]::AdjustToUniversal -bor [System.Globalization.DateTimeStyles]::AssumeUniversal)

	Invoke-Driver "create --trees $Trees"

	$driverArgs = "fanout --trees $Trees --fanout-width $Width --fanout-waves $Waves --fanout-gap-ms $Gap --output /reports/$WindowLabel-fanout.json"
	if ($Ungated) { $driverArgs += ' --fanout-ungated' }

	$started = [datetime]::UtcNow
	Invoke-Driver $driverArgs
	$elapsed = [int][math]::Ceiling(([datetime]::UtcNow - $started).TotalSeconds) + 2

	$windowPath = Join-Path $ResultsDir "$WindowLabel-window.json"
	& pwsh -NoProfile -File $collect `
		-ReadyAtUtc $readyAtUtc `
		-WindowSeconds $elapsed `
		-Label $WindowLabel `
		-OutputPath $windowPath | Out-Null

	if (-not $KeepEstate) { Invoke-Driver "teardown --trees $Trees" }

	$windowPath
}

Write-Host "=== fan-out arm $Label : width $Width x $Waves waves ===" -ForegroundColor Cyan

$gatedPath = Invoke-FanoutWindow -WindowLabel "$Label-gated" -Gap $GapMillis
$ungatedPath = Invoke-FanoutWindow -WindowLabel "$Label-ungated" -Gap $GapMillis -Ungated

$windows = @($gatedPath, $ungatedPath)

if ($WalkBack) {
	$windows += Invoke-FanoutWindow -WindowLabel "$Label-starved" -Gap $StarvedGapMillis
}

# The A/B verdict, written next to the windows it was derived from. It is
# computed here rather than left to a reader, because the single most likely
# way this arm goes wrong is that somebody reads the saturated window alone,
# sees a width of 16, and concludes the bound was demonstrated - when a width
# of 16 on its own says only that the gate was entered, not that the gate did
# anything the unbounded path would not have done.
$gated = Get-Content -Raw $gatedPath | ConvertFrom-Json
$ungated = Get-Content -Raw $ungatedPath | ConvertFrom-Json

$verdictPath = Join-Path $ResultsDir "$Label-verdict.json"
$verdict = [ordered]@{
	Label            = $Label
	Width            = $Width
	Waves            = $Waves
	GatedWindow      = [System.IO.Path]::GetFileName($gatedPath)
	UngatedWindow    = [System.IO.Path]::GetFileName($ungatedPath)
	GatedGate        = $gated.AdmissionGate
	UngatedGate      = $ungated.AdmissionGate
	Notes            = @(
		'RegimeReached on the gated window is the field to read first: every other figure here is meaningless while it is false.',
		'The ungated window must show NO admission series at all. A gate instrument that records on a path reaching no gate would let the control manufacture evidence for the treatment.',
		'The bound limits registry CALLS, not the keys those calls carry: a permit takes up to MaxBatchSize ids, so admitted work is bounded by the product of the two constants (1024, asserted by RegistryFanInGateTests.The_downstream_ceiling_is_the_product_of_the_two_constants). Do not read this arm as showing at most 16 reads reach the registry.',
		'The executable form of this comparison is RegistryFanInRegimeTests, which runs on every CI build. This JSON demonstrates the fix on one machine on one day; the fixture defends it.')
}

$verdict | ConvertTo-Json -Depth 8 | Set-Content -Path $verdictPath -Encoding utf8

Write-Host "fan-out arm complete" -ForegroundColor Green
Write-Host "  gated   : $gatedPath" -ForegroundColor DarkGray
Write-Host "  ungated : $ungatedPath" -ForegroundColor DarkGray
Write-Host "  verdict : $verdictPath" -ForegroundColor DarkGray

$windows
