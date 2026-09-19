<#
.SYNOPSIS
	Phase 1 breadth arm: does registry cost scale with the number of registered
	trees?

.DESCRIPTION
	Grows the estate through a series of checkpoints, measuring the registry at
	each one, so a single run yields the whole curve rather than one point.

	WHY A CURVE AND NOT A PAIR OF POINTS. The premise under test is that cost
	scales with K. Two points cannot distinguish a slope from a step, and the
	shape matters more than the magnitude here: the registry's own backing tree
	holds one entry per registered tree, so with DefaultMaxLeafKeys = 128 the
	first 128 trees fit in a SINGLE leaf. Up to that point there is no traversal
	to scale - a range scan reads one node - so a flat reading across K = 5..127
	is the expected result of the data structure and is NOT evidence against
	fan-in. It is evidence that the estate never left the first leaf.

	That is the trap this script exists to avoid, and it is worth stating
	plainly because the flat reading looks like a refutation. The prediction is
	a STEP at each leaf boundary, not a slope, so the checkpoints below are
	placed to straddle K = 128, 256, 512 and 1024 rather than spread evenly.
	Evenly-spaced checkpoints would have sampled the flat interior of the leaves
	and missed every transition.

	GetAllTreeIdsAsync is the member that matters on this arm. It is the only
	read on ILatticeRegistry with no [AlwaysInterleave], so it holds the
	activation's turn token for its whole duration and every other call queues
	behind it. A point read that is admitted while it runs is measuring the
	queue, not the scan. The probe therefore drives enumeration alone
	(--enumerate-pct 100) with --trees 0, which resolves no per-tree targets and
	leaves the estate otherwise idle.

	Depth is deliberately held at zero. Each checkpoint tree is registered and
	given a single first write, which is the minimum that makes it a real tree
	with real background services, and nothing more. Breadth and depth are
	separate arms precisely because a rig that grew both at once could not
	attribute a change to either.

.PARAMETER Checkpoints
	Cumulative estate sizes to measure at. Defaults straddle the leaf
	boundaries. Each is a TOTAL, not an increment; the script creates only the
	difference.

.PARAMETER Replicates
	Probe repetitions per checkpoint. Dispersion on this rig has been observed
	at a factor approaching six run to run, so a single reading per checkpoint
	cannot separate a real step from noise. Three is the floor.

.PARAMETER Fresh
	Reset the volume first. Strongly preferred: a leftover estate silently
	offsets every K on the curve, and the offset is invisible in the output
	because the script reports the K it intended to create.
#>
[CmdletBinding()]
param(
	[int[]] $Checkpoints = @(64, 120, 136, 250, 264, 500, 528, 1000, 1040, 2048),
	[ValidateRange(1, 20)]
	[int] $Replicates = 3,
	[ValidateRange(5, 600)]
	[int] $ProbeSeconds = 30,
	[ValidateRange(1, 500)]
	[int] $ProbeRate = 20,
	[switch] $Fresh,
	[string] $Tag = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $scriptRoot '_fanin-helpers.ps1')

$rig = Join-Path $scriptRoot 'rig.ps1'
$config = Get-FanInConfig
$null = Assert-FanInIsolation -Config $config

if (-not $Tag) { $Tag = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss') }
$outDir = Join-Path (Split-Path -Parent $scriptRoot) "results/breadth-$Tag"
$null = New-Item -ItemType Directory -Force -Path $outDir

Write-Host "Breadth arm -> $outDir" -ForegroundColor Cyan
Write-Host "  checkpoints: $($Checkpoints -join ', ')"
Write-Host "  replicates : $Replicates per checkpoint"

if ($Fresh) {
	Write-Host 'Resetting volume (breadth must start from a known K).' -ForegroundColor Yellow
	& $rig reset
}

& $rig up
& $rig ready

# The estate the host itself registers on boot. Every K below is reported as a
# TOTAL including these, because the registry cannot tell them apart and neither
# should the curve.
$baseline = 0
$listPath = Join-Path $outDir 'baseline-list.json'
& $rig driver -DriverArgs "list --out /reports/breadth-$Tag/baseline-list.json"
if (Test-Path $listPath) {
	$baseline = @((Get-Content -Raw $listPath | ConvertFrom-Json).Trees).Count
}
Write-Host "  host baseline: $baseline trees" -ForegroundColor DarkGray

$rows = [System.Collections.Generic.List[object]]::new()
$created = 0

foreach ($target in ($Checkpoints | Sort-Object)) {
	$want = $target - $baseline - $created
	if ($want -lt 0) {
		Write-Warning "Checkpoint $target is already below the live estate; skipping."
		continue
	}

	if ($want -gt 0) {
		Write-Host "Growing estate by $want to reach K=$target ..." -ForegroundColor Cyan
		# A distinct prefix per checkpoint is not cosmetic. RegisterAsync is
		# idempotent, so re-registering an existing id short-circuits without
		# touching the write path and returns in single-digit milliseconds. A
		# shared prefix would therefore quietly measure the short-circuit
		# instead of the real path, and would report a healthy number for work
		# that never happened.
		$prefix = "bk$($target)_"
		& $rig driver -DriverArgs "create --trees $want --prefix $prefix --parallelism 64 --leaves-per-tree 1 --keys-per-leaf 1 --out /reports/breadth-$Tag/create-k$target.json"
		$created += $want
	}

	$hostLoadStart = Get-FanInHostLoad -Config $config

	for ($r = 1; $r -le $Replicates; $r++) {
		$name = "k$target-r$r"
		Write-Host "  probe $name" -ForegroundColor DarkCyan

		# --trees 0 resolves no per-tree targets, so the probe cannot generate
		# point-read traffic that would queue behind the scan and contaminate
		# the very measurement being taken.
		& $rig driver -DriverArgs "probe --trees 0 --enumerate-pct 100 --rate $ProbeRate --duration $ProbeSeconds --out /reports/breadth-$Tag/probe-$name.json"

		$probePath = Join-Path $outDir "probe-$name.json"
		if (-not (Test-Path $probePath)) {
			Write-Warning "No probe output for $name."
			continue
		}

		$probe = Get-Content -Raw $probePath | ConvertFrom-Json
		$enum = @($probe.Operations | Where-Object { $_.Operation -match 'GetAllTreeIds' }) | Select-Object -First 1
		if (-not $enum) {
			Write-Warning "No enumeration arm in $name; is --enumerate-pct reaching the driver?"
			continue
		}

		$rows.Add([pscustomobject]@{
				K               = $target
				Replicate       = $r
				Calls           = $enum.Count
				P50Ms           = [math]::Round($enum.P50Ms, 3)
				P95Ms           = [math]::Round($enum.P95Ms, 3)
				P99Ms           = [math]::Round($enum.P99Ms, 3)
				MaxMs           = [math]::Round($enum.MaxMs, 3)
				Deadlines       = $enum.DeadlineExceeded
				PeakInFlight    = $probe.PeakInFlight
				HostCoresAtStart = $hostLoadStart.TotalCores
			})
	}
}

$csv = Join-Path $outDir 'breadth.csv'
$rows | Export-Csv -NoTypeInformation -Path $csv
$rows | ConvertTo-Json -Depth 5 | Set-Content -Path (Join-Path $outDir 'breadth.json')

Write-Host ''
Write-Host 'GetAllTreeIdsAsync vs K (median of replicates)' -ForegroundColor Green
$rows |
	Group-Object K |
	Sort-Object { [int] $_.Name } |
	ForEach-Object {
		$p50 = @($_.Group.P50Ms | Sort-Object)
		$med = $p50[[int][math]::Floor($p50.Count / 2)]
		[pscustomobject]@{
			K           = [int] $_.Name
			N           = $_.Count
			MedianP50Ms = $med
			MinP50Ms    = $p50[0]
			MaxP50Ms    = $p50[-1]
			WorstMaxMs  = ($_.Group.MaxMs | Measure-Object -Maximum).Maximum
			Deadlines   = ($_.Group.Deadlines | Measure-Object -Sum).Sum
		}
	} |
	Format-Table -AutoSize

Write-Host "Rows: $csv" -ForegroundColor Green
