#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
	Tree-size benchmark on the Layer 3 (Azure Container Apps) rig: how large can
	one tree grow before its P95 point-read latency degrades, how does adding
	silos move that, and how long does seeding take.
.DESCRIPTION
	Drives benchmark/azure-throughput/scripts/run-cohort-aca.ps1 -TreeSizeLadder
	against a rig provisioned by deploy-aca.ps1. Each silo count runs ONE cohort:
	a single producer execution grows a fresh tree rung by rung (bulk-append
	seeding of only the new keys) and, after each rung, measures steady-state
	point-read latency uniformly over every key seeded so far, on the same warm
	cluster. The producer brackets each rung with
	'[producer] tree-size-rung index=.. rung=.. phase=seed|measure|end' markers,
	which this script uses to split the cohort log into one segment per rung.

	Measuring on warm silos is deliberate: a bulk-loaded tree opened by freshly
	started silos replays its WAL leaf by leaf, serialised per partition, so a
	cold cluster measures recovery rather than steady-state reads.

	Three generated blocks are rendered into docs/lattice/performance-tree-size.md:

	  treesize        - the one-silo ladder (-Sizes): seed segment, seed time and
	                    read p50/p95/p99 per rung.
	  treesize-silos  - read p95 (p50) per tree size (-SiloAxisSizes) and silo
	                    count (-SiloCounts), same offered load per silo.
	  treesize-seed   - cumulative seed time to reach each -SiloAxisSizes size,
	                    per silo count.

	State is checkpointed per silo count under
	benchmark/.run/tree-size-report/<prefix>/state.json, so -Resume continues a
	sweep that stopped, and -DryRun re-renders the document from it.
#>
[CmdletBinding()]
param(
	[string] $NamePrefix,
	[int[]] $Sizes = @(1000000, 3000000, 10000000, 30000000),
	[int[]] $SiloAxisSizes = @(1000000, 10000000, 30000000),
	[int[]] $SiloCounts = @(1, 2, 4),
	[int] $DurationSec = 60,
	[int] $VehiclesPerSilo = 1000,
	[int] $TickHz = 5,
	[double] $DegradeP95Ms = 5.0,
	# The p95 budget is relative to the silo count's own baseline (the p95 of its smallest
	# usable rung): max(-DegradeP95Ms, -DegradeFactor x baseline). 0 keeps the absolute budget.
	[double] $DegradeFactor = 2.0,
	[string] $WorkloadMode = 'get-point',
	# Silo grain collection age (BENCH_GRAIN_COLLECTION_AGE_SEC); 0 keeps the Orleans
	# default. A short age speeds a large seed, but a bulk-loaded leaf has no persisted
	# checkpoint, so every reactivation replays its WAL partition and the read
	# measurement collapses. Leave it at 0 for published numbers.
	[int] $GrainCollectionAgeSec = 0,
	# Fail a rung's seed only when no chunk lands for this long.
	[int] $PreseedStallSec = 900,
	# Seed rate the execution ceiling assumes (keys/s).
	[int] $PreseedKeysPerSecFloor = 250,
	# Independent ladder cohorts per silo count. Above 1 each rung publishes the median
	# p95 with its range, and a range wider than -SpreadGatePct of the median AND wider
	# than -SpreadFloorMs in absolute terms is UNSTABLE (the floor stops a few-ms p95
	# from failing the gate on a 1 ms environment wobble).
	[int] $Repeats = 1,
	[double] $SpreadGatePct = 30,
	[double] $SpreadFloorMs = 1.5,
	[string] $DocPath,
	[switch] $Resume,
	[switch] $DryRun,
	[switch] $SkipDocUpdate
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# ---------------------------------------------------------------------------
# Pure helpers (loaded by Test-TreeSizeReport.ps1 through the AST).
# ---------------------------------------------------------------------------

function Format-TreeSize {
	param([Parameter(Mandatory)][long] $Keys)
	if ($Keys -ge 1000000 -and ($Keys % 1000000) -eq 0) { return "$($Keys / 1000000) M" }
	if ($Keys -ge 1000000) { return ('{0:0.#} M' -f ($Keys / 1000000.0)) }
	if ($Keys -ge 1000 -and ($Keys % 1000) -eq 0) { return "$($Keys / 1000) k" }
	return "$Keys"
}

function Format-SizeTag {
	param([Parameter(Mandatory)][long] $Keys)
	return ((Format-TreeSize -Keys $Keys) -replace ' ', '').ToLowerInvariant()
}

function Format-Ms {
	param($Value)
	if ($null -eq $Value) { return 'n/a' }
	$v = [double]$Value
	if ($v -ge 100) { return ('{0:N0} ms' -f $v) }
	if ($v -ge 10) { return ('{0:N1} ms' -f $v) }
	return ('{0:N2} ms' -f $v)
}

function Format-Duration {
	param($Ms)
	if ($null -eq $Ms) { return 'n/a' }
	$s = [double]$Ms / 1000.0
	if ($s -lt 120) { return ('{0:N0} s' -f $s) }
	$h = [math]::Floor($s / 3600)
	$m = [math]::Floor(($s - ($h * 3600)) / 60)
	$r = [math]::Round($s - ($h * 3600) - ($m * 60))
	if ($r -ge 60) { $m++; $r = 0 }
	if ($m -ge 60) { $h++; $m = 0 }
	if ($h -gt 0) { return ('{0} h {1:00} min' -f $h, $m) }
	return ('{0} min {1:00} s' -f $m, $r)
}

function Format-Rate {
	param($Keys, $Ms)
	if ($null -eq $Keys -or $null -eq $Ms -or [double]$Ms -le 0) { return 'n/a' }
	return ('{0:N0}' -f ([double]$Keys / ([double]$Ms / 1000.0)))
}

function ConvertFrom-GroupedNumber {
	param([string] $Text)
	return [long]($Text -replace ',', '')
}

function Read-TreeSizeSegment {
	<#
	.SYNOPSIS
		Parses one rung segment of a ladder cohort log into the figures the
		report publishes. Missing lines are $null, never zero.
	#>
	param([AllowEmptyString()][string] $Text)
	$result = [ordered]@{
		hasEnd = $false; done = $false
		seedMethod = $null; seedEntries = $null; seedElapsedMs = $null; seedAttempts = $null
		seedSegmentStart = $null; seedSegmentEntries = $null
		p50Ms = $null; p95Ms = $null; p99Ms = $null; maxMs = $null; readCount = $null
		failed = $null; unresolved = $null
		verifyVerified = $null; verifyMismatch = $null; verifyMissing = $null; verifyUnverifiable = $null
	}
	$vf = [regex]::Matches($Text, '\[producer\] read-verify-final verified=([\d,]+) mismatch=([\d,]+) missing=([\d,]+) unverifiable=([\d,]+)')
	if ($vf.Count -gt 0) {
		$g = $vf[$vf.Count - 1].Groups
		$result.verifyVerified = ConvertFrom-GroupedNumber $g[1].Value
		$result.verifyMismatch = ConvertFrom-GroupedNumber $g[2].Value
		$result.verifyMissing = ConvertFrom-GroupedNumber $g[3].Value
		$result.verifyUnverifiable = ConvertFrom-GroupedNumber $g[4].Value
	}
	$result.hasEnd = [regex]::IsMatch($Text, 'tree-size-rung index=\d+ rung=\d+ phase=end')
	$result.done = [regex]::IsMatch($Text, '\[producer\] DONE ')

	$seed = [regex]::Matches($Text, '\[producer\] preseed treeId=\S+ entries=(\d+)[^\r\n]*')
	if ($seed.Count -gt 0) {
		$line = $seed[$seed.Count - 1].Value
		$result.seedEntries = [long]$seed[$seed.Count - 1].Groups[1].Value
		foreach ($pair in @(
				@('method', 'seedMethod', 's'), @('elapsedMs', 'seedElapsedMs', 'l'), @('attempts', 'seedAttempts', 'l'),
				@('segmentStart', 'seedSegmentStart', 'l'), @('segmentEntries', 'seedSegmentEntries', 'l'))) {
			$f = [regex]::Match($line, "(?:^|\s)$($pair[0])=(\S+)")
			if ($f.Success) {
				$result[$pair[1]] = if ($pair[2] -eq 'l') { [long]$f.Groups[1].Value } else { $f.Groups[1].Value }
			}
		}
	}

	# Prefer the per-key steady window; fall back to any steady window.
	$lat = [regex]::Matches($Text, '\[producer\] read-latency-final window=steady unit=(\w+) count=([\d,]+) p50Ms=([\d.]+) p95Ms=([\d.]+) p99Ms=([\d.]+) maxMs=([\d.]+)')
	$pick = $null
	foreach ($l in $lat) { if ($l.Groups[1].Value -eq 'key') { $pick = $l } }
	if ($null -eq $pick -and $lat.Count -gt 0) { $pick = $lat[$lat.Count - 1] }
	if ($null -ne $pick) {
		$inv = [System.Globalization.CultureInfo]::InvariantCulture
		$result.readCount = ConvertFrom-GroupedNumber $pick.Groups[2].Value
		$result.p50Ms = [double]::Parse($pick.Groups[3].Value, $inv)
		$result.p95Ms = [double]::Parse($pick.Groups[4].Value, $inv)
		$result.p99Ms = [double]::Parse($pick.Groups[5].Value, $inv)
		$result.maxMs = [double]::Parse($pick.Groups[6].Value, $inv)
	}
	$fin = [regex]::Matches($Text, '\[silo\] FINAL ops=[\d,]+ failed=([\d,]+)[^\r\n]*')
	if ($fin.Count -gt 0) {
		$last = $fin[$fin.Count - 1]
		$result.failed = ConvertFrom-GroupedNumber $last.Groups[1].Value
		$u = [regex]::Match($last.Value, 'unresolved=([\d,]+)')
		if ($u.Success) { $result.unresolved = ConvertFrom-GroupedNumber $u.Groups[1].Value }
	}
	return $result
}

function Read-TreeSizeLadderLog {
	<#
	.SYNOPSIS
		Splits one ladder cohort log on its tree-size-rung markers and parses
		every rung segment. Returns the cohort verdict, whether the producer
		reported the whole ladder complete, and the segments keyed by rung size.
	#>
	param([Parameter(Mandatory)][string] $Path)
	$text = if (Test-Path $Path) { [System.IO.File]::ReadAllText($Path) } else { '' }
	$verdict = $null
	$v = [regex]::Matches($text, 'Verdict\s*:\s*(\w+)')
	if ($v.Count -gt 0) { $verdict = $v[$v.Count - 1].Groups[1].Value }

	# Log Analytics does not preserve line order within one second, so a rung's
	# closing lines (FINAL, read-latency-final, DONE) can land after its
	# phase=end marker. Rungs are separated by minutes of seeding, so the i-th
	# occurrence of each closing line belongs to rung index i; each rung's
	# segment is assembled from those occurrences rather than cut by markers.
	$rungByIndex = [ordered]@{}
	$endIndexes = @{}
	foreach ($m in [regex]::Matches($text, '\[producer\] tree-size-rung index=(\d+) rung=(\d+) phase=(\w+)[^\r\n]*')) {
		$idx = $m.Groups[1].Value
		if (-not $rungByIndex.Contains($idx)) { $rungByIndex[$idx] = $m.Groups[2].Value }
		if ($m.Groups[3].Value -eq 'end') { $endIndexes[$idx] = $m.Value }
	}
	$finals = [regex]::Matches($text, '\[silo\] FINAL ops=[^\r\n]*')
	$steady = [regex]::Matches($text, '\[producer\] read-latency-final window=steady [^\r\n]*')
	$dones = [regex]::Matches($text, '\[producer\] DONE [^\r\n]*')
	$verifies = [regex]::Matches($text, '\[producer\] read-verify-final [^\r\n]*')
	$segments = [ordered]@{}
	foreach ($idx in @($rungByIndex.Keys)) {
		$rung = $rungByIndex[$idx]
		$n = [int]$idx
		$sb = [System.Text.StringBuilder]::new()
		foreach ($m in [regex]::Matches($text, "\[producer\] preseed treeId=\S+ entries=$rung [^\r\n]*method=[^\r\n]*")) { [void]$sb.AppendLine($m.Value) }
		foreach ($set in @($finals, $steady, $dones, $verifies)) {
			if ($set.Count -gt $n) { [void]$sb.AppendLine($set[$n].Value) }
		}
		if ($endIndexes.ContainsKey($idx)) { [void]$sb.AppendLine($endIndexes[$idx]) }
		$segments[$rung] = $sb.ToString()
	}

	$rungs = [ordered]@{}
	foreach ($k in $segments.Keys) { $rungs[(Format-SizeTag ([long]$k))] = Read-TreeSizeSegment -Text $segments[$k] }
	return [ordered]@{
		verdict = $verdict
		ladderComplete = [regex]::IsMatch($text, '\[producer\] tree-size-ladder complete ')
		rungs = $rungs
	}
}

function Test-TreeSizeMeasureUsable {
	param($Rung)
	return ($null -ne $Rung) -and [bool]$Rung.hasEnd -and ($null -ne $Rung.p95Ms) -and ([long]$Rung.readCount -gt 0) -and
		(($null -eq $Rung.unresolved) -or ([long]$Rung.unresolved -eq 0))
}

function Test-TreeSizeRungConsistent {
	<#
	.SYNOPSIS
		False when the read verifier saw a value that differs from what the seed
		wrote, or a seeded key that read back absent. A rung with no verifier
		line (verification disabled, or an older log) is not judged inconsistent.
	#>
	param($Rung)
	if ($null -eq $Rung) { return $true }
	$mismatch = Get-StateValue -Container $Rung -Key 'verifyMismatch'
	$missing = Get-StateValue -Container $Rung -Key 'verifyMissing'
	return -not ((($null -ne $mismatch) -and ([long]$mismatch -gt 0)) -or (($null -ne $missing) -and ([long]$missing -gt 0)))
}

function Get-TreeSizeMedian {
	param([double[]] $Values)
	$s = @($Values | Sort-Object)
	if ($s.Count -eq 0) { return $null }
	$mid = [math]::Floor($s.Count / 2)
	if ($s.Count % 2 -eq 1) { return $s[$mid] }
	return ($s[$mid - 1] + $s[$mid]) / 2.0
}

function Get-TreeSizeCohortKey {
	param([Parameter(Mandatory)][int] $Silos, [int] $Repeat = 1)
	if ($Repeat -le 1) { return "ladder-n$Silos" }
	return "ladder-n$Silos-r$Repeat"
}

function Merge-TreeSizeRepeats {
	<#
	.SYNOPSIS
		Folds one rung's repeat measurements into the rung the report publishes.
		Any inconsistent repeat wins outright (a correctness failure in any run
		is the result). Otherwise the usable repeats are reduced to their median
		p50/p95/p99 and seed time, with the p95 range and its spread
		((max - min) / median) against the gate. The rung is unstable only when the
		spread exceeds the gate AND the absolute range exceeds the floor. A single
		repeat is returned as is.
	#>
	param([object[]] $Samples, [double] $SpreadGatePct = 30, [double] $SpreadFloorMs = 1.5)
	$present = @($Samples | Where-Object { $null -ne $_ })
	if ($present.Count -eq 0) { return $null }
	foreach ($r in $present) { if (-not (Test-TreeSizeRungConsistent $r)) { return $r } }
	$usable = @($present | Where-Object { (Test-TreeSizeMeasureUsable $_) -and (Test-TreeSizeSeedUsable $_) })
	if ($usable.Count -le 1) {
		if ($usable.Count -eq 1) { return $usable[0] }
		return $present[0]
	}
	$p95s = @($usable | ForEach-Object { [double]$_.p95Ms })
	$median = Get-TreeSizeMedian $p95s
	$min = ($p95s | Measure-Object -Minimum).Minimum
	$max = ($p95s | Measure-Object -Maximum).Maximum
	$spread = if ($median -gt 0) { 100.0 * ($max - $min) / $median } else { 0.0 }
	$first = $usable[0]
	$merged = [ordered]@{}
	foreach ($k in @(Get-TreeSizeKeys $first)) { $merged[$k] = Get-StateValue -Container $first -Key $k }
	$merged['p50Ms'] = Get-TreeSizeMedian @($usable | ForEach-Object { [double]$_.p50Ms })
	$merged['p95Ms'] = $median
	$merged['p99Ms'] = Get-TreeSizeMedian @($usable | ForEach-Object { [double]$_.p99Ms })
	$merged['seedElapsedMs'] = Get-TreeSizeMedian @($usable | ForEach-Object { [double]$_.seedElapsedMs })
	$merged['readCount'] = [long](Get-TreeSizeMedian @($usable | ForEach-Object { [double]$_.readCount }))
	$merged['repeats'] = $usable.Count
	$merged['p95MinMs'] = $min
	$merged['p95MaxMs'] = $max
	$merged['p95SpreadPct'] = $spread
	$merged['p95RangeMs'] = $max - $min
	$merged['unstable'] = ($spread -gt $SpreadGatePct) -and (($max - $min) -gt $SpreadFloorMs)
	return $merged
}

function Get-TreeSizeKeys {
	param($Container)
	if ($Container -is [System.Collections.IDictionary]) { return @($Container.Keys) }
	return @($Container.PSObject.Properties | ForEach-Object { $_.Name })
}

function Get-TreeSizeVerdict {
	<# The 'Within p95 budget' cell: INCONSISTENT, UNSTABLE, yes or no. #>
	param($Rung, [double] $BudgetMs)
	if (-not (Test-TreeSizeRungConsistent $Rung)) { return 'INCONSISTENT' }
	if ([bool](Get-StateValue -Container $Rung -Key 'unstable')) { return 'UNSTABLE' }
	if ([double]$Rung.p95Ms -le $BudgetMs) { return 'yes' }
	return 'no'
}

function Format-TreeSizeP95 {
	<# Read p95, with the repeat range when more than one repeat was merged. #>
	param($Rung)
	$text = Format-Ms $Rung.p95Ms
	$n = Get-StateValue -Container $Rung -Key 'repeats'
	if ($null -ne $n -and [int]$n -gt 1) {
		$text += " ($(Format-Ms $Rung.p95MinMs) - $(Format-Ms $Rung.p95MaxMs), n=$n)"
	}
	return $text
}

function Test-TreeSizeSeedUsable {
	param($Rung)
	return ($null -ne $Rung) -and ($Rung.seedMethod -eq 'bulk-append') -and ($null -ne $Rung.seedElapsedMs) -and ($null -ne $Rung.seedSegmentEntries)
}

function Get-StateValue {
	param($Container, [Parameter(Mandatory)][string] $Key)
	if ($null -eq $Container) { return $null }
	if ($Container -is [System.Collections.IDictionary]) {
		if ($Container.Contains($Key)) { return $Container[$Key] }
		return $null
	}
	$p = $Container.PSObject.Properties[$Key]
	if ($null -eq $p) { return $null }
	return $p.Value
}

function Get-TreeSizeRung {
	<# One silo count's rung, merged over every recorded repeat (see Merge-TreeSizeRepeats). #>
	param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][int] $Silos, [Parameter(Mandatory)][long] $Size)
	$repeats = Get-StateValue -Container $State -Key 'repeats'
	if ($null -eq $repeats -or [int]$repeats -lt 1) { $repeats = 1 }
	$gate = Get-StateValue -Container $State -Key 'spreadGatePct'
	if ($null -eq $gate) { $gate = 30 }
	$floor = Get-StateValue -Container $State -Key 'spreadFloorMs'
	if ($null -eq $floor) { $floor = 0.0 }
	$samples = @()
	for ($k = 1; $k -le [int]$repeats; $k++) {
		$cohort = Get-StateValue -Container $State.cohorts -Key (Get-TreeSizeCohortKey -Silos $Silos -Repeat $k)
		$samples += , (Get-StateValue -Container (Get-StateValue -Container $cohort -Key 'rungs') -Key (Format-SizeTag $Size))
	}
	return Merge-TreeSizeRepeats -Samples $samples -SpreadGatePct ([double]$gate) -SpreadFloorMs ([double]$floor)
}

function Get-TreeSizeLadderFor {
	<# The rungs one silo count's cohort runs: every -Sizes rung on one silo, the silo-axis rungs otherwise. #>
	param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][int] $Silos)
	if ($Silos -eq 1) { return @($State.sizes | ForEach-Object { [long]$_ } | Sort-Object) }
	return @($State.siloAxisSizes | ForEach-Object { [long]$_ } | Sort-Object)
}

function Get-TreeSizeCumulativeSeedMs {
	<#
	.SYNOPSIS
		Wall-clock seed time to grow one silo count's tree from empty to Size:
		the sum of every rung segment up to it. $null when any is missing.
	#>
	param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][int] $Silos, [Parameter(Mandatory)][long] $Size)
	$total = 0.0
	foreach ($r in (Get-TreeSizeLadderFor -State $State -Silos $Silos)) {
		if ($r -gt $Size) { break }
		$rung = Get-TreeSizeRung -State $State -Silos $Silos -Size $r
		if (-not (Test-TreeSizeSeedUsable $rung)) { return $null }
		$total += [double]$rung.seedElapsedMs
	}
	return $total
}

function Get-TreeSizeBudgetMs {
	<#
	.SYNOPSIS
		The p95 budget for one silo count: max(degradeP95Ms, degradeFactor x baseline),
		where baseline is the p95 of the smallest consistent, stable, usable rung of
		that silo count. Falls back to degradeP95Ms when there is no baseline or no factor.
	#>
	param([Parameter(Mandatory)] $State, [int] $Silos = 1)
	$abs = [double]$State.degradeP95Ms
	$factor = Get-StateValue -Container $State -Key 'degradeFactor'
	if ($null -eq $factor -or [double]$factor -le 0) { return $abs }
	foreach ($size in (Get-TreeSizeLadderFor -State $State -Silos $Silos)) {
		$c = Get-TreeSizeRung -State $State -Silos $Silos -Size $size
		if (-not (Test-TreeSizeMeasureUsable $c) -or -not (Test-TreeSizeRungConsistent $c)) { continue }
		if ([bool](Get-StateValue -Container $c -Key 'unstable')) { continue }
		return [Math]::Max($abs, [double]$factor * [double]$c.p95Ms)
	}
	return $abs
}

function Get-TreeSizeKnee {
	<#
	.SYNOPSIS
		The first ladder size whose one-silo P95 exceeds the budget, and the
		largest size before it that stayed inside. Either may be $null.
	#>
	param([Parameter(Mandatory)] $State)
	$within = $null; $first = $null
	$budget = Get-TreeSizeBudgetMs -State $State -Silos 1
	foreach ($size in (Get-TreeSizeLadderFor -State $State -Silos 1)) {
		$c = Get-TreeSizeRung -State $State -Silos 1 -Size $size
		if (-not (Test-TreeSizeMeasureUsable $c) -or -not (Test-TreeSizeRungConsistent $c)) { continue }
		if ([double]$c.p95Ms -le $budget) {
			if ($null -eq $first) { $within = [long]$size }
		} elseif ($null -eq $first) {
			$first = [long]$size
		}
	}
	return @{ Within = $within; Exceeded = $first }
}

function Render-TreeSizeMeta {
	param([Parameter(Mandatory)][string] $Layer, [Parameter(Mandatory)][System.Collections.IDictionary] $Meta)
	$nl = "`r`n"
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append("<!-- perf-table:${Layer}:start").Append($nl)
	[void]$sb.Append("  schema=$($Meta['schema'])").Append($nl)
	foreach ($k in @($Meta.Keys | Where-Object { $_ -ne 'schema' -and $_ -ne 'methodology' } | Sort-Object)) {
		[void]$sb.Append("  $k=$($Meta[$k])").Append($nl)
	}
	[void]$sb.Append("  methodology=$($Meta['methodology'])").Append($nl)
	[void]$sb.Append('  DO-NOT-HAND-EDIT-BETWEEN-MARKERS').Append($nl)
	[void]$sb.Append('-->')
	return $sb.ToString()
}

function New-TreeSizeBaseMeta {
	param([Parameter(Mandatory)] $State)
	$m = [ordered]@{}
	$m['schema'] = 'v1'
	$m['host'] = 'Azure Container Apps (Consumption)'
	$m['siloSize'] = '4 vCPU / 8 GiB'
	$m['region'] = $State.region
	$m['dotnet'] = $State.dotnetVersion
	$m['gitSha'] = $State.gitSha
	$m['rowsMeasured'] = $State.measuredDate
	$m['workload'] = $State.workloadMode
	$m['shardCount'] = '64'
	$m['walPartitions'] = '16'
	$m['vehiclesPerSilo'] = "$($State.vehiclesPerSilo)"
	$m['tickHz'] = "$($State.tickHz)"
	$m['durationSec'] = "$($State.durationSec)"
	$m['payloadBytes'] = '245'
	if ($State.PSObject.Properties['grainCollectionAgeSec'] -and $State.grainCollectionAgeSec) {
		$m['grainCollectionAgeSec'] = "$($State.grainCollectionAgeSec)"
	}
	return $m
}

function Render-TreeSizeLadder {
	param([Parameter(Mandatory)] $State)
	$nl = "`r`n"
	$meta = New-TreeSizeBaseMeta -State $State
	$sizes = Get-TreeSizeLadderFor -State $State -Silos 1
	$meta['sizes'] = ($sizes | ForEach-Object { Format-SizeTag $_ }) -join ','
	$meta['degradeP95Ms'] = "$($State.degradeP95Ms)"
	$budgetMs = Get-TreeSizeBudgetMs -State $State -Silos 1
	$factor = Get-StateValue -Container $State -Key 'degradeFactor'
	if ($null -ne $factor -and [double]$factor -gt 0) { $meta['degradeFactor'] = "$factor"; $meta['budgetP95Ms'] = '{0:0.###}' -f $budgetMs }
	$meta['methodology'] = 'one producer execution grows a fresh tree on one silo rung by rung with bulk-append seeding of only the new keys, then reads uniformly over every seeded key on the same warm cluster; read latency is the producer-side per-key GetAsync latency over t>=15s of each rung, quantiles are histogram bucket upper edges'
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append('| Tree size | Seed segment | Seed time | Seed rate (keys/s) | Read p50 | Read p95 | Read p99 | Reads measured | Within p95 budget |').Append($nl)
	[void]$sb.Append('|----------:|-------------:|----------:|-------------------:|---------:|---------:|---------:|---------------:|:-----------------:|').Append($nl)
	$n = 0
	foreach ($size in $sizes) {
		$rung = Get-TreeSizeRung -State $State -Silos 1 -Size $size
		$segment = 'n/a'; $seedTime = 'n/a'; $seedRate = 'n/a'
		if (Test-TreeSizeSeedUsable $rung) {
			$segment = Format-TreeSize ([long]$rung.seedSegmentEntries)
			$seedTime = Format-Duration $rung.seedElapsedMs
			$seedRate = Format-Rate $rung.seedSegmentEntries $rung.seedElapsedMs
		}
		if (-not (Test-TreeSizeRungConsistent $rung)) {
			[void]$sb.Append("| $(Format-TreeSize $size) | $segment | $seedTime | $seedRate | $(Format-Ms $rung.p50Ms) | $(Format-Ms $rung.p95Ms) | $(Format-Ms $rung.p99Ms) | $(if ($null -ne $rung.readCount) { '{0:N0}' -f [long]$rung.readCount } else { '-' }) | INCONSISTENT |").Append($nl)
		} elseif (Test-TreeSizeMeasureUsable $rung) {
			$n++
			$ok = Get-TreeSizeVerdict -Rung $rung -BudgetMs $budgetMs
			[void]$sb.Append("| $(Format-TreeSize $size) | $segment | $seedTime | $seedRate | $(Format-Ms $rung.p50Ms) | $(Format-TreeSizeP95 $rung) | $(Format-Ms $rung.p99Ms) | $('{0:N0}' -f [long]$rung.readCount) | $ok |").Append($nl)
		} else {
			[void]$sb.Append("| $(Format-TreeSize $size) | $segment | $seedTime | $seedRate | _not measured_ | _not measured_ | _not measured_ | - | - |").Append($nl)
		}
	}
	$meta['cohortN'] = "$n"
	$knee = Get-TreeSizeKnee -State $State
	$budget = Format-Ms $budgetMs
	$summary = if ($null -ne $knee.Exceeded -and $null -ne $knee.Within) {
		"One silo kept read P95 within $budget up to $(Format-TreeSize $knee.Within) keys and exceeded it at $(Format-TreeSize $knee.Exceeded)."
	} elseif ($null -ne $knee.Exceeded) {
		"One silo exceeded the $budget read P95 budget at the smallest size measured, $(Format-TreeSize $knee.Exceeded) keys."
	} elseif ($n -gt 0) {
		"One silo kept read P95 within $budget at every size measured, up to $(Format-TreeSize ([long]($sizes | Measure-Object -Maximum).Maximum)) keys; the knee lies beyond the ladder."
	} else { 'No ladder size has been measured yet.' }
	$block = (Render-TreeSizeMeta -Layer 'treesize' -Meta $meta) + $nl + $nl + $sb.ToString() + $nl + $summary + $nl + $nl + '<!-- perf-table:treesize:end -->'
	return @{ Block = $block; Meta = $meta; CohortN = $n }
}

function Render-TreeSizeSilos {
	param([Parameter(Mandatory)] $State)
	$nl = "`r`n"
	$meta = New-TreeSizeBaseMeta -State $State
	$counts = @($State.siloCounts | ForEach-Object { [int]$_ } | Sort-Object)
	$meta['siloCounts'] = $counts -join ','
	$meta['sizes'] = (@($State.siloAxisSizes | Sort-Object) | ForEach-Object { Format-SizeTag $_ }) -join ','
	$meta['methodology'] = 'one ladder cohort per silo count, each growing its own fresh tree and measuring on its own warm cluster at the same per-silo offered load; cells show read p95 with p50 in parentheses'
	$sb = [System.Text.StringBuilder]::new()
	$head = '| Tree size |'; $rule = '|----------:|'
	foreach ($s in $counts) { $head += " $s silo$(if ($s -ne 1) { 's' }) p95 (p50) |"; $rule += '---------------:|' }
	[void]$sb.Append($head).Append($nl).Append($rule).Append($nl)
	$n = 0
	foreach ($size in @($State.siloAxisSizes | ForEach-Object { [long]$_ } | Sort-Object)) {
		$row = "| $(Format-TreeSize $size) |"
		foreach ($s in $counts) {
			$c = Get-TreeSizeRung -State $State -Silos $s -Size $size
			if (-not (Test-TreeSizeRungConsistent $c)) {
				$row += ' INCONSISTENT |'
			} elseif (Test-TreeSizeMeasureUsable $c) {
				$n++
				$cell = "$(Format-TreeSizeP95 $c) ($(Format-Ms $c.p50Ms))"
				if ([bool](Get-StateValue -Container $c -Key 'unstable')) { $cell += ' UNSTABLE' }
				$row += " $cell |"
			} else { $row += ' _not measured_ |' }
		}
		[void]$sb.Append($row).Append($nl)
	}
	$meta['cohortN'] = "$n"
	$block = (Render-TreeSizeMeta -Layer 'treesize-silos' -Meta $meta) + $nl + $nl + $sb.ToString().TrimEnd("`r", "`n") + $nl + $nl + '<!-- perf-table:treesize-silos:end -->'
	return @{ Block = $block; Meta = $meta; CohortN = $n }
}

function Render-TreeSizeSeed {
	param([Parameter(Mandatory)] $State)
	$nl = "`r`n"
	$meta = New-TreeSizeBaseMeta -State $State
	$meta.Remove('vehiclesPerSilo'); $meta.Remove('tickHz'); $meta.Remove('durationSec')
	$counts = @($State.siloCounts | ForEach-Object { [int]$_ } | Sort-Object)
	$sizes = @($State.siloAxisSizes | ForEach-Object { [long]$_ } | Sort-Object)
	$meta['siloCounts'] = $counts -join ','
	$meta['sizes'] = ($sizes | ForEach-Object { Format-SizeTag $_ }) -join ','
	$meta['chunkSize'] = '16384'
	$meta['methodology'] = 'cumulative wall clock of the bulk-append seed segments that grew each silo count''s fresh tree from empty to the size shown (ILattice.BulkAppendChunkAsync over ordinally sorted keys from the Orleans-client producer); cells show the time with the average keys/s in parentheses'
	$sb = [System.Text.StringBuilder]::new()
	$head = '| Silos |'; $rule = '|------:|'
	foreach ($size in $sizes) { $head += " Seed to $(Format-TreeSize $size) |"; $rule += '----------------:|' }
	[void]$sb.Append($head).Append($nl).Append($rule).Append($nl)
	$n = 0
	foreach ($s in $counts) {
		$row = "| $s |"
		foreach ($size in $sizes) {
			$ms = Get-TreeSizeCumulativeSeedMs -State $State -Silos $s -Size $size
			if ($null -ne $ms) {
				$n++
				$row += " $(Format-Duration $ms) ($(Format-Rate $size $ms)) |"
			} else { $row += ' _not measured_ |' }
		}
		[void]$sb.Append($row).Append($nl)
	}
	$meta['cohortN'] = "$n"
	$block = (Render-TreeSizeMeta -Layer 'treesize-seed' -Meta $meta) + $nl + $nl + $sb.ToString().TrimEnd("`r", "`n") + $nl + $nl + '<!-- perf-table:treesize-seed:end -->'
	return @{ Block = $block; Meta = $meta; CohortN = $n }
}

function Render-TreeSizeNote {
	param([Parameter(Mandatory)][string] $Layer, [Parameter(Mandatory)] $Meta)
	$lead = "> Measured $($Meta['rowsMeasured']) on Azure Container Apps (Consumption) in $($Meta['region']) (.NET $($Meta['dotnet'])) at git sha $($Meta['gitSha']), n=$($Meta['cohortN'])"
	switch ($Layer) {
		'treesize' { return "$lead rungs of one ladder cohort. Each rung offers $($Meta['vehiclesPerSilo']) vehicles x $($Meta['tickHz']) Hz of point reads for $($Meta['durationSec']) s on the warm silo that seeded it." }
		'treesize-silos' { return "$lead cells, silo counts $($Meta['siloCounts']), one ladder cohort per silo count. Offered load scales with the silo count; all silo counts share one Azure Storage account." }
		'treesize-seed' { return "$lead cells, silo counts $($Meta['siloCounts']); every silo count seeded its own fresh tree on one Azure Storage account." }
		default { throw "Unknown layer '$Layer'" }
	}
}

function Set-TreeSizeBlock {
	param([Parameter(Mandatory)][string] $Content, [Parameter(Mandatory)][string] $Layer, [Parameter(Mandatory)][string] $Block, [Parameter(Mandatory)][string] $Note)
	$pattern = '(?s)<!-- perf-table:' + [regex]::Escape($Layer) + ':start.*?<!-- perf-table:' + [regex]::Escape($Layer) + ':end -->'
	if (-not [regex]::IsMatch($Content, $pattern)) { throw "Marker pair for '$Layer' not found." }
	$Content = [regex]::Replace($Content, $pattern, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $Block }, 1)
	$endTag = "<!-- perf-table:${Layer}:end -->"
	$notePattern = '(?m)' + [regex]::Escape($endTag) + "\r?\n(\r?\n)?(?:> Measured [^\r\n]*\r?\n(\r?\n)?)?"
	$replacement = $endTag + "`r`n`r`n" + $Note + "`r`n`r`n"
	return [regex]::Replace($Content, $notePattern, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $replacement }, 1)
}

function Update-TreeSizeDoc {
	param([Parameter(Mandatory)][string] $Content, [Parameter(Mandatory)] $State)
	$ladder = Render-TreeSizeLadder -State $State
	$Content = Set-TreeSizeBlock -Content $Content -Layer 'treesize' -Block $ladder.Block -Note (Render-TreeSizeNote -Layer 'treesize' -Meta $ladder.Meta)
	$silos = Render-TreeSizeSilos -State $State
	$Content = Set-TreeSizeBlock -Content $Content -Layer 'treesize-silos' -Block $silos.Block -Note (Render-TreeSizeNote -Layer 'treesize-silos' -Meta $silos.Meta)
	$seed = Render-TreeSizeSeed -State $State
	$Content = Set-TreeSizeBlock -Content $Content -Layer 'treesize-seed' -Block $seed.Block -Note (Render-TreeSizeNote -Layer 'treesize-seed' -Meta $seed.Meta)
	return $Content
}

function Get-TreeSizeUsableRungCount {
	param([Parameter(Mandatory)] $Cohort, [Parameter(Mandatory)][long[]] $Ladder)
	$n = 0
	foreach ($r in $Ladder) {
		$rung = Get-StateValue -Container (Get-StateValue -Container $Cohort -Key 'rungs') -Key (Format-SizeTag $r)
		if ((Test-TreeSizeMeasureUsable $rung) -and (Test-TreeSizeSeedUsable $rung) -and (Test-TreeSizeRungConsistent $rung)) { $n++ }
	}
	return $n
}

function Test-TreeSizeCohortInconsistent {
	<#
	.SYNOPSIS
		True when any measured rung of the cohort failed the read verifier. An
		inconsistency is a finding, not a transient: a retry must never replace it.
	#>
	param($Cohort)
	if ($null -eq $Cohort) { return $false }
	$rungs = Get-StateValue -Container $Cohort -Key 'rungs'
	if ($null -eq $rungs) { return $false }
	$names = if ($rungs -is [System.Collections.IDictionary]) { @($rungs.Keys) } else { @($rungs.PSObject.Properties.Name) }
	foreach ($n in $names) {
		if (-not (Test-TreeSizeRungConsistent (Get-StateValue -Container $rungs -Key $n))) { return $true }
	}
	return $false
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not $DocPath) { $DocPath = Join-Path $repoRoot 'docs/lattice/performance-tree-size.md' }
$runBase = Join-Path $PSScriptRoot '.run/tree-size-report'

function Save-TreeSizeState {
	param($State, [string] $Path)
	$dir = Split-Path $Path -Parent
	if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
	$State | ConvertTo-Json -Depth 10 | Set-Content -Path $Path -Encoding utf8
}

function Write-TreeSizeDoc {
	param($State)
	if ($SkipDocUpdate) { return }
	$content = [System.IO.File]::ReadAllText($DocPath)
	$updated = Update-TreeSizeDoc -Content $content -State $State
	[System.IO.File]::WriteAllText($DocPath, $updated)
	Write-Host "[tree-size] rendered $DocPath" -ForegroundColor Green
}

if ($DryRun) {
	$statePath = if ($NamePrefix) { Join-Path $runBase "$NamePrefix/state.json" } else {
		Get-ChildItem $runBase -Recurse -Filter state.json -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty FullName
	}
	if (-not $statePath -or -not (Test-Path $statePath)) { throw 'No tree-size state.json to re-render.' }
	$state = Get-Content $statePath -Raw | ConvertFrom-Json -AsHashtable
	Write-TreeSizeDoc -State $state
	return
}

if (-not $NamePrefix) { throw '-NamePrefix is required (the rig deploy-aca.ps1 provisioned).' }
if ($SiloCounts -notcontains 1) { throw '-SiloCounts must include 1: the ladder runs on one silo.' }
foreach ($s in $SiloAxisSizes) { if ($Sizes -notcontains $s) { throw "-SiloAxisSizes value $s is not one of -Sizes." } }
$Sizes = @($Sizes | Sort-Object -Unique)
$SiloAxisSizes = @($SiloAxisSizes | Sort-Object -Unique)

$scriptsDir = Join-Path $PSScriptRoot 'azure-throughput/scripts'
. (Join-Path $scriptsDir 'aca-common.ps1')
$runCohort = Join-Path $scriptsDir 'run-cohort-aca.ps1'
$ctx = Read-AcaContext -NamePrefix $NamePrefix
$statePath = Join-Path $runBase "$NamePrefix/state.json"

$state = $null
if ($Resume -and (Test-Path $statePath)) {
	$state = Get-Content $statePath -Raw | ConvertFrom-Json -AsHashtable
	Write-Host "[tree-size] resuming from $statePath" -ForegroundColor Cyan
}
if ($null -eq $state) {
	$state = [ordered]@{
		namePrefix = $NamePrefix; region = $ctx.location; dotnetVersion = '10.0'
		gitSha = (& git -C $repoRoot rev-parse --short HEAD).Trim()
		measuredDate = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
		workloadMode = $WorkloadMode; vehiclesPerSilo = $VehiclesPerSilo; tickHz = $TickHz; durationSec = $DurationSec
		grainCollectionAgeSec = $GrainCollectionAgeSec; degradeP95Ms = $DegradeP95Ms; sizes = $Sizes; siloAxisSizes = $SiloAxisSizes; siloCounts = $SiloCounts
		cohorts = [ordered]@{}
	}
}
$state.measuredDate = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
$state['repeats'] = $Repeats
$state['spreadGatePct'] = $SpreadGatePct
$state['spreadFloorMs'] = $SpreadFloorMs
$state['degradeFactor'] = $DegradeFactor
Save-TreeSizeState -State $state -Path $statePath

# Largest silo count first: it seeds fastest, so it shows earliest how far the sweep can reach.
foreach ($s in @($SiloCounts | Sort-Object -Descending)) {
for ($repeat = 1; $repeat -le $Repeats; $repeat++) {
	$key = Get-TreeSizeCohortKey -Silos $s -Repeat $repeat
	[long[]] $ladder = Get-TreeSizeLadderFor -State $state -Silos $s
	$existing = Get-StateValue -Container $state.cohorts -Key $key
	if (Test-TreeSizeCohortInconsistent $existing) {
		Write-Warning "[tree-size] $key is INCONSISTENT (read verifier); keeping it and not re-running"
		continue
	}
	if ($null -ne $existing -and (Get-TreeSizeUsableRungCount -Cohort $existing -Ladder $ladder) -eq $ladder.Count) {
		Write-Host "[tree-size] $key already measured; skipping" -ForegroundColor DarkGray
		continue
	}
	$best = $existing
	for ($attempt = 1; $attempt -le 2; $attempt++) {
		$tag = "ts-$key-a$attempt"
		$log = Join-Path (Get-AcaRunRoot) "$NamePrefix.n$s.$WorkloadMode.$tag.log"
		if (Test-Path $log) { Remove-Item $log -Force }
		Write-Host "[tree-size] $key attempt $attempt (silos=$s, ladder=$(($ladder | ForEach-Object { Format-SizeTag $_ }) -join ','))" -ForegroundColor Cyan
		$started = Get-Date
		# Latency comes from the producer's read-latency-final line, so the silos' [phaseA] lines are
		# unused here; over an hours-long seed they reached ~266k rows and stalled the log harvest.
		$siloEnv = @('BENCH_PHASEA_REPORT_SEC=0')
		if ($GrainCollectionAgeSec -gt 0) { $siloEnv += "BENCH_GRAIN_COLLECTION_AGE_SEC=$GrainCollectionAgeSec" }
		try {
			& $runCohort -NamePrefix $NamePrefix -SiloCount $s -WorkloadMode $WorkloadMode `
				-DurationSec $DurationSec -VehiclesPerSilo $VehiclesPerSilo -TickHz $TickHz `
				-CohortTag $tag -TreeSizeLadder ($ladder -join ',') `
				-PreseedStallSec $PreseedStallSec -PreseedKeysPerSecFloor $PreseedKeysPerSecFloor `
				-ExtraSiloEnv $siloEnv | Out-Host
		} catch {
			Write-Warning "[tree-size] $key attempt $attempt threw: $($_.Exception.Message)"
		}
		$parsed = Read-TreeSizeLadderLog -Path $log
		$parsed['logPath'] = $log
		$parsed['silos'] = $s
		$parsed['attempt'] = $attempt
		$parsed['wallClockSec'] = [int]((Get-Date) - $started).TotalSeconds
		$usable = Get-TreeSizeUsableRungCount -Cohort $parsed -Ladder $ladder
		$bestUsable = if ($null -ne $best) { Get-TreeSizeUsableRungCount -Cohort $best -Ladder $ladder } else { -1 }
		$inconsistent = Test-TreeSizeCohortInconsistent $parsed
		if ($inconsistent -or $usable -gt $bestUsable) { $best = $parsed }
		$state.cohorts[$key] = $best
		Save-TreeSizeState -State $state -Path $statePath
		Write-TreeSizeDoc -State $state
		if ($inconsistent) {
			Write-Warning "[tree-size] $key attempt $attempt is INCONSISTENT (read verifier); not retrying"
			break
		}
		if ($usable -eq $ladder.Count) { break }
		Write-Warning "[tree-size] $key attempt $attempt measured $usable of $($ladder.Count) rungs (verdict=$($parsed.verdict) ladderComplete=$($parsed.ladderComplete))"
	}
}
}

Write-TreeSizeDoc -State $state
Write-Host "[tree-size] done; state at $statePath" -ForegroundColor Green
