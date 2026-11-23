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
	[string] $WorkloadMode = 'get-point',
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
	$segments = [ordered]@{}
	foreach ($idx in @($rungByIndex.Keys)) {
		$rung = $rungByIndex[$idx]
		$n = [int]$idx
		$sb = [System.Text.StringBuilder]::new()
		foreach ($m in [regex]::Matches($text, "\[producer\] preseed treeId=\S+ entries=$rung [^\r\n]*method=[^\r\n]*")) { [void]$sb.AppendLine($m.Value) }
		foreach ($set in @($finals, $steady, $dones)) {
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
	param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][int] $Silos, [Parameter(Mandatory)][long] $Size)
	$cohort = Get-StateValue -Container $State.cohorts -Key "ladder-n$Silos"
	return Get-StateValue -Container (Get-StateValue -Container $cohort -Key 'rungs') -Key (Format-SizeTag $Size)
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

function Get-TreeSizeKnee {
	<#
	.SYNOPSIS
		The first ladder size whose one-silo P95 exceeds the budget, and the
		largest size before it that stayed inside. Either may be $null.
	#>
	param([Parameter(Mandatory)] $State)
	$within = $null; $first = $null
	foreach ($size in (Get-TreeSizeLadderFor -State $State -Silos 1)) {
		$c = Get-TreeSizeRung -State $State -Silos 1 -Size $size
		if (-not (Test-TreeSizeMeasureUsable $c)) { continue }
		if ([double]$c.p95Ms -le [double]$State.degradeP95Ms) {
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
	return $m
}

function Render-TreeSizeLadder {
	param([Parameter(Mandatory)] $State)
	$nl = "`r`n"
	$meta = New-TreeSizeBaseMeta -State $State
	$sizes = Get-TreeSizeLadderFor -State $State -Silos 1
	$meta['sizes'] = ($sizes | ForEach-Object { Format-SizeTag $_ }) -join ','
	$meta['degradeP95Ms'] = "$($State.degradeP95Ms)"
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
		if (Test-TreeSizeMeasureUsable $rung) {
			$n++
			$ok = if ([double]$rung.p95Ms -le [double]$State.degradeP95Ms) { 'yes' } else { 'no' }
			[void]$sb.Append("| $(Format-TreeSize $size) | $segment | $seedTime | $seedRate | $(Format-Ms $rung.p50Ms) | $(Format-Ms $rung.p95Ms) | $(Format-Ms $rung.p99Ms) | $('{0:N0}' -f [long]$rung.readCount) | $ok |").Append($nl)
		} else {
			[void]$sb.Append("| $(Format-TreeSize $size) | $segment | $seedTime | $seedRate | _not measured_ | _not measured_ | _not measured_ | - | - |").Append($nl)
		}
	}
	$meta['cohortN'] = "$n"
	$knee = Get-TreeSizeKnee -State $State
	$budget = Format-Ms $State.degradeP95Ms
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
			if (Test-TreeSizeMeasureUsable $c) {
				$n++
				$row += " $(Format-Ms $c.p95Ms) ($(Format-Ms $c.p50Ms)) |"
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
		if ((Test-TreeSizeMeasureUsable $rung) -and (Test-TreeSizeSeedUsable $rung)) { $n++ }
	}
	return $n
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
		degradeP95Ms = $DegradeP95Ms; sizes = $Sizes; siloAxisSizes = $SiloAxisSizes; siloCounts = $SiloCounts
		cohorts = [ordered]@{}
	}
}
$state.measuredDate = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
Save-TreeSizeState -State $state -Path $statePath

foreach ($s in @($SiloCounts | Sort-Object)) {
	$key = "ladder-n$s"
	[long[]] $ladder = Get-TreeSizeLadderFor -State $state -Silos $s
	$existing = Get-StateValue -Container $state.cohorts -Key $key
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
		try {
			& $runCohort -NamePrefix $NamePrefix -SiloCount $s -WorkloadMode $WorkloadMode `
				-DurationSec $DurationSec -VehiclesPerSilo $VehiclesPerSilo -TickHz $TickHz `
				-CohortTag $tag -TreeSizeLadder ($ladder -join ',') | Out-Host
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
		if ($usable -gt $bestUsable) { $best = $parsed }
		$state.cohorts[$key] = $best
		Save-TreeSizeState -State $state -Path $statePath
		Write-TreeSizeDoc -State $state
		if ($usable -eq $ladder.Count) { break }
		Write-Warning "[tree-size] $key attempt $attempt measured $usable of $($ladder.Count) rungs (verdict=$($parsed.verdict) ladderComplete=$($parsed.ladderComplete))"
	}
}

Write-TreeSizeDoc -State $state
Write-Host "[tree-size] done; state at $statePath" -ForegroundColor Green
