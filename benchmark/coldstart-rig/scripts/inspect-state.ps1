#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Offline census of a restored copy of the durable state. Never attaches to
	a running box.

.DESCRIPTION
	Reads durable state directly, with the stack DOWN, so a measurement is not
	perturbed by the thing being measured and a state question can be answered
	without a cold start at all. It reports:

	  * per-tree and per-shard WAL sizes;
	  * WAL data / commit / trim record counts, by walking the file-WAL
	    framing byte by byte (see FileWalRecordFormat in
	    src/lattice.storage.file);
	  * per-tree leaf counts, straight out of the persisted leaf state;
	  * leaf-snapshot rows and bytes per repository-context key prefix;
	  * per-partition projection checkpoints per tree;
	  * grain-state size by grain type.

	Every figure lands in a flat `metrics` map in the emitted JSON, so a
	downstream sub-issue can diff two censuses by key without re-deriving
	anything, and so the shipped expectations file can pin known-answer
	values by name.

	KNOWN-ANSWER VALIDATION. The rig is an instrument, so it is itself
	validated: census-expectations.json pins the figures epic #1830 quoted
	from a specific backup, and this script reports match or mismatch per
	check. Point it at that backup and every check should pass; point it at a
	different one and use -SkipExpectations.

.PARAMETER SqliteSource
	Where the grain-state database is read from. 'auto' (the default) prefers
	the master volume when it exists, because a Docker volume lives inside the
	VM's own filesystem and scans roughly an order of magnitude faster than a
	host bind mount of the same bytes; 'staging' forces the extracted host
	copy; 'volume' forces the master volume.

.PARAMETER SkipWal
	Skip the file-WAL framing walk. It is the slow half of the census (it
	reads every segment byte), so skipping it is useful when only the
	grain-state figures are wanted.

.EXAMPLE
	./inspect-state.ps1

.EXAMPLE
	./inspect-state.ps1 -SqliteSource staging -SkipExpectations
#>
[CmdletBinding()]
param(
	[string] $BackupTarball,
	[string] $StagingPath,
	[string] $ParametersFile,
	[ValidateSet('auto', 'volume', 'staging')] [string] $SqliteSource = 'auto',
	[string] $ExpectationsFile,
	[switch] $SkipExpectations,
	[switch] $SkipWal,
	[string] $OutputPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$config = Get-RigConfig -ParametersFile $ParametersFile -Override @{ BackupTarball = $BackupTarball } -ScriptRoot $here
Assert-RigIsolation -Config $config | Out-Null

$runRoot = Get-RigRunRoot -ScriptRoot $here
if (-not $StagingPath) {
	$stem = [System.IO.Path]::GetFileNameWithoutExtension("$($config.BackupTarball)")
	$StagingPath = Join-Path (Join-Path $runRoot 'state') $stem
}
if (-not (Test-Path -LiteralPath $StagingPath)) {
	throw "Staging copy not found: $StagingPath. Run prepare-master.ps1 first."
}
$StagingPath = (Resolve-Path -LiteralPath $StagingPath).Path

$useVolume = switch ($SqliteSource) {
	'volume' { $true }
	'staging' { $false }
	default { Test-RigVolumeExists -Name "$($config.MasterVolume)" }
}

Write-Host "Census source" -ForegroundColor Cyan
Write-Host "  staging  : $StagingPath"
Write-Host "  sqlite   : $(if ($useVolume) { "volume $($config.MasterVolume)" } else { 'staging directory' })"
Write-Host ''

$metrics = [ordered] @{}

# --- WAL framing walk ----------------------------------------------------
$wal = $null
if (-not $SkipWal) {
	$walRoot = Join-Path $StagingPath 'wal'
	Write-Host 'Walking the file-WAL framing ...' -ForegroundColor Cyan
	$walWatch = [System.Diagnostics.Stopwatch]::StartNew()
	$wal = Get-RigWalTreeCensus -WalRoot $walRoot
	$walWatch.Stop()
	Write-Host ("  {0} segments, {1:N0} data records, {2:N0} trim records in {3:N1}s" -f `
			$wal.Segments, $wal.TotalDataRecords, $wal.TotalTrimRecords, $walWatch.Elapsed.TotalSeconds)

	$metrics['wal.segments'] = $wal.Segments
	$metrics['wal.totalSizeBytes'] = $wal.TotalSizeBytes
	$metrics['wal.dataRecords'] = $wal.TotalDataRecords
	$metrics['wal.commitRecords'] = $wal.TotalCommitRecords
	$metrics['wal.trimRecords'] = $wal.TotalTrimRecords
	$metrics['wal.tornSegments'] = $wal.TornSegments
	foreach ($tree in $wal.Trees) {
		$metrics["wal.tree.$($tree.TreeId).sizeBytes"] = $tree.SizeBytes
		$metrics["wal.tree.$($tree.TreeId).dataRecords"] = $tree.DataRecords
		$metrics["wal.tree.$($tree.TreeId).trimRecords"] = $tree.TrimRecords
	}
}

# --- Grain-state database ------------------------------------------------
$sqliteArgs = @{ Config = $config }
if ($useVolume) { $sqliteArgs['Volume'] = "$($config.MasterVolume)" } else { $sqliteArgs['StagingPath'] = $StagingPath }

Write-Host 'Reading grain-state size by grain type ...' -ForegroundColor Cyan
$grainState = foreach ($row in (Invoke-RigSqlite @sqliteArgs -SqlName 'grain-state-by-type')) {
	$parts = ConvertFrom-RigCensusRow -Row $row -Fields 3
	[pscustomobject] @{ GrainType = $parts[0]; Rows = [long] $parts[1]; PayloadBytes = [long] $parts[2] }
}
foreach ($entry in $grainState) {
	$metrics["grainState.$($entry.GrainType).rows"] = $entry.Rows
	$metrics["grainState.$($entry.GrainType).bytes"] = $entry.PayloadBytes
}

$databaseFile = Join-Path $StagingPath 'repocontext.db'
if (Test-Path -LiteralPath $databaseFile) {
	$metrics['grainState.databaseSizeBytes'] = (Get-Item -LiteralPath $databaseFile).Length
}

Write-Host 'Reading leaf counts per tree ...' -ForegroundColor Cyan
$leafCounts = foreach ($row in (Invoke-RigSqlite @sqliteArgs -SqlName 'leaf-count-by-tree')) {
	$parts = ConvertFrom-RigCensusRow -Row $row -Fields 2
	[pscustomobject] @{ TreeId = $parts[0]; Leaves = [long] $parts[1] }
}
foreach ($entry in $leafCounts) { $metrics["leafCount.$($entry.TreeId)"] = $entry.Leaves }

Write-Host 'Reading leaf-snapshot rows and bytes per key prefix ...' -ForegroundColor Cyan
$snapshots = foreach ($row in (Invoke-RigSqlite @sqliteArgs -SqlName 'leaf-snapshot-by-prefix')) {
	$parts = ConvertFrom-RigCensusRow -Row $row -Fields 3
	[pscustomobject] @{ Prefix = $parts[0]; Snapshots = [long] $parts[1]; Bytes = [long] $parts[2] }
}
foreach ($entry in $snapshots) {
	$metrics["leafSnapshot.prefix.$($entry.Prefix).rows"] = $entry.Snapshots
	$metrics["leafSnapshot.prefix.$($entry.Prefix).bytes"] = $entry.Bytes
}

Write-Host 'Reading per-partition projection checkpoints ...' -ForegroundColor Cyan
$checkpoints = foreach ($row in (Invoke-RigSqlite @sqliteArgs -SqlName 'leaf-checkpoints-by-partition')) {
	$parts = ConvertFrom-RigCensusRow -Row $row -Fields 6
	[pscustomobject] @{
		TreeId              = $parts[0]
		Partition           = [int] $parts[1]
		Leaves              = [long] $parts[2]
		DistinctCheckpoints = [long] $parts[3]
		MinOffset           = [long] $parts[4]
		MaxOffset           = [long] $parts[5]
	}
}
foreach ($entry in $checkpoints) {
	$metrics["checkpoint.$($entry.TreeId).p$($entry.Partition).distinct"] = $entry.DistinctCheckpoints
	$metrics["checkpoint.$($entry.TreeId).p$($entry.Partition).max"] = $entry.MaxOffset
}

# --- Known-answer validation --------------------------------------------
$expectationResult = $null
if (-not $SkipExpectations) {
	if (-not $ExpectationsFile) { $ExpectationsFile = Join-Path $here '..' 'census-expectations.json' }
	if (Test-Path -LiteralPath $ExpectationsFile) {
		$ExpectationsFile = (Resolve-Path -LiteralPath $ExpectationsFile).Path
		$expectations = Get-Content -LiteralPath $ExpectationsFile -Raw | ConvertFrom-Json

		$checks = foreach ($check in $expectations.checks) {
			$actual = if ($metrics.Contains($check.metric)) { $metrics[$check.metric] } else { $null }
			$tolerance = if ($check.PSObject.Properties['tolerance']) { [double] $check.tolerance } else { 0.0 }
			$match = $false
			if ($null -ne $actual) {
				$delta = [Math]::Abs([double] $actual - [double] $check.expected)
				$allowed = if ($tolerance -gt 0) { [Math]::Abs([double] $check.expected) * $tolerance } else { 0.0 }
				$match = $delta -le $allowed
			}
			[pscustomobject] @{
				Id            = "$($check.id)"
				Metric        = "$($check.metric)"
				Expected      = $check.expected
				Actual        = $actual
				TolerancePct  = [Math]::Round($tolerance * 100.0, 3)
				Match         = $match
			}
		}
		$checks = @($checks)
		$matched = @($checks | Where-Object { $_.Match }).Count

		$expectationResult = [pscustomobject] @{
			File    = $ExpectationsFile
			Tarball = "$($expectations.tarball)"
			Matched = $matched
			Total   = $checks.Count
			Checks  = $checks
		}

		Write-Host ''
		Write-Host 'Known-answer validation against the epic census' -ForegroundColor Cyan
		foreach ($check in $checks) {
			$label = if ($check.Match) { 'MATCH ' } else { 'DIFFER' }
			$colour = if ($check.Match) { 'Green' } else { 'Red' }
			Write-Host ("  {0}  {1,-42} expected {2,14}  actual {3,14}" -f $label, $check.Id, $check.Expected, $check.Actual) -ForegroundColor $colour
		}
		Write-Host ("  {0} of {1} census checks reproduced." -f $matched, $checks.Count) `
			-ForegroundColor $(if ($matched -eq $checks.Count) { 'Green' } else { 'Red' })
	}
	else {
		Write-Host "No expectations file at $ExpectationsFile; skipping known-answer validation." -ForegroundColor Yellow
	}
}

# --- Emit ----------------------------------------------------------------
$result = [ordered] @{
	schemaVersion = 1
	kind          = 'coldstart-rig/census'
	generatedUtc  = [datetime]::UtcNow.ToString('o')
	source        = [ordered] @{
		tarball      = "$($config.BackupTarball)"
		staging      = $StagingPath
		sqliteSource = if ($useVolume) { 'volume' } else { 'staging' }
		masterVolume = "$($config.MasterVolume)"
	}
	wal                    = $wal
	grainStateByType       = @($grainState)
	leafCountsByTree       = @($leafCounts)
	leafSnapshotsByPrefix  = @($snapshots)
	checkpointsByPartition = @($checkpoints)
	expectations           = $expectationResult
	metrics                = $metrics
}

if (-not $OutputPath) {
	$censusDirectory = Join-Path $runRoot 'census'
	New-Item -ItemType Directory -Force -Path $censusDirectory | Out-Null
	$OutputPath = Join-Path $censusDirectory ("census-{0}.json" -f ([datetime]::UtcNow.ToString('yyyyMMddTHHmmssZ')))
}

$json = $result | ConvertTo-Json -Depth 8
Set-Content -LiteralPath $OutputPath -Value $json -Encoding ascii
Set-Content -LiteralPath (Join-Path (Split-Path -Parent $OutputPath) 'census-latest.json') -Value $json -Encoding ascii

Write-Host ''
Write-Host "Census written to $OutputPath" -ForegroundColor Green

if ($null -ne $expectationResult -and $expectationResult.Matched -ne $expectationResult.Total) {
	exit 1
}
