#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Synthetic scale mode: generates a corpus well beyond the current live size,
	indexes it into an isolated volume, and snapshots the result as a reusable
	scale master.

.DESCRIPTION
	The epic exists to support much larger trees, so the rig has to be able to
	measure past today's live size. The live deployment is roughly 6,800 files
	and 73,500 vectors; the defaults here generate an order of magnitude more
	embeddable content than that, and the target is a parameter so a cohort can
	be pushed further still.

	Three stages, each independently runnable:

	  1. GENERATE. Writes a deterministic synthetic source tree. The generator
	     is seeded, so the same -Files / -SymbolsPerFile / -Seed always produce
	     a byte-identical corpus and two scale cohorts are comparable. The tree
	     must live under the rig's mounted workspace root, because the box only
	     ever sees /workspace and refuses a path that escapes it.

	  2. INDEX. Brings the rig stack up on a dedicated scale working volume,
	     registers the corpus over the stateless MCP endpoint with
	     repocontext_add_repo, and polls repocontext_index_status until
	     scanning and embedding have both converged.

	  3. PROMOTE. Snapshots the indexed working volume into the SCALE MASTER
	     volume, so run-cohort.ps1 -MasterVolume <scale master> can measure
	     cold start at that size over and over from byte-identical state
	     without re-indexing.

	BE REALISTIC ABOUT COST. Embedding is the slow stage and it runs on CPU in
	the companion container: a corpus of several hundred thousand vectors takes
	hours. Generation and promotion are minutes. Run stage 2 once, promote, and
	then measure from the scale master.

.PARAMETER Files
	Number of synthetic source files to generate. Default 30000.

.PARAMETER SymbolsPerFile
	Declared types/methods per file, which is what drives the symbol-vector
	count. Default 12.

.PARAMETER Stage
	Which stages to run: 'generate', 'index', 'promote', or 'all'.

.EXAMPLE
	./generate-corpus.ps1 -Stage generate -Files 30000

.EXAMPLE
	./generate-corpus.ps1 -Stage all -Files 8000 -SymbolsPerFile 12
#>
[CmdletBinding()]
param(
	[int] $Files = 30000,
	[int] $SymbolsPerFile = 12,
	[int] $Seed = 18380,
	[string] $CorpusRepoId = 'coldstart-scale-corpus',
	[string] $TargetPath,
	[ValidateSet('generate', 'index', 'promote', 'all')] [string] $Stage = 'generate',
	[int] $IndexTimeoutMinutes = 720,
	[string] $ParametersFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$config = Get-RigConfig -ParametersFile $ParametersFile -ScriptRoot $here
Assert-RigIsolation -Config $config | Out-Null

$workspaceRoot = (Resolve-Path -LiteralPath "$($config.WorkspaceRoot)").Path
if (-not $TargetPath) { $TargetPath = Join-Path $workspaceRoot $CorpusRepoId }

# The box can only see /workspace, and refuses any registered path that
# resolves outside it, so a corpus written elsewhere could never be indexed.
$fullTarget = [System.IO.Path]::GetFullPath($TargetPath)
if (-not $fullTarget.StartsWith($workspaceRoot, [StringComparison]::OrdinalIgnoreCase)) {
	throw "Corpus path '$fullTarget' is outside the mounted workspace root '$workspaceRoot'. The box cannot see it, so it could never be indexed. Move the corpus, or point WorkspaceRoot at its parent."
}

$runStages = if ($Stage -eq 'all') { @('generate', 'index', 'promote') } else { @($Stage) }

# ---------------------------------------------------------------------------
if ($runStages -contains 'generate') {
	Write-Host "Generating $Files synthetic files ($SymbolsPerFile symbols each) under $fullTarget ..." -ForegroundColor Cyan
	if (Test-Path -LiteralPath $fullTarget) { Remove-Item -LiteralPath $fullTarget -Recurse -Force }
	New-Item -ItemType Directory -Force -Path $fullTarget | Out-Null

	# Seeded so the corpus is reproducible byte for byte: two scale cohorts
	# must differ only in the code under test, never in their input.
	$random = [System.Random]::new($Seed)
	$nouns = @('Ledger', 'Shard', 'Cursor', 'Digest', 'Envelope', 'Partition', 'Snapshot', 'Watermark', 'Manifest', 'Cohort', 'Projection', 'Replica')
	$verbs = @('Resolve', 'Compact', 'Publish', 'Reconcile', 'Materialise', 'Drain', 'Admit', 'Fold', 'Seal', 'Replay')
	$topics = @('durability', 'retrieval', 'membership', 'consolidation', 'admission', 'replay', 'embedding', 'checkpointing')

	$filesPerDirectory = 250
	$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
	$builder = [System.Text.StringBuilder]::new()

	for ($i = 0; $i -lt $Files; $i++) {
		$directory = Join-Path $fullTarget ("src/pkg{0:D4}" -f [int] [Math]::Floor($i / $filesPerDirectory))
		if (-not (Test-Path -LiteralPath $directory)) { New-Item -ItemType Directory -Force -Path $directory | Out-Null }

		$noun = $nouns[$random.Next($nouns.Length)]
		$typeName = "{0}{1}Unit{2:D6}" -f $verbs[$random.Next($verbs.Length)], $noun, $i

		[void] $builder.Clear()
		[void] $builder.AppendLine("namespace Synthetic.Scale.Pkg{0:D4};" -f [int] [Math]::Floor($i / $filesPerDirectory))
		[void] $builder.AppendLine()
		[void] $builder.AppendLine("/// <summary>")
		[void] $builder.AppendLine("/// Synthetic scale-corpus unit $i. Concerns $($topics[$random.Next($topics.Length)]) of the")
		[void] $builder.AppendLine("/// $noun plane: it exists so the vector trees can be driven well past the")
		[void] $builder.AppendLine("/// size of any real deployment and cold start measured at that scale.")
		[void] $builder.AppendLine("/// </summary>")
		[void] $builder.AppendLine("public sealed class $typeName")
		[void] $builder.AppendLine('{')
		for ($s = 0; $s -lt $SymbolsPerFile; $s++) {
			$method = "{0}{1}Async" -f $verbs[$random.Next($verbs.Length)], $nouns[$random.Next($nouns.Length)]
			[void] $builder.AppendLine("    /// <summary>Handles the $($topics[$random.Next($topics.Length)]) path for slot $s of unit $i.</summary>")
			[void] $builder.AppendLine("    public int ${method}$s(int offset)")
			[void] $builder.AppendLine('    {')
			[void] $builder.AppendLine("        // Deterministic body so the corpus hashes identically on every generation.")
			[void] $builder.AppendLine("        var seed = $($random.Next(1, 100000));")
			[void] $builder.AppendLine("        return unchecked(offset * 31 + seed + $s);")
			[void] $builder.AppendLine('    }')
			[void] $builder.AppendLine()
		}
		[void] $builder.AppendLine('}')

		$path = Join-Path $directory ("$typeName.cs")
		[System.IO.File]::WriteAllText($path, $builder.ToString(), [System.Text.UTF8Encoding]::new($false))

		if (($i + 1) % 2500 -eq 0) {
			Write-Host ("  {0,7} / {1} files ({2:N0}s)" -f ($i + 1), $Files, $stopwatch.Elapsed.TotalSeconds) -ForegroundColor DarkGray
		}
	}
	$stopwatch.Stop()

	$generated = @(Get-ChildItem -LiteralPath $fullTarget -Recurse -File)
	$totalBytes = ($generated | Measure-Object Length -Sum).Sum
	Write-Host ("Generated {0:N0} files, {1:N1} MB, in {2:N1}s. Expect roughly {3:N0} embeddable units (one per file plus one per declared symbol)." -f `
			$generated.Count, ($totalBytes / 1MB), $stopwatch.Elapsed.TotalSeconds, ($generated.Count * ($SymbolsPerFile + 1))) -ForegroundColor Green
}

# ---------------------------------------------------------------------------
if ($runStages -contains 'index') {
	Assert-RigDockerIsolation -Config $config | Out-Null

	Write-Host 'Preparing an EMPTY scale working volume ...' -ForegroundColor Cyan
	Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure | Out-Null
	Remove-RigVolume -Config $config -Name "$($config.WorkVolume)"
	New-RigVolume -Config $config -Name "$($config.WorkVolume)" | Out-Null
	New-RigVolume -Config $config -Name "$($config.HfCacheVolume)" | Out-Null

	Invoke-RigCompose -Config $config -ComposeArgs @('up', '-d') | Out-Null

	$baseUri = "http://localhost:$($config.HostPort)/"
	$readyUri = "http://localhost:$($config.HostPort)/health/ready"
	$container = Get-RigContainerName -Config $config -Service 'repocontext'
	$zero = Get-RigContainerStartedAtUtc -Container $container
	$ready = Wait-RigHttpOk -Uri $readyUri -ZeroUtc $zero -TimeoutSec $config.ReadyTimeoutSec -IntervalMs $config.ProbeIntervalMs
	if ($null -eq $ready) { throw 'The rig stack never became ready; cannot index the scale corpus.' }
	Write-Host ("Ready in {0}s. Registering the corpus ..." -f $ready) -ForegroundColor Green

	$relative = $fullTarget.Substring($workspaceRoot.Length).TrimStart('\', '/').Replace('\', '/')
	$containerPath = "/workspace/$relative"
	$add = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_add_repo' `
		-Arguments @{ path = $containerPath; repoId = $CorpusRepoId } -TimeoutSec 600
	if (-not $add.Ok) { throw "repocontext_add_repo failed for '$containerPath': $($add.Error)" }
	Write-Host "  registered $containerPath as '$CorpusRepoId'" -ForegroundColor Green

	$deadline = (Get-Date).AddMinutes($IndexTimeoutMinutes)
	$lastLine = ''
	while ((Get-Date) -lt $deadline) {
		$status = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_index_status' -Arguments @{ repoId = $CorpusRepoId } -TimeoutSec 300
		if ($status.Ok -and $status.Text) {
			$parsed = $null
			try { $parsed = $status.Text | ConvertFrom-Json } catch { $parsed = $null }
			if ($null -ne $parsed) {
				$line = "  status {0} phase {1} scanned {2} embedded {3} chunks {4}/{5}" -f `
					$parsed.status, $parsed.phase, $parsed.filesScanned, $parsed.filesEmbedded, $parsed.chunksCommitted, $parsed.chunksTotal
				if ($line -ne $lastLine) { Write-Host $line -ForegroundColor DarkGray; $lastLine = $line }
				if ("$($parsed.status)" -eq 'Failed') { throw "Indexing FAILED for '$CorpusRepoId'." }
				if ("$($parsed.status)" -eq 'Completed' -and $parsed.filesEmbedded -ge $parsed.filesScanned -and $parsed.filesScanned -gt 0) {
					Write-Host 'Indexing converged.' -ForegroundColor Green
					break
				}
			}
		}
		Start-Sleep -Seconds 15
	}
}

# ---------------------------------------------------------------------------
if ($runStages -contains 'promote') {
	$scaleMaster = "$($config.ScaleMasterVolume)"
	Write-Host "Promoting the indexed working volume to the scale master '$scaleMaster' ..." -ForegroundColor Cyan
	Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure | Out-Null
	Copy-RigVolume -Config $config -Source "$($config.WorkVolume)" -Destination $scaleMaster | Out-Null
	Write-Host ''
	Write-Host 'Scale master ready.' -ForegroundColor Green
	Write-Host "  ./run-cohort.ps1 -MasterVolume $scaleMaster -RepoId $CorpusRepoId" -ForegroundColor Cyan
}
