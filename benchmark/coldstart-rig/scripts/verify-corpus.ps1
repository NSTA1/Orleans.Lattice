#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Fingerprints the corpus a running rig box is serving, and diffs two
	fingerprints, so "no data was lost" is VERIFIED rather than assumed.

.DESCRIPTION
	An upgrade that makes cold start faster but quietly drops records has not
	improved anything. Epic #1830's acceptance criteria therefore require the
	file count, the vector count, the memory entries and a content-level spot
	check to match before and after, and a set of known queries to return
	equivalent results.

	This script produces that evidence. It talks to the box over its own MCP
	endpoint - the same surface a client uses - and records:

	  * repos          - every registered repository with its file and embedded
	                     vector counts.
	  * files          - the full paged enumeration of the structural file
	                     scope: the count, and a SHA-256 over the sorted paths,
	                     so a single missing or renamed file changes the hash.
	  * symbols        - the same for the symbol scope (count only; the symbol
	                     population is large and its count is the useful
	                     invariant).
	  * memory         - the count and sorted-key hash of every live agent
	                     memory entry. Memory is the only content on the box
	                     that no re-index could reconstruct, so it is the part
	                     where loss would be unrecoverable.

	WHAT "MATCHED" MEANS, AND WHY IT IS NOT ALWAYS EQUALITY. A volume captured
	mid-ingest RESUMES that ingest every time the box boots, so the symbol and
	vector populations climb monotonically while it runs - on both sides of the
	comparison. The invariant an upgrade must satisfy is therefore that nothing
	DISAPPEARS. Files are compared for exact equality (the file set is settled),
	symbols and memory as a superset (every key present before must still be
	present after, growth allowed), and the vector count as at-least. A missing
	key is named, not merely counted.
	  * spotChecks     - a content-level check, not a count: each named key is
	                     recalled and its stored content DIGEST recorded. A file
	                     that survived as a row but lost its content projection
	                     shows up here and nowhere else.
	  * queries        - a set of known queries, each recorded with the ordered
	                     paths it returned. Compared as SETS by default, because
	                     the epic deliberately swaps exact k-NN for a
	                     bounded-recall approximate index: identical membership
	                     is the contract, identical ordering is not.

	COMPARISON IS THE POINT. -Compare loads a previously written fingerprint and
	reports, per section, whether it matched - exiting non-zero on any
	regression, so it can gate a claim rather than decorate one.

.PARAMETER Compare
	Path to a fingerprint written by an earlier run. When supplied, the new
	fingerprint is taken and then diffed against it.

.PARAMETER RequireQueryOrder
	Compare query results as ordered lists rather than as sets. Off by default:
	an approximate index is permitted to reorder equally-relevant hits.

.EXAMPLE
	./verify-corpus.ps1 -Label before -OutputPath ./before.json

.EXAMPLE
	./verify-corpus.ps1 -Label after -Compare ./before.json
#>
[CmdletBinding()]
param(
	[string] $ParametersFile,
	[string] $RepoId,
	[string] $Label,
	[string] $OutputPath,
	[string] $Compare,
	[switch] $RequireQueryOrder
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$override = @{}
if ($PSBoundParameters.ContainsKey('RepoId')) { $override['RepoId'] = $RepoId }
$config = Get-RigConfig -ParametersFile $ParametersFile -ScriptRoot $here -Override $override
Assert-RigIsolation -Config $config | Out-Null

$baseUri = "http://localhost:$($config.HostPort)/"
$repo = "$($config.RepoId)"

# Fixed, committed workload. These are the "known queries" the acceptance
# criteria call for: the same five on both sides, chosen to span structural
# code, tests, documentation prose and captured memory, so a regression
# confined to one plane still shows up.
$knownQueries = @(
	'where is the readiness health probe wired',
	'how does the write-ahead log get trimmed',
	'shard consolidation planner adjacent pair selection',
	'approximate nearest neighbour index persistence',
	'what did earlier sessions decide about cold start'
)

<#
.SYNOPSIS
	Calls an MCP tool and returns its decoded text, throwing when the call did
	not succeed - a fingerprint built from a failed call would be a false
	negative for data loss, which is the one error this script must not make.
#>
function Invoke-RigVerifyTool {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Name,
		[hashtable] $Arguments = @{},
		[int] $TimeoutSec = 300
	)

	$result = Invoke-RigMcpTool -BaseUri $baseUri -Name $Name -Arguments $Arguments -TimeoutSec $TimeoutSec
	if (-not $result.Ok) {
		throw "MCP tool '$Name' failed: $($result.Error)"
	}
	return $result.Text
}

<#
.SYNOPSIS
	Stable SHA-256 over a set of strings, order-normalised by sorting, so the
	hash is a property of the SET and not of enumeration order.
#>
function Get-RigSetHash {
	[CmdletBinding()]
	param([string[]] $Values)

	if ($null -eq $Values -or $Values.Count -eq 0) { return 'empty' }
	$joined = (($Values | Sort-Object -CaseSensitive) -join "`n")
	$bytes = [System.Text.Encoding]::UTF8.GetBytes($joined)
	$sha = [System.Security.Cryptography.SHA256]::Create()
	try { return [System.BitConverter]::ToString($sha.ComputeHash($bytes)).Replace('-', '').ToLowerInvariant() }
	finally { $sha.Dispose() }
}

<#
.SYNOPSIS
	Walks every page of a repocontext_scan scope and returns the collected
	entry keys.
#>
function Get-RigScanKeys {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Scope,
		[string] $Topic,
		[int] $PageSize = 500,
		[int] $MaxPages = 400
	)

	# Parsed as JSON, not scraped with a regex. A continuation token is an
	# opaque base64-ish string that JSON escapes, and hand-unescaping it
	# silently truncated or repeated pages - which showed up as the SYMBOL
	# COUNT MOVING between two runs against an unchanged corpus. A count that
	# is not reproducible cannot be used to prove no data was lost, so the
	# reader has to be exact.
	$keys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
	$token = $null
	for ($page = 0; $page -lt $MaxPages; $page++) {
		$arguments = @{ repoId = $repo; scope = $Scope; pageSize = $PageSize }
		if ($Topic) { $arguments['topic'] = $Topic }
		if ($token) { $arguments['continuationToken'] = $token }

		$text = Invoke-RigVerifyTool -Name 'repocontext_scan' -Arguments $arguments
		$parsed = $null
		try { $parsed = $text | ConvertFrom-Json }
		catch { throw "repocontext_scan returned a body that is not JSON on page $page of scope '$Scope'." }

		$entries = Get-RigMember -Object $parsed -Name 'entries'
		foreach ($entry in @($entries)) {
			$key = Get-RigMember -Object $entry -Name 'key'
			if ($key) { [void] $keys.Add("$key") }
		}

		$hasMore = Get-RigMember -Object $parsed -Name 'hasMore'
		$token = Get-RigMember -Object $parsed -Name 'continuationToken'
		if ($hasMore -ne $true -or [string]::IsNullOrEmpty("$token")) { break }
	}

	$ordered = [string[]] @($keys)
	[Array]::Sort($ordered, [System.StringComparer]::Ordinal)
	return $ordered
}

Write-Host "Fingerprinting repository '$repo' on port $($config.HostPort) ..." -ForegroundColor Cyan

# --- Repositories --------------------------------------------------------
$reposText = Invoke-RigVerifyTool -Name 'repocontext_list_repos'
$repos = foreach ($match in [regex]::Matches($reposText, '"repoId"\s*:\s*"([^"]+)"\s*,\s*"lastIngested"[^,]*,\s*"fileCount"\s*:\s*(\d+)\s*,\s*"embeddedVectorCount"\s*:\s*(\d+)')) {
	[pscustomobject] @{
		repoId              = $match.Groups[1].Value
		fileCount           = [long] $match.Groups[2].Value
		embeddedVectorCount = [long] $match.Groups[3].Value
	}
}
if (@($repos).Count -eq 0) {
	# Fall back to independent field scrapes when the fields are ordered
	# differently, rather than silently reporting an empty repository set.
	$repos = @([pscustomobject] @{
			repoId              = $repo
			fileCount           = [long] ([regex]::Match($reposText, '"fileCount"\s*:\s*(\d+)').Groups[1].Value)
			embeddedVectorCount = [long] ([regex]::Match($reposText, '"embeddedVectorCount"\s*:\s*(\d+)').Groups[1].Value)
		})
}
Write-Host ("  repos: {0}" -f (($repos | ForEach-Object { "$($_.repoId) files=$($_.fileCount) vectors=$($_.embeddedVectorCount)" }) -join '; '))

# --- Structural scopes ---------------------------------------------------
$fileKeys = Get-RigScanKeys -Scope 'Files'
Write-Host ("  files scanned: {0}" -f $fileKeys.Count)
$symbolKeys = Get-RigScanKeys -Scope 'Symbols'
Write-Host ("  symbols scanned: {0}" -f $symbolKeys.Count)
$memoryKeys = Get-RigScanKeys -Scope 'Memory'
Write-Host ("  memory entries scanned: {0}" -f $memoryKeys.Count)

# --- Content-level spot checks -------------------------------------------
# A row can survive while its content projection does not, so count equality is
# not sufficient. Recall a deterministic sample - first, middle and last file by
# key order, plus the first memory entry - and record the stored digest.
$spotKeys = [System.Collections.Generic.List[string]]::new()
if ($fileKeys.Count -gt 0) {
	$sortedFiles = @($fileKeys | Sort-Object -CaseSensitive)
	$spotKeys.Add($sortedFiles[0])
	$spotKeys.Add($sortedFiles[[int] [Math]::Floor($sortedFiles.Count / 2)])
	$spotKeys.Add($sortedFiles[-1])
}
if ($memoryKeys.Count -gt 0) {
	$spotKeys.Add(@($memoryKeys | Sort-Object -CaseSensitive)[0])
}

$spotChecks = foreach ($key in $spotKeys) {
	$text = Invoke-RigVerifyTool -Name 'repocontext_recall' -Arguments @{ key = $key }
	[pscustomobject] @{
		key       = $key
		exists    = ([regex]::Match($text, '"exists"\s*:\s*(true|false)').Groups[1].Value -eq 'true')
		digest    = ([regex]::Match($text, '"digest"\s*:\s*"([^"]*)"').Groups[1].Value)
		sizeBytes = ([regex]::Match($text, '"sizeBytes"\s*:\s*"?(\d+)"?').Groups[1].Value)
		bodyHash  = Get-RigSetHash -Values @($text)
	}
}
Write-Host ("  spot checks: {0} key(s), all present: {1}" -f @($spotChecks).Count, (@($spotChecks | Where-Object { -not $_.exists }).Count -eq 0))

# --- Known queries -------------------------------------------------------
$queries = foreach ($query in $knownQueries) {
	$text = Invoke-RigVerifyTool -Name 'repocontext_search' -Arguments @{ repoId = $repo; query = $query; k = 10 }
	$paths = @([regex]::Matches($text, '"path"\s*:\s*"((?:[^"\\]|\\.)*)"') | ForEach-Object { $_.Groups[1].Value })
	[pscustomobject] @{
		query         = $query
		mode          = Get-RigRetrievalMode -Text $text
		retrievalPath = Get-RigRetrievalPath -Text $text
		hitCount      = $paths.Count
		paths         = $paths
		pathSetHash   = Get-RigSetHash -Values $paths
	}
}
foreach ($entry in $queries) {
	Write-Host ("  query '{0}' -> {1} hits ({2}/{3})" -f $entry.query, $entry.hitCount, $entry.mode, $entry.retrievalPath)
}

$fingerprint = [ordered] @{
	schemaVersion = 1
	kind          = 'coldstart-rig/corpus-fingerprint'
	label         = $(if ($Label) { $Label } else { 'fingerprint' })
	generatedUtc  = [datetime]::UtcNow.ToString('o')
	configuration = [ordered] @{
		hostPort = $config.HostPort
		mcpImage = "$($config.McpImage)"
		repoId   = $repo
	}
	repos         = @($repos)
	files         = [ordered] @{ count = $fileKeys.Count; setHash = Get-RigSetHash -Values $fileKeys; keys = @($fileKeys) }
	symbols       = [ordered] @{ count = $symbolKeys.Count; setHash = Get-RigSetHash -Values $symbolKeys; keys = @($symbolKeys) }
	memory        = [ordered] @{ count = $memoryKeys.Count; setHash = Get-RigSetHash -Values $memoryKeys; keys = @($memoryKeys) }
	spotChecks    = @($spotChecks)
	queries       = @($queries)
}

$runRoot = Get-RigRunRoot -ScriptRoot $here
if (-not $OutputPath) {
	$directory = Join-Path $runRoot 'fingerprints'
	New-Item -ItemType Directory -Force -Path $directory | Out-Null
	$stem = $(if ($Label) { $Label } else { [datetime]::UtcNow.ToString('yyyyMMddTHHmmssZ') })
	$OutputPath = Join-Path $directory "fingerprint-$stem.json"
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutputPath) | Out-Null
$fingerprint | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $OutputPath -Encoding utf8
Write-Host ''
Write-Host "Fingerprint written to $OutputPath" -ForegroundColor Green

if (-not $Compare) { exit 0 }

# --- Comparison ----------------------------------------------------------
Write-Host ''
Write-Host "Comparing against $Compare" -ForegroundColor Cyan
$baseline = Get-Content -LiteralPath $Compare -Raw | ConvertFrom-Json

$failures = [System.Collections.Generic.List[string]]::new()

<#
.SYNOPSIS
	Reports one comparison line and records a failure when it did not hold.
#>
function Assert-RigSame {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Name,
		$Before,
		$After,
		[switch] $AtLeast
	)

	$ok = if ($AtLeast) { [double] $After -ge [double] $Before } else { "$Before" -eq "$After" }
	$verdict = if ($ok) { 'MATCH  ' } else { 'MISMATCH' }
	$colour = if ($ok) { 'DarkGray' } else { 'Red' }
	Write-Host ("  {0} {1,-34} before {2}  after {3}" -f $verdict, $Name, $Before, $After) -ForegroundColor $colour
	if (-not $ok) { $failures.Add("$Name (before $Before, after $After)") }
}

<#
.SYNOPSIS
	Asserts that every key present before is still present after, allowing the
	set to have GROWN.

.DESCRIPTION
	The invariant an upgrade must satisfy is that nothing DISAPPEARS, which is
	not the same as "the count is identical". A restored volume captured
	mid-ingest resumes that ingest on every boot, so the symbol and vector
	populations climb monotonically while the box runs, on both sides of the
	comparison. Demanding equality there would report a false regression for a
	corpus that had in fact only gained records; demanding a superset reports a
	real one, and names the specific keys that went missing.
#>
function Assert-RigSubset {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Name,
		[string[]] $BeforeKeys,
		[string[]] $AfterKeys
	)

	$after = [System.Collections.Generic.HashSet[string]]::new(
		[string[]] @($AfterKeys), [System.StringComparer]::Ordinal)
	$missing = @(@($BeforeKeys) | Where-Object { -not $after.Contains("$_") })

	$ok = $missing.Count -eq 0
	$verdict = if ($ok) { 'MATCH  ' } else { 'MISMATCH' }
	$colour = if ($ok) { 'DarkGray' } else { 'Red' }
	Write-Host ("  {0} {1,-34} before {2}  after {3}  missing {4}" -f `
			$verdict, "$Name/no-key-lost", @($BeforeKeys).Count, @($AfterKeys).Count, $missing.Count) -ForegroundColor $colour
	if (-not $ok) {
		foreach ($key in ($missing | Select-Object -First 10)) { Write-Host "      lost: $key" -ForegroundColor Red }
		$failures.Add("$Name lost $($missing.Count) key(s)")
	}
}

foreach ($beforeRepo in $baseline.repos) {
	$afterRepo = $fingerprint.repos | Where-Object { $_.repoId -eq $beforeRepo.repoId } | Select-Object -First 1
	if ($null -eq $afterRepo) {
		Write-Host ("  MISMATCH repository '{0}' is GONE" -f $beforeRepo.repoId) -ForegroundColor Red
		$failures.Add("repository $($beforeRepo.repoId) missing")
		continue
	}
	Assert-RigSame -Name "repo/$($beforeRepo.repoId)/fileCount" -Before $beforeRepo.fileCount -After $afterRepo.fileCount
	# Vectors may only ever GROW across an upgrade: nothing in the epic deletes
	# an embedding, and a background reconcile may legitimately add some.
	Assert-RigSame -Name "repo/$($beforeRepo.repoId)/vectorCount" -Before $beforeRepo.embeddedVectorCount -After $afterRepo.embeddedVectorCount -AtLeast
}

Assert-RigSame -Name 'files/count' -Before $baseline.files.count -After $fingerprint.files.count
Assert-RigSame -Name 'files/setHash' -Before $baseline.files.setHash -After $fingerprint.files.setHash
Assert-RigSubset -Name 'symbols' -BeforeKeys $baseline.symbols.keys -AfterKeys $fingerprint.symbols.keys
Assert-RigSubset -Name 'memory' -BeforeKeys $baseline.memory.keys -AfterKeys $fingerprint.memory.keys

foreach ($beforeSpot in $baseline.spotChecks) {
	$afterSpot = $fingerprint.spotChecks | Where-Object { $_.key -eq $beforeSpot.key } | Select-Object -First 1
	if ($null -eq $afterSpot) {
		Write-Host ("  MISMATCH spot key '{0}' was not sampled after" -f $beforeSpot.key) -ForegroundColor Red
		$failures.Add("spot key $($beforeSpot.key) missing")
		continue
	}
	Assert-RigSame -Name "spot/exists" -Before $beforeSpot.exists -After $afterSpot.exists
	Assert-RigSame -Name "spot/digest" -Before $beforeSpot.digest -After $afterSpot.digest
}

foreach ($beforeQuery in $baseline.queries) {
	$afterQuery = $fingerprint.queries | Where-Object { $_.query -eq $beforeQuery.query } | Select-Object -First 1
	if ($null -eq $afterQuery) {
		Write-Host ("  MISMATCH query '{0}' was not run after" -f $beforeQuery.query) -ForegroundColor Red
		$failures.Add("query '$($beforeQuery.query)' missing")
		continue
	}
	$name = "query/{0}" -f ($beforeQuery.query.Substring(0, [Math]::Min(24, $beforeQuery.query.Length)))
	if ($RequireQueryOrder) {
		Assert-RigSame -Name "$name/order" -Before (@($beforeQuery.paths) -join '|') -After (@($afterQuery.paths) -join '|')
	}
	else {
		Assert-RigSame -Name "$name/set" -Before $beforeQuery.pathSetHash -After $afterQuery.pathSetHash
	}
}

Write-Host ''
if ($failures.Count -eq 0) {
	Write-Host 'Corpus comparison: NO REGRESSION. Every checked invariant held.' -ForegroundColor Green
	exit 0
}

Write-Host ("Corpus comparison: {0} REGRESSION(S)." -f $failures.Count) -ForegroundColor Red
foreach ($failure in $failures) { Write-Host "  - $failure" -ForegroundColor Red }
exit $failures.Count
