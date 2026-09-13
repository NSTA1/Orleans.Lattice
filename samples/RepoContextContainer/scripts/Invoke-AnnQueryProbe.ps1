<#
.SYNOPSIS
    Issues real retrieval queries against a running RepoContext container and
    reports which retrieval plane answered them, so a zero on
    repocontext_retrieval_ann_search_total{state="approximate"} can be read as a
    measurement rather than as an absence.

.DESCRIPTION
    The acceptance procedure for DoD-1b scores on the "approximate" arm of
    repocontext_retrieval_ann_search_total becoming non-zero. That counter is
    written from exactly one place - RepoContextRetrievalGuardReporter's
    RecordPlaneOutcome, on the per-query path - so it cannot move unless queries
    are actually issued against the container. Acceptance runs 9, 12 and 13
    deployed, waited and scraped without ever issuing one, so every DoD-1b zero
    recorded so far was produced by machinery that never executed.

    This script is the missing apparatus. It is deliberately NOT a scorer: it
    issues traffic, records what it issued, and reports what the instruments say
    about it. Whether the resulting reading passes DoD-1b is somebody else's
    decision and is made against a pre-registered predicate.

    WHAT MAKES A READING ADMISSIBLE

    A harness that issued zero queries reports "no approximate searches
    occurred", which is byte-identical to the failure it exists to detect. So
    this script refuses to emit any instrument reading at all unless BOTH of its
    own self-counts are non-zero:

      * Issued    - requests actually put on the wire.
      * Succeeded - requests that came back as a well-formed, non-error result.

    Those two are reported separately and never collapsed, because
    "issued 40, succeeded 0" (the box rejected everything) and "issued 0" (the
    harness never ran) are different failures with different owners.

    HOW A PLANE IS ATTRIBUTED

    Three independent signals are recorded, and they answer three different
    questions. Keeping them apart is the whole point of the script.

      1. The harness self-counts, above. Question: did anything ask?
      2. The TOTAL delta across all three "state" arms of
         repocontext_retrieval_ann_search_total. Because those three arms
         partition the same population - every query is counted on exactly one
         of bootstrapping / exhaustive / approximate - a non-zero total proves
         the vector plane was consulted. This is the liveness witness for the
         instrument itself, independent of whether the ANN path was chosen.
      3. The per-query retrievalPath field on each search response. Question:
         semantic or keyword, and when keyword, WHY.

    A TRAP THIS SCRIPT REFUSES TO FALL INTO

    The retrievalPath value "semantic.approximate" does NOT mean the approximate
    plane served the query, and must never be read as evidence for DoD-1b.
    AnnRepoContextSemanticIndex declares:

        public string RetrievalPath => RepoContextRetrievalPath.SemanticApproximate;

    unconditionally. It is a property of the INDEX, not of a query - one index
    serves every repository, so a state-tracking declaration would be wrong the
    moment two repositories were in different states. It therefore reads
    "semantic.approximate" identically when the EXACT fallback answered with
    complete recall. It under-promises recall by design.

    So retrievalPath is used here only to separate semantic from keyword, and to
    name the cause when the answer was keyword. The authority on which ANN
    serving state answered is the counter arms, and nothing else.

    The keyword causes are not interchangeable either, and one of them makes a
    DoD-1b zero uninterpretable rather than merely negative:

      * keyword.no_embedder - no embedding provider is bound, so the vector
        plane is never consulted and the counter cannot move. A DoD-1b zero
        under this cause says nothing about the ANN plane at all.
      * keyword.vector_plane_unavailable - the plane WAS consulted and could not
        answer, so the bootstrapping arm should move. A zero total alongside
        this cause is a contradiction worth reporting.
      * keyword.exact_fallback_suppressed - a stalled gather has left the exact
        fallback deliberately withheld.
      * keyword.index_degraded - the semantic index itself faulted.

.PARAMETER BaseUri
    Root of the container's MCP streamable-HTTP listener, which is also where
    the health probes and the /metrics scrape endpoint live. The compose file
    publishes container port 8080, overridable on the host with REPOCONTEXT_PORT.
    Defaults to http://localhost:8080.

.PARAMETER RepoId
    Repository id to query. When omitted the script calls repocontext_list_repos
    and probes every repository it finds, which is usually what you want: a
    corpus below the ANN training threshold answers exhaustively and is a
    perfectly healthy reason for the approximate arm to stay at zero.

.PARAMETER Queries
    Query strings to issue. Defaults to a built-in spread of natural-language
    queries. Every query is issued against every selected repository.

.PARAMETER Repetitions
    How many times to issue the whole query set. Defaults to 1. Raise it when
    you want more traffic without inventing more query text.

.PARAMETER K
    Number of hits to request per query. Defaults to 5.

.PARAMETER TimeoutSeconds
    Per-request timeout. Defaults to 60, matching the scrape timeout the deploy
    playbook already uses.

.PARAMETER JsonOutputPath
    Optional path to write the full machine-readable result document to, for
    attaching to an acceptance run record.

.EXAMPLE
    ./Invoke-AnnQueryProbe.ps1

    Probe every registered repository on http://localhost:8080 with the default
    query set.

.EXAMPLE
    ./Invoke-AnnQueryProbe.ps1 -RepoId lattice -Repetitions 4 -JsonOutputPath run14-ann.json

.NOTES
    This script does not deploy, does not score, and does not tune. It issues
    traffic and reports.

    Exit codes:
      0 - queries were issued and succeeded, and a reading was produced. This
          says NOTHING about whether DoD-1b passes; read the report.
      2 - refused to report: no query succeeded, so any instrument reading would
          be an absence produced by machinery that did not execute.
      3 - the container was not reachable, or the MCP handshake failed.

    The container image is distroless and shell-less. Everything here runs from
    the host against the published port; nothing requires a shell inside the
    container.
#>
[CmdletBinding()]
param(
    [string] $BaseUri = 'http://localhost:8080',
    [string] $RepoId,
    [string[]] $Queries,
    [ValidateRange(1, 1000)]
    [int] $Repetitions = 1,
    [ValidateRange(1, 100)]
    [int] $K = 5,
    [ValidateRange(1, 600)]
    [int] $TimeoutSeconds = 60,
    [string] $JsonOutputPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# The instrument DoD-1b scores on, and its three arms. The arms partition the
# whole query population, which is what lets the total act as a liveness witness
# for the instrument independently of which arm moved.
$AnnSearchMetric = 'repocontext_retrieval_ann_search_total'
$AnnStates = @('bootstrapping', 'exhaustive', 'approximate')

$BaseUri = $BaseUri.TrimEnd('/')

if (-not $Queries -or $Queries.Count -eq 0) {
    # A spread rather than one repeated string: retrieval is content-addressed,
    # and a single query embedding exercises a single neighbourhood.
    $Queries = @(
        'where is the readiness health probe wired',
        'how does the write-ahead log replay on activation',
        'what happens when the vector plane is still building',
        'conflict resolution between concurrent writers',
        'how are metrics pre-minted at zero and why',
        'backup capture failure handling',
        'grain activation lifecycle and deactivation',
        'approximate nearest neighbour partitioning threshold'
    )
}

function Write-Section {
    param([string] $Text)
    Write-Host ''
    Write-Host $Text -ForegroundColor Cyan
    Write-Host ('-' * $Text.Length) -ForegroundColor Cyan
}

<#
    Reads the Prometheus exposition and returns a hashtable of
    "metric{labels}" -> value for the ANN search instrument only. Absent is
    represented as a missing key rather than as zero, so "the series was never
    created" stays distinguishable from "the series is at zero". That
    distinction is the entire subject of this exercise and must not be flattened
    by the parser.
#>
function Get-AnnSearchSeries {
    param([string] $Uri, [int] $Timeout)

    $response = Invoke-WebRequest -Uri "$Uri/metrics" -TimeoutSec $Timeout -UseBasicParsing
    $text = $response.Content

    $series = @{}
    foreach ($line in ($text -split "`n")) {
        $trimmed = $line.Trim()
        if ($trimmed.Length -eq 0 -or $trimmed.StartsWith('#')) {
            continue
        }
        if ($trimmed -notlike "$AnnSearchMetric*") {
            continue
        }

        # <name>{<labels>} <value>  - the value may be an integer or a float.
        if ($trimmed -match '^(?<key>[^\s]+)\s+(?<value>[-+0-9.eE]+)$') {
            $series[$Matches['key']] = [double] $Matches['value']
        }
    }

    return $series
}

<#
    Sums the arms of the ANN search instrument by state tag. Returns a hashtable
    of state -> @{ Value; Present }. Present is false when no series carrying
    that state tag exists at all, which is a different and more serious reading
    than a zero: the reporter pre-mints all three arms at construction, so an
    absent arm means the series was refused at creation (a saturated collector
    ceiling) and nothing should be concluded from the instrument.
#>
function Measure-ArmTotals {
    param([hashtable] $Series)

    $result = @{}
    foreach ($state in $AnnStates) {
        $sum = 0.0
        $present = $false
        foreach ($key in $Series.Keys) {
            if ($key -match ('state="' + [regex]::Escape($state) + '"')) {
                $sum += $Series[$key]
                $present = $true
            }
        }
        $result[$state] = @{ Value = $sum; Present = $present }
    }
    return $result
}

# ---------------------------------------------------------------------------
# MCP streamable-HTTP client.
#
# The container's only application listener is the MCP endpoint; there is no
# REST search route. The reference "mcp" CLI is deliberately NOT used: a harness
# that silently no-ops when a CLI is absent reproduces, one level up, the exact
# defect it exists to delete.
# ---------------------------------------------------------------------------

$script:McpSessionId = $null
$script:NextRequestId = 1

<#
    Posts one JSON-RPC message and returns the parsed result. A streamable-HTTP
    MCP endpoint may answer either as application/json or as an SSE stream, so
    both framings are handled; an SSE body has its "data:" payloads extracted.
#>
function Invoke-McpRpc {
    param(
        [string] $Method,
        [hashtable] $Parameters,
        [switch] $IsNotification
    )

    $body = @{ jsonrpc = '2.0'; method = $Method }
    if ($Parameters) { $body['params'] = $Parameters }
    if (-not $IsNotification) {
        $body['id'] = $script:NextRequestId
        $script:NextRequestId++
    }

    $headers = @{
        'Accept'       = 'application/json, text/event-stream'
        'Content-Type' = 'application/json'
    }
    if ($script:McpSessionId) {
        $headers['Mcp-Session-Id'] = $script:McpSessionId
    }

    $json = $body | ConvertTo-Json -Depth 12 -Compress

    $response = Invoke-WebRequest -Uri $BaseUri -Method Post -Headers $headers `
        -Body $json -TimeoutSec $TimeoutSeconds -UseBasicParsing

    # The server assigns a session on initialize and expects it echoed back.
    $assigned = $response.Headers['Mcp-Session-Id']
    if ($assigned) {
        if ($assigned -is [array]) { $assigned = $assigned[0] }
        if ($assigned) { $script:McpSessionId = $assigned }
    }

    if ($IsNotification) { return $null }

    $content = $response.Content
    if (-not $content) { return $null }

    # SSE framing: pull the data payloads out and take the last complete one.
    if ($content -match '(?m)^\s*(event|data):') {
        $payloads = @()
        foreach ($line in ($content -split "`n")) {
            if ($line -match '^\s*data:\s?(?<payload>.*)$') {
                $candidate = $Matches['payload'].Trim()
                if ($candidate.Length -gt 0 -and $candidate -ne '[DONE]') {
                    $payloads += $candidate
                }
            }
        }
        if ($payloads.Count -eq 0) { return $null }
        $content = $payloads[-1]
    }

    return $content | ConvertFrom-Json
}

function Initialize-McpSession {
    $init = Invoke-McpRpc -Method 'initialize' -Parameters @{
        protocolVersion = '2024-11-05'
        capabilities    = @{}
        clientInfo      = @{ name = 'ann-query-probe'; version = '1.0' }
    }

    if (-not $init) {
        throw 'The MCP initialize call returned no parsed result.'
    }
    if ($init.PSObject.Properties.Name -contains 'error' -and $init.error) {
        throw "The MCP initialize call failed: $($init.error | ConvertTo-Json -Depth 6 -Compress)"
    }

    Invoke-McpRpc -Method 'notifications/initialized' -Parameters @{} -IsNotification | Out-Null
    return $init
}

<#
    Calls one MCP tool and returns a normalised outcome. A JSON-RPC error, a
    tool-level isError, and an unparseable body are all reported as failures
    with the reason preserved, because "issued but rejected" must never be
    silently counted as a success.
#>
function Invoke-McpTool {
    param([string] $Name, [hashtable] $Arguments)

    $outcome = [ordered]@{
        Tool      = $Name
        Succeeded = $false
        Reason    = $null
        Payload   = $null
    }

    try {
        $response = Invoke-McpRpc -Method 'tools/call' -Parameters @{
            name      = $Name
            arguments = $Arguments
        }
    }
    catch {
        $outcome.Reason = "transport: $($_.Exception.Message)"
        return $outcome
    }

    if (-not $response) {
        $outcome.Reason = 'empty response body'
        return $outcome
    }
    if ($response.PSObject.Properties.Name -contains 'error' -and $response.error) {
        $outcome.Reason = "jsonrpc error: $($response.error | ConvertTo-Json -Depth 6 -Compress)"
        return $outcome
    }
    if (-not ($response.PSObject.Properties.Name -contains 'result')) {
        $outcome.Reason = 'response carried no result member'
        return $outcome
    }

    $toolResult = $response.result
    if ($toolResult.PSObject.Properties.Name -contains 'isError' -and $toolResult.isError) {
        $outcome.Reason = 'tool reported isError'
        $outcome.Payload = $toolResult
        return $outcome
    }

    # The tool payload is carried as structuredContent when present, otherwise
    # as JSON text in the first content block.
    $payload = $null
    if ($toolResult.PSObject.Properties.Name -contains 'structuredContent' -and $toolResult.structuredContent) {
        $payload = $toolResult.structuredContent
    }
    elseif ($toolResult.PSObject.Properties.Name -contains 'content' -and $toolResult.content) {
        foreach ($block in $toolResult.content) {
            if ($block.PSObject.Properties.Name -contains 'text' -and $block.text) {
                try { $payload = $block.text | ConvertFrom-Json; break } catch { $payload = $block.text }
            }
        }
    }

    $outcome.Succeeded = $true
    $outcome.Payload = $payload
    return $outcome
}

function Get-MemberOrNull {
    param($Object, [string] $Name)
    if ($null -eq $Object) { return $null }
    if ($Object -isnot [psobject]) { return $null }
    if ($Object.PSObject.Properties.Name -contains $Name) { return $Object.$Name }
    return $null
}

# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------

Write-Section 'RepoContext ANN query probe'
Write-Host "Endpoint       : $BaseUri"
Write-Host "Repetitions    : $Repetitions"
Write-Host "Query set size : $($Queries.Count)"

# Liveness of the box itself. A failure here is a different owner's problem and
# must not be reported as a retrieval finding.
try {
    Invoke-WebRequest -Uri "$BaseUri/health/live" -TimeoutSec $TimeoutSeconds -UseBasicParsing | Out-Null
    Write-Host 'Health         : /health/live responded'
}
catch {
    Write-Host "REFUSED: the container did not answer /health/live at $BaseUri." -ForegroundColor Red
    Write-Host "  $($_.Exception.Message)"
    exit 3
}

$baselineSeries = Get-AnnSearchSeries -Uri $BaseUri -Timeout $TimeoutSeconds
$baselineArms = Measure-ArmTotals -Series $baselineSeries

try {
    Initialize-McpSession | Out-Null
    Write-Host 'MCP            : session initialised'
}
catch {
    Write-Host 'REFUSED: the MCP handshake failed, so no query could be issued.' -ForegroundColor Red
    Write-Host "  $($_.Exception.Message)"
    exit 3
}

# Resolve the repositories to probe.
$targetRepos = @()
if ($RepoId) {
    $targetRepos = @($RepoId)
}
else {
    $listed = Invoke-McpTool -Name 'repocontext_list_repos' -Arguments @{}
    if (-not $listed.Succeeded) {
        Write-Host "REFUSED: repocontext_list_repos failed ($($listed.Reason)), so no repository could be selected." -ForegroundColor Red
        exit 3
    }

    $repos = Get-MemberOrNull -Object $listed.Payload -Name 'repos'
    if ($null -eq $repos) { $repos = Get-MemberOrNull -Object $listed.Payload -Name 'repositories' }
    if ($repos) {
        foreach ($repo in $repos) {
            $id = Get-MemberOrNull -Object $repo -Name 'repoId'
            if (-not $id) { $id = Get-MemberOrNull -Object $repo -Name 'id' }
            if ($id) { $targetRepos += $id }
        }
    }
}

Write-Host "Repositories   : $(if ($targetRepos.Count) { $targetRepos -join ', ' } else { '(none resolved)' })"

# The input count is asserted before any query is issued. A probe with an empty
# input set produces "no approximate searches occurred", which is byte-identical
# to the failure this script exists to detect.
if ($targetRepos.Count -eq 0) {
    Write-Host ''
    Write-Host 'REFUSED: no repository resolved, so the input count is zero.' -ForegroundColor Red
    Write-Host 'A probe that issues no query cannot distinguish "the plane declined" from "nothing asked".'
    Write-Host 'Register a repository (repocontext_add_repo) or pass -RepoId, then re-run.'
    exit 2
}

Write-Section 'Issuing queries'

$issued = 0
$succeeded = 0
$records = @()

foreach ($repetition in 1..$Repetitions) {
    foreach ($repo in $targetRepos) {
        foreach ($query in $Queries) {
            $issued++
            $call = Invoke-McpTool -Name 'repocontext_search' -Arguments @{
                repoId = $repo
                query  = $query
                k      = $K
            }

            $record = [ordered]@{
                Repetition    = $repetition
                RepoId        = $repo
                Query         = $query
                Succeeded     = $call.Succeeded
                Reason        = $call.Reason
                Mode          = $null
                RetrievalPath = $null
                HitCount      = $null
            }

            if ($call.Succeeded) {
                $succeeded++
                $record.Mode = Get-MemberOrNull -Object $call.Payload -Name 'mode'
                $record.RetrievalPath = Get-MemberOrNull -Object $call.Payload -Name 'retrievalPath'
                $hits = Get-MemberOrNull -Object $call.Payload -Name 'hits'
                if ($null -ne $hits) { $record.HitCount = @($hits).Count }
            }

            $records += [pscustomobject] $record
        }
    }
}

Write-Host "Issued         : $issued"
Write-Host "Succeeded      : $succeeded"
Write-Host "Failed         : $($issued - $succeeded)"

# ---------------------------------------------------------------------------
# Admissibility gate. This is the spine of the script.
# ---------------------------------------------------------------------------

if ($issued -eq 0) {
    Write-Host ''
    Write-Host 'REFUSED: zero queries were issued.' -ForegroundColor Red
    Write-Host 'No instrument reading is emitted, because an unexercised counter and a'
    Write-Host 'counter the plane declined to move are the same observation.'
    exit 2
}

if ($succeeded -eq 0) {
    Write-Host ''
    Write-Host "REFUSED: $issued queries were issued and none succeeded." -ForegroundColor Red
    Write-Host 'This is a DIFFERENT failure from issuing none, and it has a different owner:'
    Write-Host 'the harness reached the box and the box rejected every call. Distinct reasons:'
    $records | Where-Object { -not $_.Succeeded } |
        Group-Object Reason |
        Sort-Object Count -Descending |
        Select-Object -First 5 |
        ForEach-Object { Write-Host "  [$($_.Count)] $($_.Name)" }
    Write-Host ''
    Write-Host 'No instrument reading is emitted.'
    exit 2
}

# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

$afterSeries = Get-AnnSearchSeries -Uri $BaseUri -Timeout $TimeoutSeconds
$afterArms = Measure-ArmTotals -Series $afterSeries

Write-Section 'Retrieval path attribution (from the search responses)'
Write-Host 'retrievalPath names which plane answered and, for keyword, why.'
Write-Host 'NOTE: "semantic.approximate" is declared unconditionally by the index and is'
Write-Host 'NOT evidence that the approximate plane served. Only the counter arms are.'
Write-Host ''

$pathGroups = $records | Where-Object { $_.Succeeded } | Group-Object RetrievalPath | Sort-Object Count -Descending
foreach ($group in $pathGroups) {
    $label = if ($group.Name) { $group.Name } else { '(absent)' }
    Write-Host ("  {0,-40} {1}" -f $label, $group.Count)
}

$modeGroups = $records | Where-Object { $_.Succeeded } | Group-Object Mode | Sort-Object Count -Descending
Write-Host ''
Write-Host 'mode:'
foreach ($group in $modeGroups) {
    $label = if ($group.Name) { $group.Name } else { '(absent)' }
    Write-Host ("  {0,-40} {1}" -f $label, $group.Count)
}

Write-Section 'Instrument deltas: repocontext_retrieval_ann_search_total'

$totalDelta = 0.0
$deltas = @{}
$absentArms = @()
foreach ($state in $AnnStates) {
    $before = $baselineArms[$state].Value
    $after = $afterArms[$state].Value
    $delta = $after - $before
    $deltas[$state] = $delta
    $totalDelta += $delta

    if (-not $afterArms[$state].Present) { $absentArms += $state }

    $presence = if ($afterArms[$state].Present) { '' } else { '   [SERIES ABSENT]' }
    Write-Host ("  state={0,-16} before={1,-10} after={2,-10} delta={3}{4}" -f $state, $before, $after, $delta, $presence)
}

Write-Host ''
Write-Host ("  TOTAL across all three arms: delta={0}" -f $totalDelta)

# ---------------------------------------------------------------------------
# Attribution of the delta to THIS probe's traffic.
#
# The container serves internal retrieval of its own (a periodic probe on the
# deployed rig runs roughly one search every two minutes), and the instrument
# arms are not tagged by caller, so a delta is not automatically attributable
# to the queries issued here. Comparing the delta against the probe's own
# succeeded count is the closest attribution available without a caller tag,
# and it is reported rather than assumed.
# ---------------------------------------------------------------------------

$attribution = $null
if ($totalDelta -eq $succeeded) {
    $attribution = 'EXACT'
    Write-Host ("  Attribution: EXACT - delta ({0}) equals succeeded ({1})." -f $totalDelta, $succeeded)
    Write-Host '    No concurrent internal traffic landed in this window.'
}
elseif ($totalDelta -gt $succeeded) {
    $attribution = 'CONTAMINATED'
    $extra = $totalDelta - $succeeded
    Write-Host ("  Attribution: CONTAMINATED - delta ({0}) exceeds succeeded ({1}) by {2}." -f $totalDelta, $succeeded, $extra) -ForegroundColor Yellow
    Write-Host '    Internal retrieval landed in the same window. This does not invalidate'
    Write-Host '    the reading: the probe still demonstrably contributed traffic. It does'
    Write-Host '    mean the per-arm deltas are not attributable to this probe alone.'
}
else {
    $attribution = 'SHORTFALL'
    $missing = $succeeded - $totalDelta
    Write-Host ("  Attribution: SHORTFALL - delta ({0}) is below succeeded ({1}) by {2}." -f $totalDelta, $succeeded, $missing) -ForegroundColor Yellow
    Write-Host '    Some successful queries did not reach the approximate plane at all, so'
    Write-Host '    they were never counted onto any arm. keyword.no_embedder is the usual'
    Write-Host '    cause; check the retrieval path attribution above.'
}

if ($absentArms.Count -gt 0) {
    Write-Host ''
    Write-Host "WARNING: these arms are ABSENT rather than zero: $($absentArms -join ', ')" -ForegroundColor Yellow
    Write-Host 'The reporter pre-mints all three at construction, so an absent arm means the'
    Write-Host 'series was refused at creation. Read lattice_metrics_series against the'
    Write-Host 'collector ceiling and check lattice_metrics_dropped_measurements_by_family_total'
    Write-Host 'before concluding anything at all from this instrument.'
}

Write-Section 'Verdict'

$verdict = $null
if ($totalDelta -eq 0) {
    $verdict = 'INSTRUMENT-DID-NOT-MOVE'
    Write-Host 'The harness succeeded but the instrument did not move.' -ForegroundColor Yellow
    Write-Host "  $succeeded queries succeeded, yet the three arms summed to zero delta."
    Write-Host '  The vector plane was therefore not consulted by these queries.'
    $noEmbedder = @($records | Where-Object { $_.RetrievalPath -eq 'keyword.no_embedder' }).Count
    if ($noEmbedder -gt 0) {
        Write-Host "  $noEmbedder response(s) reported keyword.no_embedder: no embedding provider is"
        Write-Host '  bound, so the plane is never reached and a DoD-1b zero carries NO information.'
    }
    else {
        Write-Host '  No response blamed a missing embedder, so this combination is a contradiction'
        Write-Host '  worth reporting: the plane should have counted every query it was asked.'
    }
}
elseif ($deltas['approximate'] -gt 0) {
    $verdict = 'APPROXIMATE-ARM-MOVED'
    Write-Host 'The approximate arm moved under issued traffic.' -ForegroundColor Green
    Write-Host "  approximate delta = $($deltas['approximate']) over $succeeded successful queries."
}
else {
    $verdict = 'PLANE-CONSULTED-APPROXIMATE-ZERO'
    Write-Host 'The plane was consulted and the approximate arm stayed at zero.' -ForegroundColor Yellow
    Write-Host "  total delta = $totalDelta, so this IS a measured zero rather than an absent one."
    Write-Host "  bootstrapping = $($deltas['bootstrapping']), exhaustive = $($deltas['exhaustive'])"
    if ($deltas['exhaustive'] -gt 0) {
        Write-Host '  The plane answered by exhaustive scan. Below the ANN training threshold this'
        Write-Host '  is correct behaviour and not a fault.'
    }
    if ($deltas['bootstrapping'] -gt 0) {
        Write-Host '  The plane could not answer and the fallback ladder ran.'
        $suppressed = @($records | Where-Object { $_.RetrievalPath -eq 'keyword.exact_fallback_suppressed' }).Count
        if ($suppressed -gt 0) {
            Write-Host ''
            Write-Host "  $suppressed response(s) reported keyword.exact_fallback_suppressed." -ForegroundColor Yellow
            Write-Host '  A stalled gather has left the exact fallback deliberately withheld, so'
            Write-Host '  neither the approximate plane nor the exact scan is answering and keyword'
            Write-Host '  recall is serving in their place. Note this suppression can OUTLIVE its'
            Write-Host '  cause: the breaker retries with a half-open probe, and if that probe re-runs'
            Write-Host '  the same gather against the same corpus reading it can only re-trip. Read the'
            Write-Host '  guard summary in the container log for the evaluation-to-trip ratio: a ratio'
            Write-Host '  at or near 1 is a deterministic defect, not the transient load that the'
            Write-Host '  absorbed-versus-propagated fault split would otherwise suggest.'
        }
    }
}

Write-Host ''
Write-Host "Verdict token  : $verdict"
Write-Host 'This script does not score DoD-1b. It reports what was issued and what moved.'

if ($JsonOutputPath) {
    $document = [ordered]@{
        endpoint       = $BaseUri
        timestampUtc   = (Get-Date).ToUniversalTime().ToString('o')
        repositories   = $targetRepos
        repetitions    = $Repetitions
        issued         = $issued
        succeeded      = $succeeded
        failed         = $issued - $succeeded
        armsBefore     = ($AnnStates | ForEach-Object { @{ state = $_; value = $baselineArms[$_].Value; present = $baselineArms[$_].Present } })
        armsAfter      = ($AnnStates | ForEach-Object { @{ state = $_; value = $afterArms[$_].Value; present = $afterArms[$_].Present } })
        deltas         = ($AnnStates | ForEach-Object { @{ state = $_; delta = $deltas[$_] } })
        totalDelta     = $totalDelta
        attribution    = $attribution
        verdict        = $verdict
        queries        = $records
    }
    $document | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $JsonOutputPath -Encoding utf8
    Write-Host ''
    Write-Host "Machine-readable result written to $JsonOutputPath"
}

exit 0
