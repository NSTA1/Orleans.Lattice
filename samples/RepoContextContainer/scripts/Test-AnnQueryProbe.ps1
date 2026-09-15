<#
.SYNOPSIS
    Self-test for Invoke-AnnQueryProbe.ps1. Proves the probe reports what the
    instruments say, and - more importantly - proves it REFUSES to report when
    it did not issue or did not succeed.

.DESCRIPTION
    Invoke-AnnQueryProbe exists because a zero on
    repocontext_retrieval_ann_search_total{state="approximate"} is ambiguous
    between "the approximate plane declined" and "nothing ever asked". Its value
    therefore rests entirely on its refusal paths: a probe that quietly reported
    a zero after issuing nothing would reproduce the exact defect it was written
    to delete.

    Refusal paths are the least-exercised code in any harness, so they are
    tested here against a mock rather than left to be discovered on an
    acceptance run. The mock is a real HTTP listener speaking the subset of MCP
    the probe uses, so the handshake, the tool calls, the exposition parsing and
    the delta arithmetic are all exercised for real. No container is required
    and nothing is deployed.

    The scenarios are chosen so that every verdict and every refusal is reached,
    including the two refusals the probe deliberately keeps distinct:

      * issued 0   - nothing to report, and no repository was resolved.
      * issued N, succeeded 0 - the box was reached and rejected every call.

    Those two have different owners and must never be collapsed into one
    "probe failed" message.

.PARAMETER Port
    Port for the mock listener. Defaults to 18080.

.EXAMPLE
    ./Test-AnnQueryProbe.ps1

.NOTES
    Exit code 0 means every scenario behaved as specified. Any non-zero exit
    means Invoke-AnnQueryProbe no longer does what its own documentation claims,
    and it must not be used to produce an acceptance reading until fixed.
#>
[CmdletBinding()]
param(
    [ValidateRange(1024, 65535)]
    [int] $Port = 18080
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$probePath = Join-Path $PSScriptRoot 'Invoke-AnnQueryProbe.ps1'
if (-not (Test-Path -LiteralPath $probePath)) {
    throw "Expected the probe at $probePath"
}

# ---------------------------------------------------------------------------
# Mock MCP + metrics endpoint.
#
# Runs in a background job so the probe can be invoked as a separate process,
# exactly as an operator would invoke it.
# ---------------------------------------------------------------------------

$mockServer = {
    param([int] $Port, [string] $Scenario)

    $listener = New-Object System.Net.HttpListener
    $listener.Prefixes.Add("http://localhost:$Port/")
    $listener.Start()

    # Number of searches the mock has served. The exposition is derived from
    # this, so the probe observes a genuine before/after delta rather than two
    # constants, which would make the delta arithmetic vacuous.
    $served = 0

    function Write-Reply {
        param($Context, [string] $Body, [string] $ContentType = 'application/json', [int] $Status = 200)
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Body)
        $Context.Response.StatusCode = $Status
        $Context.Response.ContentType = $ContentType
        $Context.Response.ContentLength64 = $bytes.Length
        $Context.Response.OutputStream.Write($bytes, 0, $bytes.Length)
        $Context.Response.OutputStream.Close()
    }

    try {
        while ($listener.IsListening) {
            $context = $listener.GetContext()
            $request = $context.Request
            $path = $request.Url.AbsolutePath

            if ($path -eq '/shutdown') {
                Write-Reply -Context $context -Body 'bye' -ContentType 'text/plain'
                break
            }

            if ($path -eq '/health/live') {
                Write-Reply -Context $context -Body 'ok' -ContentType 'text/plain'
                continue
            }

            if ($path -eq '/health/ready') {
                # Scenarios model the three readiness shapes the rig can show:
                # ready, not-ready-with-a-diagnosis, and an endpoint that is
                # not answering at all.
                if ($Scenario -eq 'ready-unreachable') {
                    # Answer nothing at all: close the connection without a
                    # reply so the probe's catch arm is exercised.
                    $context.Response.Abort()
                    continue
                }

                if ($Scenario -eq 'approximate') {
                    Write-Reply -Context $context -Body 'Healthy' -ContentType 'text/plain' -Status 200
                    continue
                }

                $notReady = 'Not ready: the vector plane has not served semantic retrieval, so ' +
                            'searches are answering as keyword.vector_plane_unavailable, or as ' +
                            'keyword.exact_fallback_suppressed when a stalled gather has left the ' +
                            'exact fallback withheld. Run a search and read its retrievalPath to ' +
                            'tell which. Whether a build is in progress is not known here; check ' +
                            "the index build's own status rather than inferring it from this line."
                Write-Reply -Context $context -Body $notReady -ContentType 'text/plain' -Status 503
                continue
            }

            if ($path -eq '/metrics') {
                # Which arm absorbs the served queries depends on the scenario.
                $bootstrapping = 0
                $exhaustive = 0
                $approximate = 0
                switch ($Scenario) {
                    'approximate' { $approximate = $served }
                    'contaminated' { $approximate = $served }
                    'ready-unreachable' { $approximate = $served }
                    'exhaustive' { $exhaustive = $served }
                    'bootstrapping' { $bootstrapping = $served }
                    'suppressed' { $bootstrapping = $served }
                    default { }
                }

                $lines = @(
                    "# HELP repocontext_retrieval_ann_search_total Semantic searches by plane state.",
                    "# TYPE repocontext_retrieval_ann_search_total counter"
                )
                $lines += "repocontext_retrieval_ann_search_total{state=`"bootstrapping`",lattice_tenant=`"platform`"} $bootstrapping"
                $lines += "repocontext_retrieval_ann_search_total{state=`"exhaustive`",lattice_tenant=`"platform`"} $exhaustive"
                if ($Scenario -ne 'absent-arm') {
                    $lines += "repocontext_retrieval_ann_search_total{state=`"approximate`",lattice_tenant=`"platform`"} $approximate"
                }
                $lines += "some_other_metric_total 41"

                Write-Reply -Context $context -Body (($lines -join "`n") + "`n") -ContentType 'text/plain'
                continue
            }

            # Everything else is the MCP endpoint.
            $reader = New-Object System.IO.StreamReader($request.InputStream, $request.ContentEncoding)
            $raw = $reader.ReadToEnd()
            $reader.Close()

            $message = $null
            if ($raw) { try { $message = $raw | ConvertFrom-Json } catch { $message = $null } }

            $method = $null
            if ($message -and $message.PSObject.Properties.Name -contains 'method') { $method = $message.method }

            if ($method -eq 'initialize') {
                $context.Response.Headers.Add('Mcp-Session-Id', 'mock-session-1')
                $body = @{
                    jsonrpc = '2.0'
                    id      = $message.id
                    result  = @{
                        protocolVersion = '2024-11-05'
                        capabilities    = @{ tools = @{} }
                        serverInfo      = @{ name = 'mock-repocontext'; version = '1.0' }
                    }
                } | ConvertTo-Json -Depth 8 -Compress
                Write-Reply -Context $context -Body $body
                continue
            }

            if ($method -eq 'notifications/initialized') {
                Write-Reply -Context $context -Body '' -Status 202
                continue
            }

            if ($method -eq 'tools/call') {
                $toolName = $message.params.name

                if ($toolName -eq 'repocontext_list_repos') {
                    $repoList = if ($Scenario -eq 'no-repos') { @() } else { @(@{ repoId = 'demo' }) }
                    $body = @{
                        jsonrpc = '2.0'
                        id      = $message.id
                        result  = @{ structuredContent = @{ repos = $repoList } }
                    } | ConvertTo-Json -Depth 8 -Compress
                    Write-Reply -Context $context -Body $body
                    continue
                }

                if ($toolName -eq 'repocontext_search') {
                    if ($Scenario -eq 'all-fail') {
                        $body = @{
                            jsonrpc = '2.0'
                            id      = $message.id
                            result  = @{
                                isError = $true
                                content = @(@{ type = 'text'; text = 'mock rejection' })
                            }
                        } | ConvertTo-Json -Depth 8 -Compress
                        Write-Reply -Context $context -Body $body
                        continue
                    }

                    # In the contaminated scenario the mock counts two searches
                    # for every one the probe issues, standing in for concurrent
                    # internal retrieval the probe did not cause.
                    if ($Scenario -eq 'contaminated') { $served += 2 } else { $served++ }

                    $mode = 'semantic'
                    $retrievalPath = 'semantic.approximate'
                    if ($Scenario -eq 'no-embedder') {
                        $mode = 'keyword'
                        $retrievalPath = 'keyword.no_embedder'
                    }
                    elseif ($Scenario -eq 'bootstrapping') {
                        $mode = 'keyword'
                        $retrievalPath = 'keyword.vector_plane_unavailable'
                    }
                    elseif ($Scenario -eq 'suppressed') {
                        $mode = 'keyword'
                        $retrievalPath = 'keyword.exact_fallback_suppressed'
                    }

                    $body = @{
                        jsonrpc = '2.0'
                        id      = $message.id
                        result  = @{
                            structuredContent = @{
                                mode          = $mode
                                retrievalPath = $retrievalPath
                                hits          = @(@{ key = 'repo/demo/file/a.cs'; path = 'a.cs' })
                            }
                        }
                    } | ConvertTo-Json -Depth 8 -Compress
                    Write-Reply -Context $context -Body $body
                    continue
                }
            }

            Write-Reply -Context $context -Body '{"jsonrpc":"2.0","error":{"code":-32601,"message":"unknown"}}'
        }
    }
    finally {
        if ($listener.IsListening) { $listener.Stop() }
        $listener.Close()
    }
}

function Start-Mock {
    param([string] $Scenario)

    $job = Start-Job -ScriptBlock $mockServer -ArgumentList $Port, $Scenario

    # Wait for the listener to accept before returning, so a scenario can never
    # be scored against a server that had not started.
    $deadline = (Get-Date).AddSeconds(20)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -Uri "http://localhost:$Port/health/live" -TimeoutSec 2 -UseBasicParsing | Out-Null
            return $job
        }
        catch {
            if ($job.State -eq 'Failed') {
                throw "Mock server job failed to start: $(Receive-Job $job 2>&1)"
            }
            Start-Sleep -Milliseconds 200
        }
    }

    throw "Mock server did not start listening on port $Port within 20 seconds."
}

function Stop-Mock {
    param($Job)
    try { Invoke-WebRequest -Uri "http://localhost:$Port/shutdown" -TimeoutSec 3 -UseBasicParsing | Out-Null } catch { }
    try { Wait-Job $Job -Timeout 5 | Out-Null } catch { }
    try { Remove-Job $Job -Force } catch { }
}

# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

$results = @()

function Invoke-Scenario {
    param(
        [string] $Name,
        [string] $Scenario,
        [int] $ExpectedExitCode,
        [string[]] $MustContain,
        [string[]] $MustNotContain = @(),
        [switch] $NoServer
    )

    Write-Host ''
    Write-Host "SCENARIO: $Name" -ForegroundColor Cyan

    $job = $null
    if (-not $NoServer) { $job = Start-Mock -Scenario $Scenario }

    try {
        $output = & pwsh -NoProfile -File $probePath `
            -BaseUri "http://localhost:$Port" -Repetitions 1 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
    }
    finally {
        if ($job) { Stop-Mock -Job $job }
    }

    $failures = @()
    if ($exitCode -ne $ExpectedExitCode) {
        $failures += "expected exit $ExpectedExitCode but got $exitCode"
    }
    foreach ($needle in $MustContain) {
        if ($output -notmatch [regex]::Escape($needle)) {
            $failures += "output did not contain '$needle'"
        }
    }
    foreach ($needle in $MustNotContain) {
        if ($output -match [regex]::Escape($needle)) {
            $failures += "output unexpectedly contained '$needle'"
        }
    }

    if ($failures.Count -eq 0) {
        Write-Host "  PASS (exit $exitCode)" -ForegroundColor Green
    }
    else {
        Write-Host "  FAIL" -ForegroundColor Red
        $failures | ForEach-Object { Write-Host "    - $_" -ForegroundColor Red }
        Write-Host '    ---- probe output ----'
        Write-Host $output
    }

    $script:results += [pscustomobject]@{
        Scenario = $Name
        Passed   = ($failures.Count -eq 0)
        ExitCode = $exitCode
        Failures = $failures
    }
}

Write-Host 'Self-test for Invoke-AnnQueryProbe.ps1' -ForegroundColor Cyan
Write-Host "Mock listener port: $Port"

# The approximate arm moves. This is the only scenario in which DoD-1b's arm
# rises, and the probe must say so without claiming to have scored anything.
Invoke-Scenario -Name 'approximate arm moves' -Scenario 'approximate' -ExpectedExitCode 0 -MustContain @(
    'APPROXIMATE-ARM-MOVED',
    'Issued         : 8',
    'Succeeded      : 8',
    'Attribution: EXACT',
    '/health/ready 200 (READY)'
)

# Concurrent internal retrieval lands in the same window. The probe must report
# the delta as contaminated rather than claiming all of it, because the arms
# carry no caller tag and the attribution is genuinely not available.
Invoke-Scenario -Name 'internal traffic contaminates the delta' -Scenario 'contaminated' -ExpectedExitCode 0 -MustContain @(
    'Attribution: CONTAMINATED',
    'not attributable to this probe alone'
)

# The rig's present state: a stalled gather has left the exact fallback
# withheld. The probe must name the suppression and warn that it can outlive
# its cause.
Invoke-Scenario -Name 'exact fallback suppressed (live rig shape)' -Scenario 'suppressed' -ExpectedExitCode 0 -MustContain @(
    'PLANE-CONSULTED-APPROXIMATE-ZERO',
    'keyword.exact_fallback_suppressed',
    'OUTLIVES its',
    'deterministic defect',
    'DO NOT record the breaker as the root cause',
    'ShardRootGrain.ReadLeafAsync -> TimeoutException'
)

# A 503 on readiness is the system's own diagnosis and is the most authoritative
# line the harness can emit. It must be reported verbatim, and it must NOT be
# treated as a probe failure or gate the measurement.
Invoke-Scenario -Name 'readiness 503 is reported, not treated as failure' -Scenario 'suppressed' -ExpectedExitCode 0 -MustContain @(
    '/health/ready 503 (NOT READY)',
    'The system says, in its own words:',
    'the vector plane has not served semantic retrieval',
    'more authoritative than any counter delta'
)

# Readiness unreadable must not abort the run: liveness already passed, so the
# counter delta still stands and suppressing it would discard the measurement.
Invoke-Scenario -Name 'readiness unreadable does not gate the measurement' -Scenario 'ready-unreachable' -ExpectedExitCode 0 -MustContain @(
    '/health/ready could not be read',
    'Continuing. The counter delta below stands on its own.',
    'APPROXIMATE-ARM-MOVED'
)

# The plane answered exhaustively. The approximate arm is zero, but the total
# moved, so this is a MEASURED zero. That distinction is the whole point.
Invoke-Scenario -Name 'exhaustive: measured zero on approximate' -Scenario 'exhaustive' -ExpectedExitCode 0 -MustContain @(
    'PLANE-CONSULTED-APPROXIMATE-ZERO',
    'measured zero rather than an absent one',
    'exhaustive scan'
)

# No embedder bound: the plane is never consulted, so the counter cannot move
# and a DoD-1b zero carries no information at all.
Invoke-Scenario -Name 'no embedder: instrument cannot move' -Scenario 'no-embedder' -ExpectedExitCode 0 -MustContain @(
    'INSTRUMENT-DID-NOT-MOVE',
    'keyword.no_embedder',
    'carries NO information',
    'Attribution: SHORTFALL'
)

# The plane was consulted and could not answer. The bootstrapping arm absorbs
# the traffic, so the total still moves.
Invoke-Scenario -Name 'bootstrapping: fallback ladder ran' -Scenario 'bootstrapping' -ExpectedExitCode 0 -MustContain @(
    'PLANE-CONSULTED-APPROXIMATE-ZERO',
    'keyword.vector_plane_unavailable',
    'could not answer'
)

# REFUSAL 1: no repository resolved, so the input count is zero. The probe must
# refuse before issuing anything, and must not print a delta.
Invoke-Scenario -Name 'REFUSAL: zero repositories resolved' -Scenario 'no-repos' -ExpectedExitCode 2 -MustContain @(
    'REFUSED',
    'input count is zero'
) -MustNotContain @(
    'TOTAL across all three arms'
)

# REFUSAL 2: queries were issued and every one was rejected. This is a
# DIFFERENT failure from refusal 1 and the probe must say so, again without
# printing an instrument reading.
Invoke-Scenario -Name 'REFUSAL: issued but none succeeded' -Scenario 'all-fail' -ExpectedExitCode 2 -MustContain @(
    'REFUSED',
    'none succeeded',
    'DIFFERENT failure'
) -MustNotContain @(
    'TOTAL across all three arms'
)

# An arm that is absent rather than zero means the series was refused at
# creation, and nothing may be concluded from the instrument.
Invoke-Scenario -Name 'absent arm is not a zero' -Scenario 'absent-arm' -ExpectedExitCode 0 -MustContain @(
    'SERIES ABSENT',
    'refused at creation'
)

# The box is not there at all. That is a different owner's problem and must not
# be reported as a retrieval finding.
Invoke-Scenario -Name 'REFUSAL: container unreachable' -Scenario 'none' -ExpectedExitCode 3 -NoServer -MustContain @(
    'REFUSED',
    'did not answer /health/live'
)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

Write-Host ''
Write-Host 'SUMMARY' -ForegroundColor Cyan
Write-Host '-------' -ForegroundColor Cyan

# Asserting the scenario count is non-zero before reporting, for the same
# reason the probe asserts its issued count: a runner that executed nothing
# reports no failures, and so does a clean run.
if ($results.Count -eq 0) {
    Write-Host 'REFUSED: no scenario executed, so this run is not evidence of anything.' -ForegroundColor Red
    exit 2
}

foreach ($result in $results) {
    $status = if ($result.Passed) { 'PASS' } else { 'FAIL' }
    $colour = if ($result.Passed) { 'Green' } else { 'Red' }
    Write-Host ("  {0,-6} {1}" -f $status, $result.Scenario) -ForegroundColor $colour
}

$failed = @($results | Where-Object { -not $_.Passed }).Count
Write-Host ''
Write-Host "$($results.Count) scenario(s), $failed failed."

if ($failed -gt 0) { exit 1 }
exit 0
