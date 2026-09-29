<#
    .SYNOPSIS
        Minimal MCP streamable-HTTP client shared by the container probes.

    .DESCRIPTION
        The repocontext container's only application listener is the MCP
        endpoint; there is no REST route to any of this. The reference "mcp" CLI
        is deliberately NOT used: a harness that silently no-ops when a CLI is
        absent reproduces, one level up, the exact defect a probe exists to
        delete.

        This file is dot-sourced, following the `_`-prefixed convention already
        used by _provenance.ps1, _deployManifest.ps1 and _tuningKnobs.ps1. It was
        extracted from Invoke-AnnQueryProbe.ps1 when a second probe needed the
        same client: a copy would have drifted, and the usual direction of drift
        is the copy quietly proving less than the original while looking
        identical at the call site.

        Call Set-McpEndpoint once before any other function here.

    .EXAMPLE
        . "$PSScriptRoot/_mcpClient.ps1"
        Set-McpEndpoint -BaseUri 'http://localhost:8080' -TimeoutSeconds 240 -ClientName 'my-probe'
        Initialize-McpSession | Out-Null
        $outcome = Invoke-McpTool -Name 'repocontext_health' -Arguments @{ repoId = 'lattice' }
#>

$script:McpSessionId = $null
$script:NextRequestId = 1
$script:McpBaseUri = $null
$script:McpTimeoutSeconds = 240
$script:McpClientName = 'repocontext-probe'

<#
    Points the client at one endpoint and resets any session already in force.

    The reset is not incidental: a session id belongs to the endpoint that
    issued it, so carrying one across a re-point would send the previous box's
    session to a different box, which fails in a way that reads as a protocol
    fault rather than as a harness mistake.
#>
function Set-McpEndpoint {
    param(
        [Parameter(Mandatory)][string] $BaseUri,
        [int] $TimeoutSeconds = 240,
        [string] $ClientName = 'repocontext-probe'
    )

    $script:McpBaseUri = $BaseUri.TrimEnd('/')
    $script:McpTimeoutSeconds = $TimeoutSeconds
    $script:McpClientName = $ClientName
    $script:McpSessionId = $null
    $script:NextRequestId = 1
}

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

    if (-not $script:McpBaseUri) {
        throw 'Call Set-McpEndpoint before using the MCP client.'
    }

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

    $response = Invoke-WebRequest -Uri $script:McpBaseUri -Method Post -Headers $headers `
        -Body $json -TimeoutSec $script:McpTimeoutSeconds -UseBasicParsing

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
        clientInfo      = @{ name = $script:McpClientName; version = '1.0' }
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
