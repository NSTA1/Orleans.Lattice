#requires -Version 7
<#
.SYNOPSIS
    Tails ACI container logs for the azure-throughput benchmark.
.DESCRIPTION
    The silo container writes a one-line-per-second "Entries written per second={rate}"
    record. The producer container writes its own outbound rate.

    `az container logs --follow` silently wedges against ACI after a few seconds
    (the stream connection drops and the CLI does not surface an error or
    reconnect), so this script polls `az container logs` on an interval and
    prints only the new tail since the previous poll. The container itself
    is unaffected; the polling is purely client-side.

    Optional args:
      -Container  one of 'silo' (default) or 'producer'.
      -IntervalSec  poll interval in seconds (default 3).
#>

[CmdletBinding()]
param(
    [ValidateSet('silo', 'producer')]
    [string] $Container = 'silo',
    [int]    $IntervalSec = 3
)

$ErrorActionPreference = 'Stop'

$ctxPath = Join-Path $PSScriptRoot '.context.json'
if (-not (Test-Path $ctxPath)) {
    throw "Run 10-provision.ps1 first; missing $ctxPath."
}
$ctx = Get-Content $ctxPath | ConvertFrom-Json
$containerGroup = "$($ctx.Prefix)-bench"

Write-Host "[logs] polling $Container from $containerGroup every ${IntervalSec}s (Ctrl+C to stop) ..." -ForegroundColor Cyan

# Track the last line we've already printed so each poll only emits the
# delta. ACI returns the full container stdout buffer on every call, so
# we de-duplicate client-side rather than burning bandwidth on `--follow`.
# We compare line-by-line rather than as a single concatenated blob:
# (a) the buffer can shrink between polls (ACI truncation) which a raw
# Substring on the previous full text would crash on, and (b) line-level
# diffing lets us print partial new lines correctly even when the buffer
# rotates.
[string[]] $lastLines = @()
$firstPoll = $true
while ($true) {
    try {
        # Default output (no --output tsv): tsv collapses every newline
        # to a tab and produces one giant line, which defeats line-level
        # diffing. The CLI's default is the raw stdout buffer with
        # newlines preserved, which is what we want.
        $raw = az container logs `
            --resource-group $ctx.ResourceGroup `
            --name $containerGroup `
            --container-name $Container 2>$null
    }
    catch {
        Write-Host "[logs] poll failed: $($_.Exception.Message) - retrying in ${IntervalSec}s" -ForegroundColor Yellow
        Start-Sleep -Seconds $IntervalSec
        continue
    }

    if ($null -ne $raw) {
        # $raw arrives as either a single multi-line string or a string[]
        # depending on the PowerShell pipeline state. Normalise to a
        # string[] so the comparison below is well-defined regardless.
        $currentLines = if ($raw -is [string]) { $raw -split "`r?`n" } else { [string[]]$raw }

        if ($firstPoll) {
            if ($currentLines.Length -gt 0) { Write-Host ($currentLines -join "`n") }
            $firstPoll = $false
        }
        else {
            # Find the longest common prefix of $lastLines and $currentLines.
            # Anything after that prefix in $currentLines is new content
            # to print; if $currentLines is shorter or diverges early, the
            # buffer rotated and we reprint the whole thing.
            $prefixLen = 0
            $maxPrefix = [Math]::Min($lastLines.Length, $currentLines.Length)
            while ($prefixLen -lt $maxPrefix -and $lastLines[$prefixLen] -eq $currentLines[$prefixLen]) {
                $prefixLen++
            }

            if ($prefixLen -lt $lastLines.Length) {
                # Divergence: buffer rotated or truncated. Reprint everything.
                Write-Host "[logs] -- log buffer rotated --" -ForegroundColor DarkGray
                Write-Host ($currentLines -join "`n")
            }
            elseif ($prefixLen -lt $currentLines.Length) {
                # Pure append: print only the new tail.
                $newTail = $currentLines[$prefixLen..($currentLines.Length - 1)]
                Write-Host ($newTail -join "`n")
            }
            # else: nothing new, stay silent.
        }
        $lastLines = $currentLines
    }

    Start-Sleep -Seconds $IntervalSec
}
