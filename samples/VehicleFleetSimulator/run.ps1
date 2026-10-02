<#
.SYNOPSIS
    Stand up (or tear down) the full Vehicle Fleet Simulator stack locally via docker compose.

.DESCRIPTION
    Brings up Azurite + Silo + API + UI in containers, detached. The Azurite data volume
    is ALWAYS wiped before starting so every run begins with clean clustering and
    reminder tables. Grain state is in-memory and the sample does not configure
    streams, so no durable grain or stream-queue state is preserved across runs.

.PARAMETER Down
    Tear the stack down without bringing it back up. Always removes volumes.

.EXAMPLE
    ./run.ps1
    Wipe state, build (if needed), and start the stack detached.

.EXAMPLE
    ./run.ps1 -Down
    Stop the stack and remove all volumes.
#>
[CmdletBinding()]
param(
    [switch]$Down
)

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSCommandPath)

function Invoke-Compose {
    param([string[]]$ComposeArgs)
    Write-Host "→ docker compose $($ComposeArgs -join ' ')" -ForegroundColor Cyan
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose exited with code $LASTEXITCODE" }
}

# Always tear down with -v to wipe the azurite-data volume before (re)starting.
Invoke-Compose @('down', '-v')

if ($Down) { return }

Invoke-Compose @('up', '--build', '-d')
