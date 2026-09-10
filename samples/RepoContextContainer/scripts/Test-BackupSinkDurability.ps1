<#
.SYNOPSIS
    Demonstrates that the RepoContext agent-memory backup sink survives
    "docker compose down -v".

.DESCRIPTION
    This repository lost several hundred durable agent-memory entries to a
    routine "docker compose down -v". The backup sink added for that incident is
    only worth having if the same gesture does not also destroy the backups, so
    that property is demonstrated here rather than asserted in a comment.

    The demonstration is deliberately end to end. The unit fixtures in
    test/lattice.api.mcp.repocontext/Host/RepoContextBackupSinkVolumeTests.cs
    already prove, by resolving the real compose file, that the sink source is a
    host bind mount and is absent from the project-managed volumes block. That is
    a statement about the file. This script is a statement about docker: it
    starts the sink, lets Azurite write its own on-disk state, runs the exact
    destructive command, and then checks what is left.

    Everything runs under an isolated compose project name and an isolated host
    directory, so it cannot touch a stack you already have running. It starts one
    Azurite container and nothing else - not the MCP server, not the embedder.

.PARAMETER ProjectName
    The isolated compose project to create and destroy. Must not be the project
    name of any stack you care about.

.PARAMETER SinkPath
    Host directory to use as the bind-mount source. Defaults to a fresh
    directory under the system temp path. It is left in place on success so you
    can inspect it; the script prints where it is.

.PARAMETER Force
    Proceed even when containers from another compose project are running. The
    script otherwise refuses, because CPU contention from this probe can confound
    a benchmark measurement in progress.

.EXAMPLE
    ./Test-BackupSinkDurability.ps1

.NOTES
    Exit code 0 means the sink survived. Any non-zero exit means the durability
    claim in docs/lattice.api.mcp.repocontext/container.md is not true of this
    machine, and the documented survival list must be corrected before anyone
    relies on it.
#>
[CmdletBinding()]
param(
    [string] $ProjectName = 'repocontext-backup-durability-probe',
    [string] $SinkPath,
    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$composeFile = Join-Path (Split-Path -Parent $PSScriptRoot) 'docker-compose.yml'
if (-not (Test-Path -LiteralPath $composeFile)) {
    throw "Expected the sample compose file at $composeFile"
}

if ($ProjectName -eq 'repocontextcontainer' -or $ProjectName -eq 'repocontext') {
    throw "Refusing to use project name '$ProjectName': this script runs 'down -v' and must never target a real stack."
}

if (-not $SinkPath) {
    $SinkPath = Join-Path ([System.IO.Path]::GetTempPath()) "repocontext-sink-probe-$([System.Guid]::NewGuid().ToString('N').Substring(0, 8))"
}

function Invoke-Docker {
    param([string[]] $Arguments, [switch] $IgnoreFailure)

    Write-Verbose "docker $($Arguments -join ' ')"
    $output = & docker @Arguments 2>&1
    if ($LASTEXITCODE -ne 0 -and -not $IgnoreFailure) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE`n$output"
    }

    return $output
}

# A probe that competes for CPU with a measurement in progress corrupts the
# measurement, so refuse by default rather than leaving it to be noticed later.
$running = Invoke-Docker -Arguments @('ps', '--format', '{{.Names}}') -IgnoreFailure
$foreign = @($running | Where-Object { $_ -and $_ -notlike "$ProjectName*" })
if ($foreign.Count -gt 0 -and -not $Force) {
    Write-Host 'Containers from another compose project are running:' -ForegroundColor Yellow
    $foreign | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
    throw 'Refusing to start a probe container while other containers are running. Re-run with -Force if you are certain nothing is being measured.'
}

New-Item -ItemType Directory -Path $SinkPath -Force | Out-Null
$SinkPath = (Resolve-Path -LiteralPath $SinkPath).Path

Write-Host "Compose project : $ProjectName"
Write-Host "Sink host path  : $SinkPath"
Write-Host ''

$env:REPOCONTEXT_BACKUP_PATH = $SinkPath
$env:REPOCONTEXT_BACKUP_SINK_PORT = '11099'

$composeArgs = @('compose', '-f', $composeFile, '-p', $ProjectName)
$succeeded = $false

try {
    Write-Host 'Starting the backup sink alone (no MCP server, no embedder).'
    Invoke-Docker -Arguments ($composeArgs + @('up', '-d', 'azurite-backup-sink')) | Out-Null

    # Azurite writes its metadata database on start. Waiting for that is what
    # makes this a test of real service state rather than of a file we planted.
    $deadline = (Get-Date).AddSeconds(60)
    $seeded = $false
    while ((Get-Date) -lt $deadline) {
        if (@(Get-ChildItem -LiteralPath $SinkPath -Force -ErrorAction SilentlyContinue).Count -gt 0) {
            $seeded = $true
            break
        }

        Start-Sleep -Seconds 2
    }

    if (-not $seeded) {
        throw "Azurite wrote nothing to $SinkPath within 60s, so this run cannot demonstrate anything about durability."
    }

    $before = @(Get-ChildItem -LiteralPath $SinkPath -Force -Recurse | Select-Object -ExpandProperty FullName)
    Write-Host "Azurite wrote $($before.Count) item(s) into the bind mount:"
    $before | Select-Object -First 10 | ForEach-Object { Write-Host "  $_" }

    $marker = Join-Path $SinkPath 'durability-probe.txt'
    Set-Content -LiteralPath $marker -Value "written $(Get-Date -Format o)" -Encoding ascii

    $projectVolumes = @(Invoke-Docker -Arguments @('volume', 'ls', '--quiet', '--filter', "label=com.docker.compose.project=$ProjectName"))
    Write-Host ''
    Write-Host "Project-managed volumes before down -v : $($projectVolumes.Count)"

    Write-Host ''
    Write-Host 'Running the exact command that caused the original loss: docker compose down -v' -ForegroundColor Cyan
    Invoke-Docker -Arguments ($composeArgs + @('down', '-v', '--remove-orphans')) | Out-Null

    $survivingVolumes = @(Invoke-Docker -Arguments @('volume', 'ls', '--quiet', '--filter', "label=com.docker.compose.project=$ProjectName"))
    if ($survivingVolumes.Count -ne 0) {
        throw "down -v left $($survivingVolumes.Count) project volume(s) behind, so this run does not demonstrate that -v did its usual work."
    }

    Write-Host 'Project-managed volumes after down -v  : 0 (as expected: -v removed them)'

    if (-not (Test-Path -LiteralPath $SinkPath)) {
        throw "FAILED: the sink directory $SinkPath was removed by down -v."
    }

    $after = @(Get-ChildItem -LiteralPath $SinkPath -Force -Recurse | Select-Object -ExpandProperty FullName)
    $lost = @($before | Where-Object { $after -notcontains $_ })
    if ($lost.Count -gt 0) {
        Write-Host 'FAILED: down -v removed sink content:' -ForegroundColor Red
        $lost | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
        throw "down -v destroyed $($lost.Count) item(s) of sink state."
    }

    if (-not (Test-Path -LiteralPath $marker)) {
        throw "FAILED: the marker file was removed by down -v."
    }

    Write-Host ''
    Write-Host "PASSED: down -v removed every project volume and left all $($after.Count) sink item(s) intact." -ForegroundColor Green
    Write-Host "Inspect the surviving sink state at: $SinkPath"
    Write-Host ''
    Write-Host 'This demonstrates survival of down -v on this host only. It is a'
    Write-Host 'same-host copy, not an off-site backup: deleting the directory above,'
    Write-Host 'git clean -xdf, disk loss, or host loss all still destroy it.'
    $succeeded = $true
}
finally {
    if (-not $succeeded) {
        Write-Host ''
        Write-Host 'Cleaning up the probe project after a failure.' -ForegroundColor Yellow
        Invoke-Docker -Arguments ($composeArgs + @('down', '-v', '--remove-orphans')) -IgnoreFailure | Out-Null
    }

    Remove-Item Env:REPOCONTEXT_BACKUP_PATH -ErrorAction SilentlyContinue
    Remove-Item Env:REPOCONTEXT_BACKUP_SINK_PORT -ErrorAction SilentlyContinue
}
