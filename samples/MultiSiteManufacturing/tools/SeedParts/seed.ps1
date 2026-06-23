<#
.SYNOPSIS
  Publishes the SeedParts tool and runs it INSIDE the running cluster's Docker
  network, inserting parts into a lattice tree so the Explorer topology panel
  has enough content to render a multi-layer graph.

  The tool must run inside the Docker network because the only host-published
  ports route the read-only state API / replication / Blazor dashboard - the
  Orleans gateways and Azurite (which the client needs for clustering) are
  internal to msmfg_<cluster>-net.

  This file is a local dev aid and is intentionally NOT committed.

.EXAMPLE
  ./seed.ps1                 # 500 parts into the US cluster's mfg-facts tree
  ./seed.ps1 -Cluster eu -Count 500
#>
[CmdletBinding()]
param(
    [ValidateSet('us', 'eu')]
    [string]$Cluster = 'us',
    [int]$Count = 500,
    [string]$Tree = 'mfg-facts',
    [string]$RuntimeImage = 'mcr.microsoft.com/dotnet/sdk:10.0'
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$publish = Join-Path $here 'publish'
$network = "msmfg_$Cluster-net"

Write-Host "Publishing SeedParts..." -ForegroundColor Cyan
dotnet publish (Join-Path $here 'SeedParts.csproj') -c Release -o $publish --nologo -v q

if (-not (docker network ls --format '{{.Name}}' | Where-Object { $_ -eq $network })) {
    throw "Docker network '$network' not found. Is the '$Cluster' cluster running? (./run.ps1)"
}

Write-Host "Seeding $Count parts into '$Tree' on cluster '$Cluster'..." -ForegroundColor Cyan
docker run --rm --network $network -v "${publish}:/app" -w /app `
    $RuntimeImage dotnet /app/SeedParts.dll --cluster $Cluster --count $Count --tree $Tree
