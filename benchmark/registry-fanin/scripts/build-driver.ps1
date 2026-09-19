#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Publishes the sidecar driver on the host and lays it into a COPY-only image.

.DESCRIPTION
	The publish runs on the HOST, not in Docker, deliberately. This repository
	restores through a private feed proxy configured in the host's NuGet.Config,
	and while that proxy IS reachable from a container, plumbing the config in
	as a build secret for every driver rebuild buys nothing: the driver changes
	far more often than the silo image does during a measurement session, and a
	COPY-only image rebuilds in seconds with no network access at all.

	Publishing linux-x64 framework-dependent (not self-contained) keeps the
	image small and matches the aspnet runtime base the silo itself uses.
#>

[CmdletBinding()]
param(
	[string] $ParametersFile,
	[switch] $SkipImage
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$RigRoot = Split-Path -Parent $PSScriptRoot
$RepoRoot = Split-Path -Parent (Split-Path -Parent $RigRoot)

$config = Get-FanInConfig -ScriptRoot $PSScriptRoot -ParametersFile $ParametersFile
$null = Assert-FanInIsolation -Config $config

$project = Join-Path $RigRoot 'Driver/Orleans.Lattice.Benchmark.RegistryFanIn.csproj'
$publish = Join-Path $RigRoot 'Driver/publish'

Write-Host "Publishing the driver to $publish" -ForegroundColor Cyan
if (Test-Path -LiteralPath $publish) { Remove-Item -LiteralPath $publish -Recurse -Force }

& dotnet publish $project -c Release -r linux-x64 --self-contained false -o $publish
if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE." }

if ($SkipImage) {
	Write-Host 'Published. Image build skipped.' -ForegroundColor Green
	return
}

# The build context is the PUBLISH directory, not the repository root, so an
# accidental wide context cannot bake the working tree - including a
# NuGet.Config carrying a private feed - into an image layer.
Write-Host "Building $($config.DriverImage)" -ForegroundColor Cyan
& docker build -f (Join-Path $RigRoot 'Driver/Dockerfile') -t $config.DriverImage $publish
if ($LASTEXITCODE -ne 0) { throw "docker build failed with exit code $LASTEXITCODE." }

Write-Host "Built $($config.DriverImage)." -ForegroundColor Green
