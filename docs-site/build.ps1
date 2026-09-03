# Builds the Orleans.Lattice documentation site.
#
#   .\build.ps1                    build into docs-site/_site
#   .\build.ps1 -Serve             build, then serve on http://localhost:8137
#   .\build.ps1 -MaxWarnings 0     fail if DocFX reports more than N warnings
#
# Requires the docfx global tool: dotnet tool install --global docfx
#
# DocFX reports a broken relative link as InvalidFileLink and a broken in-page
# anchor as InvalidBookmark, so the warning count is a link-integrity signal.
# -MaxWarnings turns that into a ratchet: CI passes at the current known-debt
# count, so a NEWLY broken link fails the build while the existing backlog is
# worked down. Lower the ceiling as the backlog shrinks; 0 is the goal.

param(
    [switch]$Serve,
    [int]$MaxWarnings = -1,
    [int]$Port = 8137
)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    & (Join-Path $PSScriptRoot 'stage.ps1')

    $output = & docfx build (Join-Path $PSScriptRoot 'docfx.json') --logLevel warning 2>&1
    $output | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0) { throw "docfx build failed with exit code $LASTEXITCODE" }

    # DocFX resolves inbound README.md links to index.html but emits README.html,
    # so the landing page is published under both names.
    $site = Join-Path $PSScriptRoot '_site'
    $readme = Join-Path $site 'README.html'
    if (Test-Path $readme) { Copy-Item $readme (Join-Path $site 'index.html') -Force }

    $pages = (Get-ChildItem $site -Recurse -Filter *.html | Measure-Object).Count
    $size = '{0:N1} MB' -f ((Get-ChildItem $site -Recurse -File | Measure-Object Length -Sum).Sum / 1MB)
    Write-Host "Site built: $pages pages, $size"

    $warnings = 0
    $summary = $output | Select-String -Pattern '^\s*(\d+)\s+warning\(s\)' | Select-Object -Last 1
    if ($summary) { $warnings = [int]$summary.Matches[0].Groups[1].Value }

    $byType = $output |
        Select-String -Pattern 'warning (\w+):' -AllMatches |
        ForEach-Object { $_.Matches } |
        ForEach-Object { $_.Groups[1].Value } |
        Group-Object | Sort-Object Count -Descending
    foreach ($entry in $byType) { Write-Host ("  {0,4} {1}" -f $entry.Count, $entry.Name) }

    if ($MaxWarnings -ge 0 -and $warnings -gt $MaxWarnings) {
        throw "Documentation link check failed: $warnings warning(s), ceiling is $MaxWarnings. A newly broken link or anchor was introduced - fix it, or lower the ceiling if you have fixed existing ones."
    }
    $ceiling = if ($MaxWarnings -ge 0) { ", ceiling $MaxWarnings" } else { '' }
    Write-Host "Link check: $warnings warning(s)$ceiling"

    if ($Serve) { docfx serve $site --port $Port }
}
finally { Pop-Location }
