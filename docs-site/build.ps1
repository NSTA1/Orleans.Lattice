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

    # Clear previous output so a page that is no longer generated cannot linger.
    # CI always builds from a fresh checkout, so without this a local build can
    # disagree with CI and serve stale pages that no longer exist.
    $site = Join-Path $PSScriptRoot '_site'
    if (Test-Path $site) { Remove-Item $site -Recurse -Force }

    $output = & docfx build (Join-Path $PSScriptRoot 'docfx.json') --logLevel warning 2>&1
    $output | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0) { throw "docfx build failed with exit code $LASTEXITCODE" }

    # DocFX resolves inbound README.md links to index.html but emits README.html,
    # so the landing page is published under both names.
    $readme = Join-Path $site 'README.html'
    if (Test-Path $readme) { Copy-Item $readme (Join-Path $site 'index.html') -Force }

    $pages = (Get-ChildItem $site -Recurse -Filter *.html | Measure-Object).Count
    $size = '{0:N1} MB' -f ((Get-ChildItem $site -Recurse -File | Measure-Object Length -Sum).Sum / 1MB)
    Write-Host "Site built: $pages pages, $size"

    # Match against an ANSI-stripped copy of the output. docfx colours its
    # summary line when it detects a terminal, which CI provides, so there the
    # line arrives as ESC[38;5;11m    2 warning(s)ESC[0m. The '^\s*' anchor
    # below cannot match past that leading escape, so the count silently stayed
    # at its 0 default and -MaxWarnings 0 passed on exactly the runs it exists
    # to gate. The by-type breakdown was never affected because its pattern is
    # unanchored, which is why the log printed the contradiction in plain sight:
    # "2 warning(s)" and "2 InvalidBookmark" above "Link check: 0 warning(s)".
    $ansi = [regex]"$([char]27)\[[0-9;]*m"
    $plain = $output | ForEach-Object { $ansi.Replace([string]$_, '') }

    $summary = $plain | Select-String -Pattern '^\s*(\d+)\s+warning\(s\)' | Select-Object -Last 1
    $warnings = if ($summary) { [int]$summary.Matches[0].Groups[1].Value } else { $null }

    $byType = $plain |
        Select-String -Pattern 'warning (\w+):' -AllMatches |
        ForEach-Object { $_.Matches } |
        ForEach-Object { $_.Groups[1].Value } |
        Group-Object | Sort-Object Count -Descending
    foreach ($entry in $byType) { Write-Host ("  {0,4} {1}" -f $entry.Count, $entry.Name) }

    if ($null -eq $warnings) {
        # Fail closed on a count that was never read. Defaulting to 0 makes an
        # unparsed summary byte-identical in the result to a clean build, which
        # is the precise shape of the defect above: the gate reported a number
        # it had not obtained, and reported it as passing.
        if ($MaxWarnings -ge 0) {
            throw "Documentation link check could not read a warning count: the docfx summary line ('N warning(s)') was not found in the build output, so the ceiling of $MaxWarnings was never applied. This is a failure, not a clean build - a count that defaults to 0 is indistinguishable from one that was read as 0."
        }
        Write-Host 'Link check: warning count unreadable (no ceiling requested)'
    }
    elseif ($MaxWarnings -ge 0 -and $warnings -gt $MaxWarnings) {
        throw "Documentation link check failed: $warnings warning(s), ceiling is $MaxWarnings. A newly broken link or anchor was introduced - fix it, or lower the ceiling if you have fixed existing ones."
    }
    else {
        $ceiling = if ($MaxWarnings -ge 0) { ", ceiling $MaxWarnings" } else { '' }
        Write-Host "Link check: $warnings warning(s)$ceiling"
    }

    if ($Serve) { docfx serve $site --port $Port }
}
finally { Pop-Location }
