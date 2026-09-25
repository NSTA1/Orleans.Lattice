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
#
# After DocFX, the build publishes each page's markdown alternate beside it and
# links the page to it, then fails unless every page has one, every page's
# source link names a file in the repository, and llms.txt, sitemap.xml and the
# footer's docs version are all in place (see stage.ps1 for what they are).

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

    # The home page is authored (docs-site/pages/index.md) and builds to
    # index.html itself. Should it ever be missing, fall back to publishing the
    # README as the landing page, so the site root never 404s.
    $readme = Join-Path $site 'README.html'
    $landing = Join-Path $site 'index.html'
    if ((Test-Path $readme) -and -not (Test-Path $landing)) { Copy-Item $readme $landing }

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

    # --- The site's surface for agents and LLM tooling ---
    # stage.ps1 writes every page's markdown alternate to obj/agent. Each is
    # published beside the page it renders, and the page's head points at it
    # (rel="alternate", type="text/markdown"), so a reader that fetches
    # page.html can find page.md without guessing.
    $agent = Join-Path $PSScriptRoot 'obj/agent'
    if (-not (Test-Path $agent)) { throw "No markdown alternates at $agent; stage.ps1 writes them, so it did not run to completion." }
    $utf8 = New-Object System.Text.UTF8Encoding $false
    foreach ($markdown in Get-ChildItem $agent -Recurse -File) {
        $target = Join-Path $site $markdown.FullName.Substring($agent.Length).TrimStart([char[]]@('\', '/'))
        New-Item -ItemType Directory -Path (Split-Path $target) -Force | Out-Null
        Copy-Item -LiteralPath $markdown.FullName -Destination $target -Force
    }

    $unpaired = New-Object System.Collections.Generic.List[string]
    $badSource = New-Object System.Collections.Generic.List[string]
    $pagesWithAlternate = 0
    foreach ($html in Get-ChildItem $site -Recurse -Filter *.html) {
        $relative = $html.FullName.Substring($site.Length).TrimStart([char[]]@('\', '/')).Replace('\', '/')
        if ($html.Name -eq 'toc.html' -or $relative.StartsWith('public/')) { continue }
        $text = [System.IO.File]::ReadAllText($html.FullName)

        # Every page's source link must name a file in the repository, never the
        # staged copy DocFX built it from.
        $docurl = [regex]::Match($text, '<meta name="docfx:docurl" content="(?<url>[^"]*)"')
        if (-not $docurl.Success -or $docurl.Groups['url'].Value -match '/docs-site/(src|_site)/') { $badSource.Add($relative) }

        $alternate = [System.IO.Path]::ChangeExtension($html.FullName, '.md')
        $head = $text.IndexOf('</head>')
        if (-not (Test-Path -LiteralPath $alternate) -or $head -lt 0) { $unpaired.Add($relative); continue }
        if (-not $text.Contains('type="text/markdown"')) {
            $link = "<link rel=`"alternate`" type=`"text/markdown`" href=`"$([System.IO.Path]::GetFileName($alternate))`" title=`"This page as markdown`">`n  "
            [System.IO.File]::WriteAllText($html.FullName, $text.Insert($head, $link), $utf8)
        }
        $pagesWithAlternate++
    }
    if ($unpaired.Count -gt 0) {
        throw "$($unpaired.Count) page(s) have no markdown alternate: $(($unpaired | Select-Object -First 10) -join ', '). stage.ps1 writes one for every staged page, so a page it did not stage reached the site."
    }
    if ($badSource.Count -gt 0) {
        throw "$($badSource.Count) page(s) have no source link, or one into the staged copy: $(($badSource | Select-Object -First 10) -join ', '). stage.ps1 sets each page's docurl from where it was staged from."
    }
    Write-Host "Markdown alternates: $pagesWithAlternate page(s) linked to their .md"

    # llms.txt, the sitemap, and the footer's version line are what an agent
    # reads first; the build fails without them rather than publishing a site
    # that quietly lacks its entry point.
    $siteUrl = [string](Get-Content (Join-Path $PSScriptRoot 'docfx.json') -Raw | ConvertFrom-Json).build.sitemap.baseUrl
    $llms = Join-Path $site 'llms.txt'
    if (-not (Test-Path $llms)) { throw 'The site has no llms.txt; stage.ps1 generates it and docfx.json publishes it as a resource.' }
    $deadLinks = @(foreach ($m in [regex]::Matches([System.IO.File]::ReadAllText($llms), '\]\((?<url>[^)\s]+)\)')) {
        $url = $m.Groups['url'].Value
        if (-not $url.StartsWith($siteUrl)) { continue }
        $path = [System.Uri]::UnescapeDataString(($url.Substring($siteUrl.Length) -split '#')[0])
        if (-not (Test-Path -LiteralPath (Join-Path $site $path))) { $url }
    })
    if ($deadLinks.Count -gt 0) { throw "llms.txt links to $($deadLinks.Count) address(es) the site does not have: $(($deadLinks | Select-Object -First 10) -join ', ')" }
    $sitemap = Join-Path $site 'sitemap.xml'
    if (-not (Test-Path $sitemap) -or -not ([System.IO.File]::ReadAllText($sitemap)).Contains("<loc>${siteUrl}index.html</loc>")) {
        throw "The site has no sitemap.xml listing ${siteUrl}index.html; docfx.json's build.sitemap produces it."
    }
    if (-not ([System.IO.File]::ReadAllText($landing)).Contains('class="lt-footer-version"')) {
        throw "The home page's footer has no docs version. stage.ps1 writes it to obj/site-metadata.json, which docfx.json lists in globalMetadataFiles."
    }
    Write-Host "Agent entry points: llms.txt, llms-full.txt, sitemap.xml"

    if ($Serve) { docfx serve $site --port $Port }
}
finally { Pop-Location }
