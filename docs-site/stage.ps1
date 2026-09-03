# Stages the Orleans.Lattice markdown corpus into a DocFX source tree.
#
# The corpus is not laid out as a website: pages link up to root files, sideways
# into source code, and at directories with no page of their own, and there is no
# navigation anywhere. This script produces a buildable tree without editing a
# single tracked document, so the repository stays the source of truth:
#
#   1. copies the root pages to the site root, so that the corpus's
#      ../../README.md style up-links resolve exactly as they do in the repo;
#   2. copies docs/ underneath, plus the markdown from the directories the corpus
#      links into (samples, benchmark, spec, reference-architecture);
#   3. rewrites every link that does not resolve inside the site to a github.com
#      URL, so no page 404s;
#   4. generates the navigation, grouping packages by the seam PACKAGES.md files
#      them under, so the nav cannot drift as packages are added.
#
# Runs on Windows and Linux; keep it free of platform-specific path literals.

param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path,
    [string]$Staging  = (Join-Path $PSScriptRoot 'src')
)

$ErrorActionPreference = 'Stop'

$separators = [char[]]@('\', '/')
function ConvertTo-SiteRelative([string]$FullPath, [string]$Root) {
    $FullPath.Substring($Root.Length).TrimStart($separators).Replace('\', '/')
}

if (Test-Path $Staging) { Remove-Item $Staging -Recurse -Force }
New-Item -ItemType Directory -Path $Staging -Force | Out-Null

# Root pages, kept at the site root so the corpus's up-links resolve unchanged.
#
# DocFX titles a page "{page title} | {_appTitle}", and these pages carry the
# product name in their own H1 ("Orleans.Lattice Features"), which renders as
# "Orleans.Lattice Features | Orleans.Lattice". Front matter gives each staged
# copy the short title its navigation entry uses, so the browser tab reads
# "Features | Orleans.Lattice". Only the staged copy is affected; the H1 the
# reader sees, and the file in the repository, are untouched.
$rootPageTitles = [ordered]@{
    'README.md'                 = 'Home'
    'FEATURES.md'               = 'Features'
    'PACKAGES.md'               = 'Packages'
    'reference-architecture.md' = 'Reference Architecture'
}

function Set-PageTitle([string]$Text, [string]$Title) {
    # The corpus carries no front matter of its own, so this only ever prepends.
    if ($Text -match '(?s)^\s*---\r?\n') { return $Text }
    $newline = if ($Text.Contains("`r`n")) { "`r`n" } else { "`n" }
    return "---$newline" + "title: $Title$newline" + "---$newline$newline" + $Text
}

foreach ($page in $rootPageTitles.Keys) {
    $text = Set-PageTitle (Get-Content (Join-Path $RepoRoot $page) -Raw) $rootPageTitles[$page]
    Set-Content -Path (Join-Path $Staging $page) -Value $text -NoNewline -Encoding utf8
}

# The docs corpus, preserving the docs/<package>/ layout.
Copy-Item (Join-Path $RepoRoot 'docs') (Join-Path $Staging 'docs') -Recurse

# Directories the corpus links into that carry their own markdown. Sample and
# spec READMEs are real documentation and belong in the site.
foreach ($extra in @('samples', 'benchmark', 'spec', 'reference-architecture')) {
    $source = Join-Path $RepoRoot $extra
    if (-not (Test-Path $source)) { continue }
    $target = Join-Path $Staging $extra
    Get-ChildItem $source -Recurse -Filter *.md | ForEach-Object {
        $destination = Join-Path $target (ConvertTo-SiteRelative $_.FullName $source)
        New-Item -ItemType Directory -Path (Split-Path $destination) -Force | Out-Null
        Copy-Item $_.FullName $destination
    }
}

# Standalone root files the corpus links to.
foreach ($file in @('LICENSE', 'llms.txt')) {
    $source = Join-Path $RepoRoot $file
    if (Test-Path $source) { Copy-Item $source (Join-Path $Staging $file) }
}

# --- Release history, released entries only ---
# The site describes what has shipped. CHANGELOG.md's rolling "## Unreleased"
# section describes work merged to main but not yet published to NuGet, so a
# reader of the site would be told about changes they cannot install. The file
# marks the boundary explicitly ("## Unreleased" ... "## Released"), which is
# what makes this a clean cut rather than a guess about which prose is current.
#
# Only the current release line is published. The archived changelogs
# (CHANGELOG.old.v6/v7/v8.md) are deliberately left in the repository and linked
# to github.com instead: they are a fixed historical record that links into
# living documents, so their cross-references rot as those documents evolve, and
# the only ways to keep them on the site would be to rewrite release history or
# to weaken the link gate. Their links from CHANGELOG.md are rewritten to the
# repository like any other out-of-site target.
function Remove-UnreleasedSection([string]$Text) {
    # Drop the Unreleased heading and its body, up to the next "## " heading.
    return [regex]::Replace($Text, '(?ms)^##[ \t]+Unreleased[ \t]*\r?\n.*?(?=^##[ \t]+\S)', '')
}

$changelog = Join-Path $RepoRoot 'CHANGELOG.md'
if (Test-Path $changelog) {
    $released = Remove-UnreleasedSection (Get-Content $changelog -Raw)

    # Fail loudly rather than silently publishing unreleased notes: a changelog
    # whose heading text drifts would otherwise slip the whole section onto the
    # public site with no signal.
    if ($released -match '(?m)^##[ \t]+Unreleased[ \t]*$') {
        throw 'CHANGELOG.md still contains an Unreleased section after filtering - the heading shape changed, and unreleased notes would have been published.'
    }

    Set-Content -Path (Join-Path $Staging 'CHANGELOG.md') -Value (Set-PageTitle $released 'Changelog') -NoNewline -Encoding utf8
}

# Site branding. The Explorer's favicon is the project's existing mark - a B+
# tree root branching to two internal nodes and four leaves, on .NET purple - so
# the site reuses it rather than DocFX's stock "D" placeholder. That asset is
# authored at 456x456 for an app icon and the navbar applies no size constraint
# to the logo, so the intrinsic width/height are rewritten here to the ~38px the
# template expects. The viewBox is preserved, so the art scales rather than crops.
$logoSource = Join-Path (Join-Path $RepoRoot 'src') 'lattice.explorer/UI/wwwroot/favicon.svg'
if (Test-Path $logoSource) {
    $svg = [regex]::Replace(
        (Get-Content $logoSource -Raw),
        '(<svg\b[^>]*?)\bwidth="[^"]*"\s+height="[^"]*"',
        '$1width="38" height="38"',
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    Set-Content -Path (Join-Path $Staging 'lattice-logo.svg') -Value $svg -NoNewline -Encoding utf8
}

# --- Rewrite every link that does not resolve inside the site to github.com ---
# Resolution-based rather than pattern-based: any relative target absent from the
# staged tree has no site counterpart (source code, spec files, agent
# instructions), as does a link to a directory or to a staged non-markdown file.
# Anything that does resolve to a page is left untouched.
$blobBase = 'https://github.com/NSTA1/Orleans.Lattice/blob/main'
$treeBase = 'https://github.com/NSTA1/Orleans.Lattice/tree/main'
$rewritten = 0

Get-ChildItem $Staging -Recurse -Filter *.md | ForEach-Object {
    $file = $_
    $text = Get-Content $file.FullName -Raw
    $original = $text

    $text = [regex]::Replace($text, '\]\(([^)\s#][^)\s]*?)(#[^)\s]*)?\)', {
        param($match)
        $target = $match.Groups[1].Value
        $anchor = $match.Groups[2].Value

        # Leave absolute URLs, protocol-relative links, and mailto alone.
        if ($target -match '^([a-z][a-z0-9+.-]*:|//)') { return $match.Value }

        $resolved = Join-Path $file.DirectoryName $target
        if (Test-Path -LiteralPath $resolved) {
            $item = Get-Item -LiteralPath $resolved
            if ($item.PSIsContainer) {
                # A directory has no page of its own; prefer its README.
                if (Test-Path -LiteralPath (Join-Path $item.FullName 'README.md')) {
                    $script:rewritten++
                    return "]($($target.TrimEnd('/'))/README.md$anchor)"
                }
            }
            elseif ($item.Extension -ne '.md') {
                # A staged non-markdown file (LICENSE, llms.txt) has no page either.
                $script:rewritten++
                return "]($blobBase/$(ConvertTo-SiteRelative $item.FullName $Staging)$anchor)"
            }
            else { return $match.Value }
        }

        # Absent from the site: point at the same path in the repository.
        $full = [System.IO.Path]::GetFullPath($resolved)
        if (-not $full.StartsWith($Staging)) { return $match.Value }

        $repoRelative = ConvertTo-SiteRelative $full $Staging
        $script:rewritten++
        $base = if ([System.IO.Path]::GetExtension($repoRelative)) { $blobBase } else { $treeBase }
        return "]($base/$repoRelative$anchor)"
    })

    if ($text -ne $original) { Set-Content -Path $file.FullName -Value $text -NoNewline -Encoding utf8 }
}
Write-Host "Rewrote $rewritten out-of-site link(s) to github.com URLs"

# --- A TOC per package directory, driving that package's sidebar ---
$docsRoot = Join-Path $Staging 'docs'
$packageDirs = Get-ChildItem $docsRoot -Directory | Sort-Object Name

foreach ($dir in $packageDirs) {
    $lines = New-Object System.Collections.Generic.List[string]
    # README first, then alphabetical.
    $files = Get-ChildItem $dir.FullName -Filter *.md |
        Sort-Object { if ($_.Name -eq 'README.md') { '0' } else { '1' + $_.Name } }
    foreach ($f in $files) {
        $title = if ($f.Name -eq 'README.md') {
            'Overview'
        } else {
            (Get-Culture).TextInfo.ToTitleCase(($f.BaseName -replace '-', ' '))
        }
        $lines.Add("- name: $title")
        $lines.Add("  href: $($f.Name)")
    }
    Set-Content -Path (Join-Path $dir.FullName 'toc.yml') -Value $lines -Encoding utf8
}

# --- Group directories by the section they are catalogued under ---
# PACKAGES.md groups every package by the seam it fills, and FEATURES.md groups
# every capability by concern with a link to its sample. Both groupings are
# parsed from those files rather than hand-maintained here, so a package or
# sample added to a catalogue is grouped in the navigation automatically.
function Get-SectionMap {
    param(
        [string]$Path,
        [string]$LinkPattern,
        [string[]]$ExcludeHeadings = @()
    )

    $map = @{}
    $order = New-Object System.Collections.Generic.List[string]
    $current = $null

    foreach ($line in (Get-Content $Path)) {
        if ($line -match '^##\s+(.+?)\s*$') {
            $heading = $Matches[1]
            if ($ExcludeHeadings -contains $heading) { $current = $null; continue }
            $current = ($heading -replace '\s*\(in progress\)\s*$', '')
            if (-not $order.Contains($current)) { $order.Add($current) }
            continue
        }
        if (-not $current) { continue }
        foreach ($m in [regex]::Matches($line, $LinkPattern)) {
            $key = $m.Groups[1].Value
            if (-not $map.ContainsKey($key)) { $map[$key] = $current }
        }
    }

    return @{ Map = $map; Order = $order }
}

# Assigns every directory a section, appending a catch-all for anything the
# catalogue does not mention so nothing is dropped from the navigation.
function Add-FallbackSection {
    param($SectionMap, $Directories, [string]$Fallback)

    foreach ($dir in $Directories) {
        if (-not $SectionMap.Map.ContainsKey($dir.Name)) { $SectionMap.Map[$dir.Name] = $Fallback }
    }
    if ($Directories.Name | Where-Object { $SectionMap.Map[$_] -eq $Fallback }) {
        if (-not $SectionMap.Order.Contains($Fallback)) { $SectionMap.Order.Add($Fallback) }
    }
    return $SectionMap
}

$packageSections = Get-SectionMap `
    -Path (Join-Path $RepoRoot 'PACKAGES.md') `
    -LinkPattern '\]\(docs/([^/)]+)/' `
    -ExcludeHeadings @('Contents', 'Related')

# docs/crdt is a docs-only conceptual topic with no src/ counterpart, so it is
# absent from PACKAGES.md and lands in the catch-all.
$packageSections = Add-FallbackSection $packageSections $packageDirs 'Concepts'
$sectionOf = $packageSections.Map
$sectionOrder = $packageSections.Order

# --- Top-level docs TOC: grouped sections, each holding its package TOCs ---
$docsToc = New-Object System.Collections.Generic.List[string]
$docsToc.Add('- name: Overview')
$docsToc.Add('  href: index.md')
foreach ($section in $sectionOrder) {
    $members = $packageDirs | Where-Object { $sectionOf[$_.Name] -eq $section }
    if (-not $members) { continue }
    $docsToc.Add("- name: $section")
    $docsToc.Add('  items:')
    foreach ($dir in $members) {
        $docsToc.Add("  - name: $($dir.Name)")
        $docsToc.Add("    href: $($dir.Name)/toc.yml")
    }
}
if (Test-Path (Join-Path $docsRoot 'RELEASING.md')) {
    $docsToc.Add('- name: Releasing')
    $docsToc.Add('  href: RELEASING.md')
}
Set-Content -Path (Join-Path $docsRoot 'toc.yml') -Value $docsToc -Encoding utf8

# --- Documentation landing page, grouped the same way as the sidebar ---
$index = New-Object System.Collections.Generic.List[string]
$index.Add('# Documentation')
$index.Add('')
$index.Add('Every package''s documentation, grouped by the seam it fills. See the')
$index.Add('[package inventory](../PACKAGES.md) and the')
$index.Add('[capability catalogue](../FEATURES.md).')
$index.Add('')
foreach ($section in $sectionOrder) {
    $members = $packageDirs | Where-Object { $sectionOf[$_.Name] -eq $section }
    if (-not $members) { continue }
    $index.Add("## $section")
    $index.Add('')
    foreach ($dir in $members) {
        $landing = if (Test-Path (Join-Path $dir.FullName 'README.md')) {
            "$($dir.Name)/README.md"
        } else {
            $first = Get-ChildItem $dir.FullName -Filter *.md | Sort-Object Name | Select-Object -First 1
            if ($first) { "$($dir.Name)/$($first.Name)" } else { $null }
        }
        if ($landing) {
            $count = (Get-ChildItem $dir.FullName -Filter *.md | Measure-Object).Count
            $suffix = if ($count -eq 1) { '1 document' } else { "$count documents" }
            $index.Add("- [$($dir.Name)]($landing) - $suffix")
        }
    }
    $index.Add('')
}
Set-Content -Path (Join-Path $docsRoot 'index.md') -Value $index -Encoding utf8

# --- Sample sources, rendered as pages ---
# On github.com a sample folder is browsable, so Program.cs is one click from the
# sample's README. Nothing equivalent exists on a docs site, so each sample gets a
# generated "Source" page that inlines its files. Two samples
# (MultiSiteManufacturing, VehicleFleetSimulator) hold the bulk of the sample
# code, so the listing is capped and the remainder is linked to the repository -
# a 600 KB page helps nobody.
$sampleSourceExtensions = @('.cs', '.razor', '.proto', '.csproj', '.json', '.yml', '.yaml', '.ps1')
$sampleLanguage = @{
    '.cs' = 'csharp'; '.razor' = 'razor'; '.proto' = 'protobuf'; '.csproj' = 'xml'
    '.json' = 'json'; '.yml' = 'yaml'; '.yaml' = 'yaml'; '.ps1' = 'powershell'
}
$maxSampleFiles = 20
$maxSampleBytes = 200KB

$samplesSource = Join-Path $RepoRoot 'samples'
$samplesStaged = Join-Path $Staging 'samples'
$sampleDirs = @()

if (Test-Path $samplesSource) {
    $sampleDirs = Get-ChildItem $samplesSource -Directory | Sort-Object Name
}

foreach ($sample in $sampleDirs) {
    $sources = Get-ChildItem $sample.FullName -Recurse -File |
        Where-Object {
            $sampleSourceExtensions -contains $_.Extension -and
            $_.FullName -notmatch '[\\/](bin|obj)[\\/]'
        } |
        # Entry points first, then by path, so Program.cs leads.
        Sort-Object @{ Expression = { if ($_.Name -eq 'Program.cs') { 0 } else { 1 } } },
                    @{ Expression = { (ConvertTo-SiteRelative $_.FullName $sample.FullName) } }

    if (-not $sources) { continue }

    $page = New-Object System.Collections.Generic.List[string]
    $page.Add("# $($sample.Name) source")
    $page.Add('')
    $page.Add("The source of the [$($sample.Name)]($treeBase/samples/$($sample.Name)) sample.")
    $page.Add('')

    $emitted = 0
    $bytes = 0
    $skipped = New-Object System.Collections.Generic.List[string]

    foreach ($source in $sources) {
        $relative = ConvertTo-SiteRelative $source.FullName $sample.FullName
        if ($emitted -ge $maxSampleFiles -or ($bytes + $source.Length) -gt $maxSampleBytes) {
            $skipped.Add($relative)
            continue
        }

        $language = $sampleLanguage[$source.Extension]
        if (-not $language) { $language = 'text' }

        $page.Add("## $relative")
        $page.Add('')
        # A four-backtick fence so a source file containing a triple fence cannot
        # terminate the block early.
        $page.Add('````' + $language)
        $page.Add(((Get-Content $source.FullName -Raw) -replace '\s+$', ''))
        $page.Add('````')
        $page.Add('')

        $emitted++
        $bytes += $source.Length
    }

    if ($skipped.Count -gt 0) {
        $page.Add('## Remaining files')
        $page.Add('')
        $page.Add("This sample is too large to inline in full. The remaining $($skipped.Count) file(s) are in the repository:")
        $page.Add('')
        foreach ($relative in $skipped) {
            $page.Add("- [$relative]($blobBase/samples/$($sample.Name)/$relative)")
        }
        $page.Add('')
    }

    $target = Join-Path $samplesStaged $sample.Name
    New-Item -ItemType Directory -Path $target -Force | Out-Null
    Set-Content -Path (Join-Path $target 'source.md') -Value $page -Encoding utf8
}

# --- Samples index and TOC, grouped by concern from FEATURES.md ---
if ($sampleDirs) {
    $sampleSections = Get-SectionMap `
        -Path (Join-Path $RepoRoot 'FEATURES.md') `
        -LinkPattern '\]\(samples/([^/)]+)[/)]' `
        -ExcludeHeadings @('Contents', 'Related')
    $sampleSections = Add-FallbackSection $sampleSections $sampleDirs 'Other samples'

    $samplesToc = New-Object System.Collections.Generic.List[string]
    $samplesIndex = New-Object System.Collections.Generic.List[string]
    $samplesToc.Add('- name: Overview')
    $samplesToc.Add('  href: index.md')
    $samplesIndex.Add('# Samples')
    $samplesIndex.Add('')
    $samplesIndex.Add('Runnable projects exercising the platform, grouped by concern as in the')
    $samplesIndex.Add('[capability catalogue](../FEATURES.md). Each sample''s own README explains what')
    $samplesIndex.Add('it demonstrates and how to run it; its Source page lists the code.')
    $samplesIndex.Add('')

    foreach ($section in $sampleSections.Order) {
        $members = $sampleDirs | Where-Object { $sampleSections.Map[$_.Name] -eq $section }
        if (-not $members) { continue }

        $tocEntries = New-Object System.Collections.Generic.List[string]
        $indexEntries = New-Object System.Collections.Generic.List[string]

        foreach ($sample in $members) {
            $staged = Join-Path $samplesStaged $sample.Name
            $hasReadme = Test-Path (Join-Path $staged 'README.md')
            $hasSource = Test-Path (Join-Path $staged 'source.md')
            if (-not $hasReadme -and -not $hasSource) { continue }

            $tocEntries.Add("  - name: $($sample.Name)")
            $tocEntries.Add('    items:')
            if ($hasReadme) {
                $tocEntries.Add('    - name: Overview')
                $tocEntries.Add("      href: $($sample.Name)/README.md")
            }
            if ($hasSource) {
                $tocEntries.Add('    - name: Source')
                $tocEntries.Add("      href: $($sample.Name)/source.md")
            }

            $landing = if ($hasReadme) { "$($sample.Name)/README.md" } else { "$($sample.Name)/source.md" }
            $indexEntries.Add("- [$($sample.Name)]($landing)")
        }
        if ($tocEntries.Count -eq 0) { continue }

        $samplesToc.Add("- name: $section")
        $samplesToc.Add('  items:')
        foreach ($entry in $tocEntries) { $samplesToc.Add($entry) }

        $samplesIndex.Add("## $section")
        $samplesIndex.Add('')
        foreach ($entry in $indexEntries) { $samplesIndex.Add($entry) }
        $samplesIndex.Add('')
    }

    Set-Content -Path (Join-Path $samplesStaged 'toc.yml') -Value $samplesToc -Encoding utf8
    Set-Content -Path (Join-Path $samplesStaged 'index.md') -Value $samplesIndex -Encoding utf8
}

# --- Site root TOC, which becomes the navbar ---
# "href: docs/" is a FOLDER reference, so DocFX treats docs/toc.yml as a separate
# navigation scope that drives the sidebar. Pointing at "docs/toc.yml" instead
# would MERGE all 47 package TOCs into the root TOC, and the modern template
# renders a root node with children as a navbar dropdown - producing a single
# dropdown taller than the viewport.
$rootToc = @(
    '- name: Home',
    '  href: README.md',
    '- name: Documentation',
    '  href: docs/',
    '- name: Samples',
    '  href: samples/',
    '- name: Features',
    '  href: FEATURES.md',
    '- name: Packages',
    '  href: PACKAGES.md',
    '- name: Reference Architecture',
    '  href: reference-architecture.md',
    '- name: Changelog',
    '  href: CHANGELOG.md'
)
Set-Content -Path (Join-Path $Staging 'toc.yml') -Value $rootToc -Encoding utf8

$mdCount = (Get-ChildItem $Staging -Recurse -Filter *.md | Measure-Object).Count
$tocCount = (Get-ChildItem $Staging -Recurse -Filter toc.yml | Measure-Object).Count
Write-Host "Staged $mdCount markdown files and generated $tocCount TOC files under $Staging"
