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
#      links into (samples, spec, reference-architecture). benchmark/ is left
#      out: its markdown is internal rig notes, so step 3 sends links to it to
#      github.com instead;
#   3. rewrites every link that does not resolve inside the site to a github.com
#      URL, so no page 404s;
#   4. generates the navigation, grouping packages by the seam PACKAGES.md files
#      them under, so the nav cannot drift as packages are added;
#   5. turns docs/videos into the Videos section, playing each published
#      episode from docs-site/media and leaving unpublished ones out;
#   6. stages the site's own authored pages (docs-site/pages: the home page)
#      AFTER step 3, so a broken link in them is never rewritten away and fails
#      the zero-warning link gate instead;
#   7. splits any page too large to read in a few fetches - the API and
#      configuration references, the metrics catalogue, the release history -
#      into an index and a page per section (lib/split.ps1);
#   8. gives every page a source link to the file it came from, and the site a
#      visible version: what it documents, from which ref, and when it was built;
#   9. writes the site's machine-readable surface for agents and LLM tooling: a
#      markdown alternate of every page (lib/agent.ps1), llms.txt generated from
#      the same catalogue as the documentation map, llms-full.txt, and each
#      package's pages in one file of their own.
#
# Runs on Windows and Linux; keep it free of platform-specific path literals.

param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path,
    [string]$Staging  = (Join-Path $PSScriptRoot 'src'),
    # Build intermediates that are not DocFX input: the markdown alternates,
    # which build.ps1 copies beside the rendered pages, and the footer metadata.
    [string]$Intermediate = (Join-Path $PSScriptRoot 'obj')
)

$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'lib/markdown.ps1')
. (Join-Path $PSScriptRoot 'lib/split.ps1')
. (Join-Path $PSScriptRoot 'lib/agent.ps1')

$separators = [char[]]@('\', '/')
function ConvertTo-SiteRelative([string]$FullPath, [string]$Root) {
    $FullPath.Substring($Root.Length).TrimStart($separators).Replace('\', '/')
}

if (Test-Path $Staging) { Remove-Item $Staging -Recurse -Force }
New-Item -ItemType Directory -Path $Staging -Force | Out-Null
if (Test-Path $Intermediate) { Remove-Item $Intermediate -Recurse -Force }
New-Item -ItemType Directory -Path $Intermediate -Force | Out-Null

# --- What this build documents ---
# The site is published from a release line, and a reader - or an agent - has no
# other way to tell which release a page describes, so the build records it once
# here: the ref and commit it was built from, the release that ref belongs to,
# and when. Source links point at the same ref, so a page's source is the text
# that was published, not whatever main holds now.
#
# In CI the ref comes from the event: a lattice-v<X.Y.Z> tag push, a dispatch on
# release/<X.Y>, or a pull request's head branch. Anywhere else it is the
# checkout's own branch or tag. Tags are read from origin because the CI
# checkout is shallow, and from the local clone when origin is unreachable.
function Invoke-GitText([string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $prompt = $env:GIT_TERMINAL_PROMPT
    $env:GIT_TERMINAL_PROMPT = '0'
    try {
        $output = & git -C $RepoRoot @Arguments 2>$null
        if ($LASTEXITCODE -ne 0) { return @() }
        return @($output | Where-Object { $_ })
    }
    catch { return @() }
    finally {
        $ErrorActionPreference = $previous
        $env:GIT_TERMINAL_PROMPT = $prompt
    }
}

$docfxConfig = Get-Content (Join-Path $PSScriptRoot 'docfx.json') -Raw | ConvertFrom-Json
$siteUrl = [string]$docfxConfig.build.sitemap.baseUrl
if (-not $siteUrl) { throw 'docfx.json has no build.sitemap.baseUrl; it is the published site URL, and llms.txt and the markdown alternates link from it.' }
if (-not $siteUrl.EndsWith('/')) { $siteUrl += '/' }
$repositoryUrl = 'https://github.com/NSTA1/Orleans.Lattice'

$commit = Invoke-GitText @('rev-parse', 'HEAD') | Select-Object -First 1
$ref = $null
if ($env:GITHUB_ACTIONS -eq 'true') {
    if ($env:GITHUB_HEAD_REF) { $ref = $env:GITHUB_HEAD_REF }
    elseif ($env:GITHUB_REF_NAME) { $ref = $env:GITHUB_REF_NAME }
}
if (-not $ref) { $ref = Invoke-GitText @('symbolic-ref', '--short', '-q', 'HEAD') | Select-Object -First 1 }
if (-not $ref) { $ref = Invoke-GitText @('describe', '--tags', '--exact-match', 'HEAD') | Select-Object -First 1 }
if (-not $ref) { $ref = if ($commit) { $commit } else { 'main' } }

$tagNames = @(Invoke-GitText @('-c', 'credential.helper=', '-c', 'http.lowSpeedLimit=1000', '-c', 'http.lowSpeedTime=20', 'ls-remote', '--tags', '--refs', 'origin') |
    ForEach-Object { ($_ -split "`t")[-1] -replace '^refs/tags/', '' })
if ($tagNames.Count -eq 0) { $tagNames = @(Invoke-GitText @('tag', '--list')) }

# The newest published version of each package, keyed by its tag stem
# ("lattice.replication" for Orleans.Lattice.Replication). Every per-package tag
# push publishes that version to NuGet (docs/RELEASING.md), so this is what the
# NuGet badges on PACKAGES.md show, written as text.
$packageVersions = @{}
foreach ($name in $tagNames) {
    if ($name -notmatch '^(?<stem>[a-z0-9.]+)-v(?<version>\d+\.\d+\.\d+)$') { continue }
    $version = [version]$Matches.version
    if (-not $packageVersions.ContainsKey($Matches.stem) -or $packageVersions[$Matches.stem] -lt $version) {
        $packageVersions[$Matches.stem] = $version
    }
}
function Get-PackageVersion([string]$PackageId) {
    $stem = ($PackageId -replace '^Orleans\.', '').ToLowerInvariant()
    if ($packageVersions.ContainsKey($stem)) { return [string]$packageVersions[$stem] }
    return $null
}

$releaseLine = $null
$release = $null
if ($ref -match '^lattice-v(?<line>\d+\.\d+)\.\d+$') {
    $releaseLine = $Matches.line
    $release = $ref.Substring('lattice-v'.Length)
}
elseif ($ref -match '^release/(?<line>\d+\.\d+)$') {
    $releaseLine = $Matches.line
    $newest = $tagNames |
        Where-Object { $_ -match "^lattice-v$([regex]::Escape($releaseLine))\.\d+$" } |
        ForEach-Object { [version]($_.Substring('lattice-v'.Length)) } |
        Sort-Object -Descending | Select-Object -First 1
    if ($newest) { $release = [string]$newest }
}
$built = [DateTime]::UtcNow
$site = [pscustomobject]@{
    Url         = $siteUrl
    Repository  = $repositoryUrl
    Ref         = $ref
    Commit      = $commit
    ShortCommit = if ($commit) { $commit.Substring(0, [Math]::Min(9, $commit.Length)) } else { $null }
    Line        = $releaseLine
    Release     = $release
    Built       = $built
    BuiltDate   = $built.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    # What a reader is told the site documents, in one phrase.
    Label       = if ($release) { "Orleans.Lattice $release (release line $releaseLine)" }
                  elseif ($releaseLine) { "the Orleans.Lattice $releaseLine release line" }
                  else { "unreleased work on $ref" }
}
Write-Host "Documenting $($site.Label), from $($site.Ref) at $($site.ShortCommit), built $($site.BuiltDate)"

# The version of each package the site documents: its newest tag on the release
# line the site is built from, or on an earlier line, never on a later one. A
# package tagged on the next line before the core tag moves the site there is
# not yet what these pages describe. Off a release line, the newest tag.
$documentedVersions = @{}
$lineCeiling = if ($releaseLine) { [version]$releaseLine } else { $null }
foreach ($name in $tagNames) {
    if ($name -notmatch '^(?<stem>[a-z0-9.]+)-v(?<version>\d+\.\d+\.\d+)$') { continue }
    $version = [version]$Matches.version
    if ($lineCeiling -and [version]"$($version.Major).$($version.Minor)" -gt $lineCeiling) { continue }
    if (-not $documentedVersions.ContainsKey($Matches.stem) -or $documentedVersions[$Matches.stem] -lt $version) {
        $documentedVersions[$Matches.stem] = $version
    }
}
function Get-DocumentedVersion([string]$PackageId) {
    $stem = ($PackageId -replace '^Orleans\.', '').ToLowerInvariant()
    if ($documentedVersions.ContainsKey($stem)) { return [string]$documentedVersions[$stem] }
    return $null
}

# Links into the repository point at the ref the site was built from.
function Get-SourceUrl([string]$RepoPath, [int]$FromLine = 0, [int]$ToLine = 0) {
    $url = "$repositoryUrl/blob/$($site.Ref)/$RepoPath"
    if ($FromLine -gt 0) { $url += "?plain=1#L$FromLine" + $(if ($ToLine -gt $FromLine) { "-L$ToLine" } else { '' }) }
    return $url
}
function Get-TreeUrl([string]$RepoPath) { return "$repositoryUrl/tree/$($site.Ref)/$RepoPath" }

# Where each staged page came from, keyed by its site-relative path: the URL of
# its source, written onto the page as DocFX's docurl at the end. DocFX would
# otherwise derive it from the staged copy's own path, docs-site/src/..., which
# is not a file in the repository.
$origins = @{}

# Text that a reader of the page's source sees and a sighted reader does not.
# The generated lists set each label in its own box - a name above its count,
# a title beside its description - so the layout separates them; take the boxes
# away, as any tool that reads a page as text does, and they run together
# ("BuildWriting code", "50 documentsThe core"). A visually hidden separator
# keeps them apart without moving a pixel: it is out of flow, so a flex or grid
# container gives it no cell and no gap.
function Get-Separator([string]$Text) { return "<span class=`"visually-hidden`">$Text</span>" }

# A status pill ("unreleased", "in progress"), which reads as a parenthesis.
# Built by concatenation: a parenthesis inside a quoted argument of a $( )
# subexpression ends the subexpression early when it sits inside a string.
function Get-StatusBadge([string]$Status) {
    return '<span class="lt-status">' + (Get-Separator ' (') + $Status + (Get-Separator ')') + '</span>'
}

# Root pages, kept at the site root so the corpus's up-links resolve unchanged.
#
# DocFX titles a page "{page title} | {_appTitle}", and these pages carry the
# product name in their own H1 ("Orleans.Lattice Features"), which renders as
# "Orleans.Lattice Features | Orleans.Lattice". Front matter gives each staged
# copy the short title its navigation entry uses, so the browser tab reads
# "Features | Orleans.Lattice". Only the staged copy is affected; the H1 the
# reader sees, and the file in the repository, are untouched. The README is the
# platform overview; the site's home page is authored under docs-site/pages.
$rootPageTitles = [ordered]@{
    'README.md'                 = 'Overview'
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
    $origins[$page] = Get-SourceUrl $page
}

# --- Package versions as text ---
# PACKAGES.md (and the README's header) show each published package's version as
# a shields.io badge whose only text is its alt, "NuGet", so a reader that does
# not render images - a screen reader, a text browser, an agent - learns nothing
# from it. The staged copy's alt text carries the version the badge draws, read
# from the package's newest tag (see $packageVersions above). A badge whose
# package has no tag is left as it is. The text is fixed when the site is built
# while the image stays live, so the two differ only after a wave that pushes no
# core tag, which does not redeploy the site (docs/RELEASING.md).
$badgePattern = [regex]'\[!\[NuGet\]\((?<image>https://img\.shields\.io/nuget/v/(?<id>[A-Za-z0-9.]+))\)\]\((?<link>https://www\.nuget\.org/packages/\k<id>/?)\)'
$badgesVersioned = 0
foreach ($page in @('README.md', 'PACKAGES.md')) {
    $file = Join-Path $Staging $page
    $text = Get-Content $file -Raw
    $text = $badgePattern.Replace($text, {
        param($match)
        $version = Get-PackageVersion $match.Groups['id'].Value
        if (-not $version) { return $match.Value }
        $script:badgesVersioned++
        return "[![NuGet $version]($($match.Groups['image'].Value))]($($match.Groups['link'].Value))"
    })
    Set-Content -Path $file -Value $text -NoNewline -Encoding utf8
}
Write-Host "Wrote the version onto $badgesVersioned NuGet badge(s)"

# The docs corpus, preserving the docs/<package>/ layout.
Copy-Item (Join-Path $RepoRoot 'docs') (Join-Path $Staging 'docs') -Recurse
Get-ChildItem (Join-Path $Staging 'docs') -Recurse -Filter *.md | ForEach-Object {
    $relative = ConvertTo-SiteRelative $_.FullName $Staging
    $origins[$relative] = Get-SourceUrl $relative
}

# Directories the corpus links into that carry their own markdown. Sample and
# spec READMEs are real documentation and belong in the site. benchmark/ is
# deliberately absent: its markdown is internal notes for operating the rigs,
# not user documentation, so it must not be published or searchable. Links to
# it are rewritten to github.com like any other target outside the site.
foreach ($extra in @('samples', 'spec', 'reference-architecture')) {
    $source = Join-Path $RepoRoot $extra
    if (-not (Test-Path $source)) { continue }
    $target = Join-Path $Staging $extra
    Get-ChildItem $source -Recurse -Filter *.md | ForEach-Object {
        $destination = Join-Path $target (ConvertTo-SiteRelative $_.FullName $source)
        New-Item -ItemType Directory -Path (Split-Path $destination) -Force | Out-Null
        Copy-Item $_.FullName $destination
        $relative = "$extra/$(ConvertTo-SiteRelative $_.FullName $source)"
        $origins[$relative] = Get-SourceUrl $relative
    }
}

# Standalone root files the corpus links to. The repository's llms.txt is not
# among them: the site writes its own, generated from the documentation map at
# the end of this script, and links to llms.txt resolve to that.
foreach ($file in @('LICENSE')) {
    $source = Join-Path $RepoRoot $file
    if (Test-Path $source) { Copy-Item $source (Join-Path $Staging $file) }
}
$siteGenerated = @('llms.txt', 'llms-full.txt')

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
    $origins['CHANGELOG.md'] = Get-SourceUrl 'CHANGELOG.md'
}

# Site branding - the mark, favicon, fonts, and theme - lives in the DocFX
# template (docs-site/template/public), which DocFX copies into the output.

# --- Videos: the companion pages ---
# docs/videos/<slug>.md is an episode's companion page - its transcript and the
# code it shows - written by the videos/ workspace. Each carries exactly one
# generated block,
#
#   <!-- video:begin episode="<slug>" path="<path>" order="<n>" length="<m:ss>" cut="<hex>" -->
#   ...a note for github.com readers...
#   <!-- video:end -->
#
# which is replaced here, from begin to end inclusive, by the player when
# docs-site/media holds the cut's three files (<slug>-<cut>.mp4, .vtt and .jpg),
# and removed otherwise. An episode is published only when it has a cut AND its
# media is present, and a page whose episode is not published is left out of the
# site altogether. That happens here, BEFORE the link rewrite below, so a link to
# such a page falls back to the repository instead of reaching a page with no
# video.
#
# The media is copied into the staged site once, under media/, and every page
# that plays an episode uses that copy. docfx.json lists media/** as a resource,
# so the link gate checks every <source> and <track> a player points at. A poster
# is not a link DocFX follows, which is why a player is only emitted once all
# three files are known to exist.
#
# docs/videos is not a package directory: the navigation below gives it a tab,
# an index and a TOC of its own instead of a place in the documentation map.
$videoPaths = [ordered]@{
    'front-door'   = 'Front door'
    'build'        = 'Build'
    'evaluate'     = 'Evaluate'
    'operate'      = 'Operate'
    'secure'       = 'Deep dive: Secure'
    'how-it-works' = 'Deep dive: How it works'
}
$videoBlock = [regex]'(?s)<!--[ \t]*video:begin\b(?<attributes>.*?)-->.*?<!--[ \t]*video:end[ \t]*-->'
$videoMedia = Join-Path $PSScriptRoot 'media'
$videoMediaStaged = Join-Path $Staging 'media'
$videosStaged = Join-Path (Join-Path $Staging 'docs') 'videos'
$episodes = New-Object System.Collections.Generic.List[object]

# m:ss or h:mm:ss, as the ISO 8601 duration a <time datetime> takes.
function ConvertTo-IsoDuration([string]$Length) {
    $parts = @($Length.Split(':') | ForEach-Object { [int]$_ })
    if ($parts.Count -eq 2) { $parts = @(0) + $parts }
    $iso = 'PT'
    if ($parts[0]) { $iso += "$($parts[0])H" }
    if ($parts[1]) { $iso += "$($parts[1])M" }
    return "$iso$($parts[2])S"
}

# The one piece of markup that plays an episode, on its companion page and on the
# home page alike. $MediaPath is the path from the page to the staged media/.
# Nothing loads until the reader presses play, and the width and height reserve
# the 16:9 frame before the poster arrives.
function Get-VideoPlayer($Episode, [string]$MediaPath, [string]$CaptionHtml) {
    $base = "$MediaPath/$($Episode.Slug)-$($Episode.Cut)"
    $label = [System.Net.WebUtility]::HtmlEncode("$($Episode.Title), video, $($Episode.Length)")
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('<figure class="lt-video">')
    $lines.Add('<div class="lt-video-frame">')
    $lines.Add("<video controls preload=`"none`" playsinline poster=`"$base.jpg`" width=`"1920`" height=`"1080`" aria-label=`"$label`">")
    $lines.Add("<source src=`"$base.mp4`" type=`"video/mp4`">")
    $lines.Add("<track kind=`"captions`" srclang=`"en`" label=`"English`" src=`"$base.vtt`">")
    $lines.Add("<p>This browser cannot play the video here. <a href=`"$base.mp4`">Download it</a> (MP4, $($Episode.Size)).</p>")
    $lines.Add('</video>')
    $lines.Add('</div>')
    if ($CaptionHtml) { $lines.Add("<figcaption class=`"lt-video-caption`">$CaptionHtml</figcaption>") }
    $lines.Add('</figure>')
    return $lines -join "`n"
}

if (Test-Path $videosStaged) {
    foreach ($page in @(Get-ChildItem $videosStaged -Filter *.md | Sort-Object Name)) {
        # The index is generated below; it is never a companion page.
        if ($page.Name -ieq 'index.md') { Remove-Item -LiteralPath $page.FullName; continue }

        $text = Get-Content -LiteralPath $page.FullName -Raw
        $blocks = $videoBlock.Matches($text)
        if ($blocks.Count -ne 1) {
            throw "docs/videos/$($page.Name) must carry exactly one video block, from <!-- video:begin ... --> to <!-- video:end -->, and it has $($blocks.Count)."
        }
        $attributes = @{}
        foreach ($m in [regex]::Matches($blocks[0].Groups['attributes'].Value, '(?<name>[a-z-]+)="(?<value>[^"]*)"')) {
            $attributes[$m.Groups['name'].Value] = $m.Groups['value'].Value
        }

        # Fail on a malformed block rather than guess, so a contract drift in the
        # videos/ generator surfaces here instead of as a quietly missing episode.
        $slug = $page.BaseName
        $where = "The video block in docs/videos/$($page.Name)"
        if ($attributes['episode'] -ne $slug) {
            throw "$where names episode '$($attributes['episode'])', but a companion page's episode is its file name, '$slug'."
        }
        if (-not $videoPaths.Contains([string]$attributes['path'])) {
            throw "$where has path '$($attributes['path'])', which is not one of: $($videoPaths.Keys -join ', ')."
        }
        $order = 0
        if (-not [int]::TryParse([string]$attributes['order'], [ref]$order) -or $order -lt 1) {
            throw "$where has order '$($attributes['order'])'; it must be an integer from 1."
        }
        $length = [string]$attributes['length']
        if ($length -notmatch '^(\d+:)?\d{1,2}:\d{2}$') {
            throw "$where has length '$length'; it must read m:ss or h:mm:ss."
        }

        $cut = [string]$attributes['cut']
        $files = @()
        if ($cut) { $files = @('mp4', 'vtt', 'jpg' | ForEach-Object { Join-Path $videoMedia "$slug-$cut.$_" }) }
        $missing = @($files | Where-Object { -not (Test-Path -LiteralPath $_) })
        if (-not $cut -or $missing.Count -gt 0) {
            Remove-Item -LiteralPath $page.FullName
            $reason = if (-not $cut) { 'it has no cut yet' } else { "docs-site/media has no $(Split-Path $missing[0] -Leaf)" }
            Write-Host "Left docs/videos/$($page.Name) out of the site: $reason"
            continue
        }

        New-Item -ItemType Directory -Path $videoMediaStaged -Force | Out-Null
        foreach ($file in $files) { Copy-Item -LiteralPath $file -Destination $videoMediaStaged -Force }

        $heading = [regex]::Match($text, '(?m)^#[ \t]+(.+?)[ \t]*#*[ \t]*\r?$')
        $episode = [pscustomobject]@{
            Slug          = $slug
            Page          = $page.Name
            Path          = [string]$attributes['path']
            Order         = $order
            Length        = $length
            Cut           = $cut
            Title         = if ($heading.Success) { $heading.Groups[1].Value -replace '`', '' } else { $slug }
            Size          = [string]::Format([Globalization.CultureInfo]::InvariantCulture, '{0:N1} MB', ((Get-Item -LiteralPath $files[0]).Length / 1e6))
            HasTranscript = $text -match '(?m)^##[ \t]+Transcript[ \t]*\r?$'
            Idea          = $null
        }

        $base = "../../media/$slug-$cut"
        $facts = @(
            "<span><time datetime=`"$(ConvertTo-IsoDuration $length)`">$length</time></span>",
            '<span>English captions</span>',
            "<span><a href=`"$base.mp4`">Download the MP4</a> ($($episode.Size))</span>"
        ) -join (Get-Separator ', ')
        $player = Get-VideoPlayer $episode '../../media' $facts
        $newline = if ($text.Contains("`r`n")) { "`r`n" } else { "`n" }
        $block = $blocks[0]
        $text = $text.Substring(0, $block.Index) + ($player -replace "`n", $newline) + $text.Substring($block.Index + $block.Length)
        Set-Content -LiteralPath $page.FullName -Value $text -NoNewline -Encoding utf8
        $episodes.Add($episode)
    }
}
Write-Host "Staged $($episodes.Count) published video episode(s)"

# --- Rewrite every link that does not resolve inside the site to github.com ---
# Resolution-based rather than pattern-based: any relative target absent from the
# staged tree has no site counterpart (source code, spec files, agent
# instructions), as does a link to a directory or to a staged non-markdown file.
# Anything that does resolve to a page is left untouched. A repository link points
# at the ref the site is built from, so it shows the source this page documents.
$blobBase = "$repositoryUrl/blob/$($site.Ref)"
$treeBase = "$repositoryUrl/tree/$($site.Ref)"
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

        # The site writes its own llms.txt and llms-full.txt after this pass, so
        # a link to them stays on the site rather than going to the repository.
        $full = [System.IO.Path]::GetFullPath($resolved)
        if ($full.StartsWith($Staging) -and $siteGenerated -contains (ConvertTo-SiteRelative $full $Staging)) { return $match.Value }

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
                # A staged non-markdown file (LICENSE) has no page either.
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

# --- Helpers shared by the generated pages ---

# A double-quoted YAML scalar. A title such as "Security: identity" would
# otherwise parse as a nested mapping and break the TOC.
function ConvertTo-YamlString([string]$Value) {
    return '"' + ($Value -replace '\\', '\\' -replace '"', '\"') + '"'
}

# Inline markdown to HTML, for text placed inside a raw HTML block, where DocFX
# does not run the markdown parser.
function ConvertTo-HtmlText([string]$Markdown) {
    $text = [System.Net.WebUtility]::HtmlEncode($Markdown)
    $text = [regex]::Replace($text, '`([^`]+)`', '<code>$1</code>')
    $text = [regex]::Replace($text, '\*\*([^*]+)\*\*', '<strong>$1</strong>')
    $text = [regex]::Replace($text, '\[([^\]]+)\]\([^)]*\)', '$1')
    return $text
}

# Markdown copied out of a root page (PACKAGES.md) onto a page elsewhere in the
# site keeps the root page's relative links, which would then resolve from the
# wrong directory. This rebases them: a relative target gains the prefix, and a
# bare in-page anchor is pointed back at the page it came from. Absolute URLs
# are left alone.
function ConvertTo-RebasedMarkdown([string]$Markdown, [string]$Prefix, [string]$SourcePage) {
    return [regex]::Replace($Markdown, '\]\(([^)\s]+)\)', {
        param($match)
        $target = $match.Groups[1].Value
        if ($target -match '^([a-z][a-z0-9+.-]*:|//)') { return $match.Value }
        if ($target.StartsWith('#')) { return "]($Prefix$SourcePage$target)" }
        return "]($Prefix$target)"
    })
}

function Get-FirstSentence([string]$Text) {
    if (-not $Text) { return $null }
    $parts = [regex]::Split($Text.Trim(), '(?<=[a-z0-9\)\]`''"])(?<!\be\.g)(?<!\bi\.e)\.\s+(?=[A-Z`*\[])')
    $first = $parts[0].Trim()
    if (-not $first.EndsWith('.')) { $first += '.' }
    return $first
}

# The first prose paragraph under a page's H1, skipping headings, quotes,
# tables, lists, fences, and images, joined onto one line.
function Get-FirstParagraph([string]$Path) {
    $lines = New-Object System.Collections.Generic.List[string]
    $seenTitle = $false
    $inFence = $false
    foreach ($line in (Get-Content -LiteralPath $Path)) {
        if ($line -match '^\s*(```|~~~)') { $inFence = -not $inFence; if ($lines.Count) { break }; continue }
        if ($inFence) { continue }
        if (-not $seenTitle) { if ($line -match '^#\s') { $seenTitle = $true }; continue }
        if ($line.Trim() -eq '') { if ($lines.Count) { break }; continue }
        if ($line -match '^\s*(#|>|\||[-*+]\s|\d+\.\s|!\[|<)') { if ($lines.Count) { break }; continue }
        $lines.Add($line.Trim())
    }
    if ($lines.Count -eq 0) { return $null }
    return ($lines -join ' ')
}

# The page's H1, shortened for navigation: markdown stripped, the package id a
# title opens with dropped (the page already sits under that package), a
# trailing parenthetical dropped, and a long "Topic: detail" cut to its topic.
function Get-DocTitle([string]$Path, [string]$PackageId) {
    $match = Select-String -LiteralPath $Path -Pattern '^#\s+(.+?)\s*#*\s*$' | Select-Object -First 1
    if (-not $match) { return $null }
    $title = $match.Matches[0].Groups[1].Value
    $title = [regex]::Replace($title, '\[([^\]]+)\]\([^)]*\)', '$1')
    $title = $title -replace '`', '' -replace '\*\*', ''
    if ($PackageId -and $title.Length -gt $PackageId.Length -and
        $title.StartsWith($PackageId + ' ', [StringComparison]::OrdinalIgnoreCase)) {
        $title = $title.Substring($PackageId.Length + 1)
    }
    $title = [regex]::Replace($title, '\s*\([^()]*\)\s*$', '')
    if ($title.Length -gt 34 -and $title -match '^([^:]{3,}):\s') { $title = $Matches[1] }
    $title = $title.Trim()
    if ($title) { $title = $title.Substring(0, 1).ToUpperInvariant() + $title.Substring(1) }
    return $title
}

# A directory's README, matched case-insensitively (docs/crdt carries
# readme.md) so the result is the same on Windows and on Linux CI.
function Get-Readme([System.IO.DirectoryInfo]$Directory) {
    return Get-ChildItem -LiteralPath $Directory.FullName -Filter *.md |
        Where-Object { $_.Name -ieq 'README.md' } | Select-Object -First 1
}

# DocFX's heading ids: lower-cased, punctuation dropped, each space a hyphen
# ("AI / RepoContext" -> "ai--repocontext"). The link gate checks every use.
function ConvertTo-Slug([string]$Heading) {
    return (($Heading.ToLowerInvariant() -replace '[^a-z0-9 \-]', '') -replace ' ', '-')
}

# --- Join figures: the animated order diagrams ---
# The home page and every CRDT explainer carry one order diagram of a merge,
# generated from docs-site/figures/join-figures.json so that every figure shares
# one geometry, one notation, and one animation: states are nodes, the order
# runs upward, and two concurrent writes meet at their join. A figure is
# complete without script - it shows the converged state - and main.js animates
# it from the routes written onto its tokens here, so the script knows nothing
# about any one CRDT.
#
# A figure's labels are drawn, not written: each <text> lives in a label sheet
# staged beside the site (figures/<id>-<width>.svg) and is drawn in place by a
# <use> that carries its class, so it inherits the label's type and ink exactly
# as the <text> would. The page itself holds none of their words, so a reader
# that takes the page as text - an agent, a text browser - gets the figure's
# title and prose description instead of its labels run together ("merge
# Bmerge AA=3, B=5join"). The buttons, which only work with script, name
# themselves the same way, from CSS (see main.css).
#
# Each scenario mirrors the Behaviour example on its page, and the figure is
# inserted at the top of that section of the STAGED page only. The tracked
# document is untouched, so it still renders on github.com exactly as before.
$figureSource = Join-Path $PSScriptRoot 'figures/join-figures.json'
if (-not (Test-Path $figureSource)) { throw "Missing $figureSource, which the home page and the CRDT explainers draw from." }
$figureSpecs = @((Get-Content $figureSource -Raw | ConvertFrom-Json).figures)
$figureSheets = Join-Path $Staging 'figures'

function Get-EncodedHtml([string]$Text) {
    return [System.Net.WebUtility]::HtmlEncode($Text)
}

# One figure as a single HTML block with no blank lines, so markdown passes it
# through untouched. Three shapes cover the primitives:
#   diamond  two incomparable writes (a, b) and their join above both;
#   chain    with a middle node: comparable values, as in a register over a
#            total order, where the join IS the higher write and 'direct' names
#            the writer that reaches it in one step, drawn as a curve;
#   chain    without one: a write that changes nothing (a remove that observed
#            nothing), where 'noop' names the writer that stays at the bottom
#            until the merge lifts it.
# $SitePrefix is the path from the page up to the site root, where the label
# sheets are staged.
function Get-JoinFigure {
    param(
        [Parameter(Mandatory = $true)] $Spec,
        [int]$Width = 560,
        [string]$CaptionHtml,
        [string]$SitePrefix = '',
        [switch]$Inline
    )

    $id = [string]$Spec.id
    $cx = [int]($Width / 2)
    $nodes = $Spec.nodes
    $edgeText = $Spec.edges
    $svg = New-Object System.Collections.Generic.List[string]
    $segments = New-Object System.Collections.Generic.List[object]
    $routes = @{}
    $sheetName = "$id-$Width"
    $sheet = New-Object System.Collections.Generic.List[string]

    # The label goes into the figure's sheet, and a <use> draws it here.
    function Add-Text([string]$Class, [int]$X, [int]$Y, [string]$Value, [string]$Anchor) {
        if (-not $Value) { return }
        $anchorAttribute = if ($Anchor -and $Anchor -ne 'start') { " text-anchor=`"$Anchor`"" } else { '' }
        $labelId = "$sheetName-$($sheet.Count + 1)"
        $sheet.Add("<text id=`"$labelId`" x=`"$X`" y=`"$Y`"$anchorAttribute>$(Get-EncodedHtml $Value)</text>")
        $svg.Add("<use class=`"$Class`" href=`"${SitePrefix}figures/$sheetName.svg#$labelId`"/>")
    }

    # State lines 18 units apart, then the note 20 below the last of them.
    function Add-NodeLabel($Node, [int]$X, [int]$FirstBaseline, [string]$Anchor) {
        $y = $FirstBaseline
        foreach ($line in @($Node.state)) {
            Add-Text 'lt-label-state' $X $y ([string]$line) $Anchor
            $y += 18
        }
        Add-Text 'lt-label-note' $X ($y + 2) ([string]$Node.note) $Anchor
    }

    function Get-Lines($Node) { return @($Node.state).Count }

    # A token's route: where it starts, then one entry per phase (the writes,
    # then the merges). An empty phase holds the token where it is.
    function Get-Route([int[]]$Start, [object[]]$Phases) {
        $route = [ordered]@{ start = $Start; phases = $Phases }
        return Get-EncodedHtml (ConvertTo-Json -InputObject $route -Compress -Depth 6)
    }

    function Get-EdgeMarkup($Segment, [string]$Class) {
        $from = $points[$Segment.From]
        $to = $points[$Segment.To]
        $classAttribute = if ($Class) { " class=`"$Class`" pathLength=`"100`"" } else { '' }
        if ($Segment.Via) {
            return "<path$classAttribute d=`"M $($from[0]) $($from[1]) Q $($Segment.Via[0]) $($Segment.Via[1]) $($to[0]) $($to[1])`"/>"
        }
        return "<line$classAttribute x1=`"$($from[0])`" y1=`"$($from[1])`" x2=`"$($to[0])`" y2=`"$($to[1])`"/>"
    }

    switch ($Spec.layout) {
        'diamond' {
            $points = @{ bottom = @($cx, 356); a = @(($cx - 138), 206); b = @(($cx + 138), 206); join = @($cx, 56) }
            $kinds = [ordered]@{ bottom = 'bottom'; a = 'concurrent'; b = 'concurrent'; join = 'join' }
            $top = 'join'
            foreach ($s in @(@('bottom', 'a', 'low'), @('bottom', 'b', 'low'), @('a', 'join', 'high'), @('b', 'join', 'high'))) {
                $segments.Add(@{ From = $s[0]; To = $s[1]; Phase = $s[2]; Via = $null })
            }
            $routes['a'] = Get-Route $points.bottom @([ordered]@{ to = $points.a }, [ordered]@{ to = $points.join })
            $routes['b'] = Get-Route $points.bottom @([ordered]@{ to = $points.b }, [ordered]@{ to = $points.join })
        }
        'chain' {
            $points = @{ bottom = @($cx, 356); top = @($cx, 56) }
            $top = 'top'
            if ($null -ne $nodes.middle) {
                $direct = [string]$Spec.direct
                if (@('a', 'b') -notcontains $direct) { throw "Join figure '$id' is a three-node chain, so it must name its 'direct' writer, a or b." }
                $other = if ($direct -eq 'a') { 'b' } else { 'a' }
                $side = if ($direct -eq 'a') { -1 } else { 1 }
                $via = @(($cx + $side * 220), 206)
                $points.middle = @($cx, 206)
                $kinds = [ordered]@{ bottom = 'bottom'; middle = 'concurrent'; top = 'join' }
                $segments.Add(@{ From = 'bottom'; To = 'middle'; Phase = 'low'; Via = $null })
                $segments.Add(@{ From = 'bottom'; To = 'top'; Phase = 'low'; Via = $via })
                $segments.Add(@{ From = 'middle'; To = 'top'; Phase = 'high'; Via = $null })
                $routes[$direct] = Get-Route $points.bottom @([ordered]@{ to = $points.top; via = $via }, [ordered]@{})
                $routes[$other] = Get-Route $points.bottom @([ordered]@{ to = $points.middle }, [ordered]@{ to = $points.top })
            }
            else {
                $noop = [string]$Spec.noop
                if (@('a', 'b') -notcontains $noop) { throw "Join figure '$id' is a two-node chain, so it must name its 'noop' writer, a or b." }
                $mover = if ($noop -eq 'a') { 'b' } else { 'a' }
                # No curve, so node labels take the right and the one edge label the left.
                $side = -1
                $kinds = [ordered]@{ bottom = 'bottom'; top = 'join' }
                $segments.Add(@{ From = 'bottom'; To = 'top'; Phase = 'low'; Via = $null })
                $routes[$mover] = Get-Route $points.bottom @([ordered]@{ to = $points.top }, [ordered]@{})
                $routes[$noop] = Get-Route $points.bottom @([ordered]@{}, [ordered]@{ to = $points.top })
            }
        }
        default { throw "Join figure '$id' has an unknown layout '$($Spec.layout)'." }
    }

    $redeliverFrom = $points[[string]$Spec.redeliver.from]
    if (-not $redeliverFrom) { throw "Join figure '$id' redelivers from '$($Spec.redeliver.from)', which is not one of its nodes." }
    $routes['dup'] = Get-Route $redeliverFrom @([ordered]@{ to = $points[$top] })

    # Edges: a quiet base drawing, then the ink that draws itself as it plays.
    $svg.Add('<g class="lt-edges-base">')
    foreach ($s in $segments) { $svg.Add((Get-EdgeMarkup $s '')) }
    $svg.Add('</g>')
    $svg.Add('<g class="lt-edges">')
    foreach ($s in $segments) { $svg.Add((Get-EdgeMarkup $s "lt-edge lt-edge-$($s.Phase)")) }
    $svg.Add('</g>')

    if ($Spec.layout -eq 'diamond') {
        $svg.Add("<line class=`"lt-concurrent-line`" x1=`"$($cx - 114)`" y1=`"206`" x2=`"$($cx + 114)`" y2=`"206`"/>")
        Add-Text 'lt-label-concurrent' $cx 198 ([string]$Spec.concurrent) 'middle'
        # A phone draws an explainer's figure too small for the full sentence,
        # so it shows the word before the colon instead (see main.css).
        Add-Text 'lt-label-concurrent lt-label-concurrent-short' $cx 198 (([string]$Spec.concurrent -split ':')[0]) 'middle'
        Add-Text 'lt-label-edge' ($cx - 82) 296 ([string]$edgeText.lowerLeft) 'end'
        Add-Text 'lt-label-edge' ($cx + 82) 296 ([string]$edgeText.lowerRight) 'start'
        Add-Text 'lt-label-edge' ($cx - 82) 124 ([string]$edgeText.upperLeft) 'end'
        Add-Text 'lt-label-edge' ($cx + 82) 124 ([string]$edgeText.upperRight) 'start'
    }
    elseif ($null -ne $nodes.middle) {
        # Node and chain-edge labels sit on the side away from the curve.
        $labelSide = -$side
        $anchor = if ($labelSide -gt 0) { 'start' } else { 'end' }
        Add-Text 'lt-label-edge' ($cx + $side * 122) 210 ([string]$edgeText.direct) $(if ($side -lt 0) { 'end' } else { 'start' })
        Add-Text 'lt-label-edge' ($cx + $labelSide * 14) 285 ([string]$edgeText.lower) $anchor
        Add-Text 'lt-label-edge' ($cx + $labelSide * 14) 135 ([string]$edgeText.upper) $anchor
    }
    else {
        Add-Text 'lt-label-edge' ($cx - 16) 210 ([string]$edgeText.lower) 'end'
    }

    $svg.Add("<circle class=`"lt-join-pulse`" cx=`"$($points[$top][0])`" cy=`"$($points[$top][1])`" r=`"13`"/>")
    foreach ($name in $kinds.Keys) {
        $point = $points[$name]
        $radius = if ($kinds[$name] -eq 'join') { 13 } else { 9 }
        $svg.Add("<circle class=`"lt-node lt-node-$($kinds[$name])`" data-lt-node=`"$name`" cx=`"$($point[0])`" cy=`"$($point[1])`" r=`"$radius`"/>")
    }

    # The join's label hides while the merge replays and returns when it lands.
    if ($Spec.layout -eq 'diamond') {
        $svg.Add('<g class="lt-label-join">')
        Add-NodeLabel $nodes.join ($cx + 24) (44 - 18 * ((Get-Lines $nodes.join) - 1)) 'start'
        $svg.Add('</g>')
        Add-NodeLabel $nodes.a ($cx - 154) (203 - 9 * ((Get-Lines $nodes.a) - 1)) 'end'
        Add-NodeLabel $nodes.b ($cx + 154) (203 - 9 * ((Get-Lines $nodes.b) - 1)) 'start'
        Add-NodeLabel $nodes.bottom ($cx + 22) 366 'start'
    }
    else {
        $labelSide = -$side
        $x = $cx + $labelSide * 24
        $anchor = if ($labelSide -gt 0) { 'start' } else { 'end' }
        $svg.Add('<g class="lt-label-join">')
        Add-NodeLabel $nodes.top $x (50 - 18 * ((Get-Lines $nodes.top) - 1)) $anchor
        $svg.Add('</g>')
        if ($null -ne $nodes.middle) { Add-NodeLabel $nodes.middle $x (203 - 9 * ((Get-Lines $nodes.middle) - 1)) $anchor }
        Add-NodeLabel $nodes.bottom $x (353 - 9 * ((Get-Lines $nodes.bottom) - 1)) $anchor
    }

    foreach ($token in @('a', 'b')) {
        $svg.Add("<circle class=`"lt-token`" data-lt-token=`"$token`" data-lt-route=`"$($routes[$token])`" cx=`"0`" cy=`"0`" r=`"6`"/>")
    }
    $svg.Add("<circle class=`"lt-token lt-token-dup`" data-lt-token=`"dup`" data-lt-route=`"$($routes['dup'])`" cx=`"0`" cy=`"0`" r=`"6`"/>")

    $steps = @($Spec.steps)
    $redeliver = $Spec.redeliver
    $figureClass = if ($Inline) { 'lt-join lt-join-inline' } else { 'lt-join' }
    $data = @(
        "data-lt-step1=`"$(Get-EncodedHtml $steps[0])`"",
        "data-lt-step2=`"$(Get-EncodedHtml $steps[1])`"",
        "data-lt-settled=`"$(Get-EncodedHtml $Spec.settled)`"",
        "data-lt-pending=`"$(Get-EncodedHtml $redeliver.pending)`"",
        "data-lt-done=`"$(Get-EncodedHtml $redeliver.done)`"",
        "data-lt-reduced=`"$(Get-EncodedHtml $redeliver.reduced)`""
    ) -join ' '
    $caption = if ($CaptionHtml) { $CaptionHtml } else { Get-EncodedHtml $Spec.rule }

    $html = New-Object System.Collections.Generic.List[string]
    $html.Add("<figure class=`"$figureClass`" data-lt-join $data>")
    $html.Add("<svg class=`"lt-join-svg`" viewBox=`"0 0 $Width 420`" role=`"img`" aria-labelledby=`"lt-join-title-$id lt-join-desc-$id`">")
    # The title ends as a sentence, so a reader that takes the page as text does
    # not run it into the description that follows it.
    $figureTitle = [string]$Spec.title
    if ($figureTitle -notmatch '[.!?]$') { $figureTitle += '.' }
    $html.Add("<title id=`"lt-join-title-$id`">$(Get-EncodedHtml $figureTitle)</title>")
    $html.Add("<desc id=`"lt-join-desc-$id`">$(Get-EncodedHtml $Spec.description)</desc>")
    foreach ($line in $svg) { $html.Add($line) }
    $html.Add('</svg>')
    $html.Add('<figcaption class="lt-join-caption">')
    $html.Add("<p class=`"lt-join-status`" data-lt-join-status aria-live=`"polite`">$(Get-EncodedHtml $Spec.settled)</p>")
    $html.Add('<div class="lt-join-controls">')
    $html.Add('<button type="button" class="lt-button" data-lt-join-replay aria-label="Replay the merge" data-lt-label="Replay the merge"></button>')
    $redeliverLabel = Get-EncodedHtml $redeliver.label
    $html.Add("<button type=`"button`" class=`"lt-button lt-button-quiet`" data-lt-join-redeliver aria-label=`"$redeliverLabel`" data-lt-label=`"$redeliverLabel`"></button>")
    $html.Add('</div>')
    $html.Add("<p class=`"lt-join-source`">$caption</p>")
    $html.Add('</figcaption>')
    $html.Add('</figure>')

    # The sheet the <use> elements draw from. A figure drawn twice at the same
    # width (the G-Counter's, on the home page and its explainer) shares it.
    New-Item -ItemType Directory -Path $figureSheets -Force | Out-Null
    $sheetXml = @('<svg xmlns="http://www.w3.org/2000/svg">') + @($sheet) + @('</svg>')
    [System.IO.File]::WriteAllText((Join-Path $figureSheets "$sheetName.svg"), ($sheetXml -join "`n") + "`n", (New-Object System.Text.UTF8Encoding $false))
    return ($html -join "`n")
}

# Place each explainer's figure at the top of its Behaviour section. A figure
# whose page or heading has gone throws rather than silently disappearing.
$placedFigures = 0
foreach ($spec in $figureSpecs) {
    if (-not $spec.page) { continue }
    $page = Join-Path $Staging $spec.page
    if (-not (Test-Path -LiteralPath $page)) {
        throw "Join figure '$($spec.id)' belongs on $($spec.page), which is not in the corpus. Update docs-site/figures/join-figures.json."
    }
    $text = Get-Content -LiteralPath $page -Raw
    $heading = [regex]'(?m)^## Behaviour[ \t]*(?=\r?$)'
    $found = $heading.Matches($text).Count
    if ($found -ne 1) {
        throw "Join figure '$($spec.id)' goes at the top of the '## Behaviour' section of $($spec.page), which has $found such headings."
    }
    $newline = if ($text.Contains("`r`n")) { "`r`n" } else { "`n" }
    $depth = ([string]$spec.page).Split('/').Count - 1
    $figure = (Get-JoinFigure -Spec $spec -Inline -SitePrefix ('../' * $depth)) -replace "`n", $newline
    $text = $heading.Replace($text, { param($match) $match.Value + $newline + $newline + $figure }, 1)
    Set-Content -LiteralPath $page -Value $text -NoNewline -Encoding utf8
    $placedFigures++
}
Write-Host "Placed $placedFigures join figure(s) on CRDT explainer pages"

# --- Pages too large to read: split into an index and a page per section ---
# A reader - above all an agent, which fetches a page in slices of a few tens of
# thousands of characters - should not have to read the whole API reference to
# reach one type. Any staged page larger than $pageBudget becomes an index (its
# introduction, its short sections, and a list of the rest) and one page per
# level-2 section, in a folder named after it; a section larger than
# $sectionBudget is split again at its level-3 headings (see lib/split.ps1).
#
# The release history is split by release whatever its size, because a release
# is what a reader looks one up by: every "## [YYYY-MM-DD]" section becomes
# changelog/<date>.md.
#
# Every heading keeps its id, and every link to a heading that moved - from this
# page or any other - is pointed at its new page once everything is staged, so
# the zero-warning link gate proves the split lost nothing.
$pageBudget = 100000
$sectionBudget = 60000
$splits = @{}

# The difference between a line's number in the staged page's body and in its
# source: zero for a copied page, and not zero where staging added front matter
# or, in the changelog, removed the Unreleased section above $Anchor.
function Get-SourceLineOffset([string]$Page, [string]$SourcePath, [string]$Anchor) {
    $body = @((Split-FrontMatter ([System.IO.File]::ReadAllText((Join-Path $Staging $Page)))).Body -split '\r?\n')
    $source = @([System.IO.File]::ReadAllLines($SourcePath))
    if (-not $Anchor) { $Anchor = $body | Where-Object { $_.Trim() } | Select-Object -First 1 }
    $inBody = [array]::IndexOf($body, $Anchor)
    $inSource = [array]::IndexOf($source, $Anchor)
    if ($inBody -lt 0 -or $inSource -lt 0) { return $null }
    return $inSource - $inBody
}

# Splits one staged page and records where each new page came from: its lines
# in the source file where there is one, the page's own origin where it was
# generated.
function Invoke-PageSplit([string]$Page, [hashtable]$Options) {
    $source = Join-Path $RepoRoot $Page
    $offset = if (Test-Path -LiteralPath $source) { Get-SourceLineOffset $Page $source $Options['Anchor'] } else { $null }
    $Options.Remove('Anchor')
    $result = Split-LargePage -Staging $Staging -Page $Page -SectionBudget $sectionBudget @Options
    if (-not $result) { return }
    $splits[$Page] = $result
    foreach ($entry in $result.Pages) {
        $origins[$entry.Path] = if ($null -ne $offset) { Get-SourceUrl $Page ($entry.Start + 1 + $offset) ($entry.End + $offset) } else { $origins[$Page] }
    }
    Write-Host "Split $Page into $($result.Pages.Count) page(s)"
}

foreach ($file in @(Get-ChildItem $Staging -Recurse -Filter *.md)) {
    $relative = ConvertTo-SiteRelative $file.FullName $Staging
    if ($relative -eq 'CHANGELOG.md' -or $file.Length -le $pageBudget) { continue }
    Invoke-PageSplit $relative @{ InlineBelow = 5000 }
}

if (Test-Path (Join-Path $Staging 'CHANGELOG.md')) {
    $firstRelease = Get-Content (Join-Path $Staging 'CHANGELOG.md') | Where-Object { $_ -match '^## \[' } | Select-Object -First 1
    Invoke-PageSplit 'CHANGELOG.md' @{
        Anchor          = $firstRelease
        Directory       = 'changelog'
        Title           = { param($plain) 'Release ' + $plain.Trim('[', ']') }
        # A release is looked up by the version it shipped. Its opening paragraph
        # names that ("advance to `9.7.1`"), but often after the first sentence,
        # which the contents would cut short, so they lead with it. Locals carry
        # a prefix: this runs inside Split-LargePage's scope.
        Describe        = {
            param([string]$RelPlain, [string[]]$RelLines, [int]$RelStart, [int]$RelEnd)
            $relDescription = Get-RangeDescription $RelLines $RelStart $RelEnd
            $relIntro = New-Object System.Collections.Generic.List[string]
            for ($relAt = $RelStart; $relAt -lt $RelEnd -and $RelLines[$relAt] -notmatch '^#{1,6}\s'; $relAt++) { $relIntro.Add($RelLines[$relAt]) }
            $relShipped = @([regex]::Matches(($relIntro -join ' '), '\bto\s+`(?<v>\d+\.\d+\.\d+)`') | ForEach-Object { '`' + $_.Groups['v'].Value + '`' } | Select-Object -Unique)
            if ($relShipped.Count -eq 0) { return $relDescription }
            $relList = if ($relShipped.Count -le 2) { $relShipped -join ' and ' } else { ($relShipped[0..($relShipped.Count - 2)] -join ', ') + ', and ' + $relShipped[-1] }
            return "Ships $relList. $relDescription"
        }
        Keep            = @('Older releases')
        Drop            = @('Released')
        ContentsHeading = 'Releases'
        ContentsLede    = 'One page per release, newest first. Each opens with the package versions that release shipped.'
        PartOf          = 'Part of the [changelog]({0}).'
        PreviousLabel   = 'Newer release'
        NextLabel       = 'Older release'
    }
    $result = $splits['CHANGELOG.md']
    if ($result) {
        # The releases get a sidebar of their own, newest first.
        $changelogToc = New-Object System.Collections.Generic.List[string]
        $changelogToc.Add('- name: All releases')
        $changelogToc.Add('  href: ../CHANGELOG.md')
        foreach ($entry in $result.Children) {
            $changelogToc.Add("- name: $(ConvertTo-YamlString $entry.TocName)")
            $changelogToc.Add("  href: $([System.IO.Path]::GetFileName($entry.Path))")
            if ($entry.Children.Count -gt 0) {
                $changelogToc.Add('  items:')
                foreach ($child in $entry.Children) {
                    $changelogToc.Add("  - name: $(ConvertTo-YamlString $child.TocName)")
                    $changelogToc.Add("    href: $([System.IO.Path]::GetFileName($child.Path))")
                }
            }
        }
        Set-Content -Path (Join-Path $Staging 'changelog/toc.yml') -Value $changelogToc -Encoding utf8
    }
}

# The TOC entries a split page's sections add beneath it, with hrefs relative to
# the directory of the TOC that lists them.
function Add-SplitTocItems($Lines, [string]$Page, [string]$TocDirectory, [string]$Indent) {
    $result = $splits[$Page]
    if (-not $result) { return }
    $Lines.Add("$Indent  items:")
    foreach ($entry in $result.Children) {
        $Lines.Add("$Indent  - name: $(ConvertTo-YamlString $entry.TocName)")
        $Lines.Add("$Indent    href: $(Get-SiteRelativeLink $TocDirectory $entry.Path)")
        if ($entry.Children.Count -gt 0) {
            $Lines.Add("$Indent    items:")
            foreach ($child in $entry.Children) {
                $Lines.Add("$Indent    - name: $(ConvertTo-YamlString $child.TocName)")
                $Lines.Add("$Indent      href: $(Get-SiteRelativeLink $TocDirectory $child.Path)")
            }
        }
    }
}

# --- The package catalogue, parsed from PACKAGES.md ---
# One entry per "## " section (Contents and Related excepted), each with its
# lede and its table rows: package id, published or not, description, and the
# docs directory the row links. The navigation, the documentation map, and the
# home page's seam summary are all generated from this, so a package added to
# PACKAGES.md is filed, named, and labelled everywhere without touching this
# script.
function Get-PackageCatalogue([string]$Path) {
    $sections = New-Object System.Collections.Generic.List[object]
    $current = $null
    $lede = $null
    foreach ($line in (Get-Content -LiteralPath $Path)) {
        if ($line -match '^##\s+(.+?)\s*$') {
            $heading = $Matches[1]
            $current = $null
            if (@('Contents', 'Related') -contains $heading) { continue }
            $current = [pscustomobject]@{
                Name       = ($heading -replace '\s*\(in progress\)\s*$', '')
                InProgress = [bool]($heading -match '\(in progress\)\s*$')
                Lede       = $null
                Rows       = New-Object System.Collections.Generic.List[object]
            }
            $sections.Add($current)
            $lede = New-Object System.Collections.Generic.List[string]
            continue
        }
        if (-not $current) { continue }
        if ($line.StartsWith('|')) {
            if ($lede -and $lede.Count) { $current.Lede = $lede -join ' ' }
            $lede = $null
            $cells = $line.Split('|')
            if ($cells.Count -lt 6 -or $cells[1] -notmatch '^\s*`([^`]+)`\s*$') { continue }
            $id = $Matches[1]
            $docs = [regex]::Match($cells[$cells.Count - 2], '\]\(docs/([^/)]+)/([^)#]*)')
            $current.Rows.Add([pscustomobject]@{
                Id          = $id
                Released    = -not ($cells[2] -match 'Unreleased')
                Description = (($cells[3..($cells.Count - 3)]) -join '|').Trim()
                DocsDir     = if ($docs.Success) { $docs.Groups[1].Value } else { $null }
                DocsFile    = if ($docs.Success) { $docs.Groups[2].Value } else { $null }
            })
            continue
        }
        if ($null -ne $lede) {
            if ($line.Trim() -eq '') { if ($lede.Count) { $current.Lede = $lede -join ' '; $lede = $null } }
            else { $lede.Add($line.Trim()) }
        }
    }
    return $sections
}

$catalogue = Get-PackageCatalogue (Join-Path $RepoRoot 'PACKAGES.md')
$docsRoot = Join-Path $Staging 'docs'
# docs/videos is the video series' own section (see Videos below), not a package.
$packageDirs = Get-ChildItem $docsRoot -Directory | Where-Object { $_.Name -ne 'videos' } | Sort-Object Name

# docs/crdt is a docs-only conceptual topic with no src/ counterpart, so it is
# absent from PACKAGES.md and lands in this catch-all.
$fallbackSection = 'Concepts'

# Everything the generated pages need to know about one docs/<dir>.
$dirInfo = @{}
foreach ($dir in $packageDirs) {
    $name = $dir.Name
    $exact = $null; $firstRow = $null; $section = $null
    foreach ($s in $catalogue) {
        foreach ($r in $s.Rows) {
            if ($r.DocsDir -eq $name) {
                if (-not $section) { $section = $s }
                if (-not $firstRow) { $firstRow = $r }
            }
            if (-not $exact -and $r.Id -ieq ('Orleans.' + $name)) { $exact = $r }
        }
    }

    # The package id in its published casing: the exact package where one exists,
    # otherwise the family prefix of the packages that document here (the one
    # docs/lattice.explorer directory serves every Orleans.Lattice.Explorer.*).
    $id = if ($exact) { $exact.Id } else { $null }
    if (-not $id) {
        $depth = $name.Split('.').Count + 1
        foreach ($s in $catalogue) {
            foreach ($r in $s.Rows) {
                if (-not $id -and $r.Id.ToLowerInvariant().StartsWith('orleans.' + $name + '.')) {
                    $id = ($r.Id.Split('.')[0..($depth - 1)]) -join '.'
                }
            }
        }
    }

    $readme = Get-Readme $dir
    $display = if ($id -eq 'Orleans.Lattice') { 'Orleans.Lattice' }
        elseif ($id -and $id.StartsWith('Orleans.Lattice.')) { $id.Substring('Orleans.Lattice.'.Length) }
        elseif ($id) { $id }
        elseif ($readme) { (Get-DocTitle $readme.FullName $null) -replace '\s+in Orleans\.Lattice$', '' }
        else { $name }

    $status = if ($exact -and -not $exact.Released) { 'unreleased' }
        elseif ($section -and $section.InProgress) { 'in progress' }
        elseif (-not $exact -and $firstRow -and -not $firstRow.Released) { 'unreleased' }
        else { $null }

    $row = if ($exact) { $exact } else { $firstRow }
    $description = if ($row) { Get-FirstSentence $row.Description }
        elseif ($readme) { Get-FirstSentence (Get-FirstParagraph $readme.FullName) }
        else { $null }

    # Where the map sends a reader: the page PACKAGES.md names, else the README,
    # else the first page.
    $landing = $null
    if ($row -and $row.DocsDir -eq $name -and $row.DocsFile -and (Test-Path -LiteralPath (Join-Path $dir.FullName $row.DocsFile))) {
        $landing = $row.DocsFile
    }
    elseif ($readme) { $landing = $readme.Name }
    else {
        $first = Get-ChildItem -LiteralPath $dir.FullName -Filter *.md | Sort-Object Name | Select-Object -First 1
        if ($first) { $landing = $first.Name }
    }

    $dirInfo[$name] = [pscustomobject]@{
        Section     = if ($section) { $section.Name } else { $fallbackSection }
        Id          = $id
        # Whether Id is one package, rather than the family prefix of several.
        Exact       = [bool]$exact
        Display     = $display
        Status      = $status
        Description = $description
        Landing     = $landing
        Readme      = $readme
        Count       = (Get-ChildItem -LiteralPath $dir.FullName -Filter *.md | Measure-Object).Count
    }
}

$sectionOrder = New-Object System.Collections.Generic.List[string]
foreach ($s in $catalogue) { if (-not $sectionOrder.Contains($s.Name)) { $sectionOrder.Add($s.Name) } }
if ($dirInfo.Values | Where-Object { $_.Section -eq $fallbackSection }) { $sectionOrder.Add($fallbackSection) }

# --- A TOC per package directory, driving that package's sidebar ---
# Pages are titled from their own H1, not their file name, so "ttl.md" reads
# "TTL" rather than "Ttl". README first, then alphabetical by that title. A page
# split into sections lists them beneath it. The same entries, in the same
# order, make each package's part of llms.txt.
$packagePages = @{}
foreach ($dir in $packageDirs) {
    $info = $dirInfo[$dir.Name]
    $entries = Get-ChildItem -LiteralPath $dir.FullName -Filter *.md | ForEach-Object {
        $isReadme = $_.Name -ieq 'README.md'
        $title = if ($isReadme) { 'Overview' } else { Get-DocTitle $_.FullName $info.Id }
        if (-not $title) { $title = (Get-Culture).TextInfo.ToTitleCase(($_.BaseName -replace '-', ' ')) }
        [pscustomobject]@{ Name = $_.Name; Title = $title; Order = if ($isReadme) { '0' } else { '1' + $title.ToLowerInvariant() } }
    } | Sort-Object Order
    $packagePages[$dir.Name] = @($entries)

    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($entry in $entries) {
        $lines.Add("- name: $(ConvertTo-YamlString $entry.Title)")
        $lines.Add("  href: $($entry.Name)")
        Add-SplitTocItems $lines "docs/$($dir.Name)/$($entry.Name)" "docs/$($dir.Name)" ''
    }
    Set-Content -Path (Join-Path $dir.FullName 'toc.yml') -Value $lines -Encoding utf8
}

# --- Top-level docs TOC: grouped sections, each holding its package TOCs ---
# A package's label carries its status, so an unreleased or in-progress package
# is never presented as shipped.
$docsToc = New-Object System.Collections.Generic.List[string]
$docsToc.Add('- name: Documentation map')
$docsToc.Add('  href: index.md')
foreach ($section in $sectionOrder) {
    $members = $packageDirs | Where-Object { $dirInfo[$_.Name].Section -eq $section }
    if (-not $members) { continue }
    $docsToc.Add("- name: $(ConvertTo-YamlString $section)")
    $docsToc.Add('  items:')
    foreach ($dir in $members) {
        $info = $dirInfo[$dir.Name]
        $label = if ($info.Status) { "$($info.Display) ($($info.Status))" } else { $info.Display }
        $docsToc.Add("  - name: $(ConvertTo-YamlString $label)")
        $docsToc.Add("    href: $($dir.Name)/toc.yml")
    }
}
if (Test-Path (Join-Path $docsRoot 'RELEASING.md')) {
    $docsToc.Add('- name: Releasing')
    $docsToc.Add('  href: RELEASING.md')
}
Set-Content -Path (Join-Path $docsRoot 'toc.yml') -Value $docsToc -Encoding utf8

# --- The documentation map, grouped the same way as the sidebar ---
# Each package is a node: filled when it is published, hollow when it is not,
# with its status also written out so it never rests on the glyph alone.
$index = New-Object System.Collections.Generic.List[string]
$index.Add('---')
$index.Add('title: Documentation map')
$index.Add('---')
$index.Add('')
$index.Add('# Documentation map')
$index.Add('')
$index.Add('Every package''s documentation, grouped by the seam it fills. A filled node is')
$index.Add('published on NuGet; a hollow node is unreleased or in progress and builds from')
$index.Add('source. For installation see the [package inventory](../PACKAGES.md); for what')
$index.Add('each capability does, see the [capability catalogue](../FEATURES.md).')
$index.Add('')
$index.Add('<ul class="lt-legend" aria-hidden="true"><li>Published</li><li class="lt-unreleased">Unreleased or in progress</li></ul>')
$index.Add('')
foreach ($section in $sectionOrder) {
    $members = $packageDirs | Where-Object { $dirInfo[$_.Name].Section -eq $section -and $dirInfo[$_.Name].Landing }
    if (-not $members) { continue }
    $index.Add("## $section")
    $index.Add('')
    $entry = $catalogue | Where-Object { $_.Name -eq $section } | Select-Object -First 1
    if ($entry -and $entry.Lede) {
        $index.Add((ConvertTo-RebasedMarkdown $entry.Lede '../' 'PACKAGES.md'))
        $index.Add('')
    }
    elseif ($section -eq $fallbackSection) {
        $index.Add('Documentation-only topics that explain ideas the packages share.')
        $index.Add('')
    }
    $index.Add('<ul class="lt-map-list">')
    foreach ($dir in $members) {
        $info = $dirInfo[$dir.Name]
        $class = if ($info.Status) { ' class="lt-unreleased"' } else { '' }
        $status = if ($info.Status) { Get-StatusBadge $info.Status } else { '' }
        $count = if ($info.Count -eq 1) { '1 document' } else { "$($info.Count) documents" }
        $meta = if ($info.Id) { "<code>$($info.Id)</code> &middot; $count" } else { $count }
        $desc = if ($info.Description) { (Get-Separator '. ') + "<span class=`"lt-map-desc`">$(ConvertTo-HtmlText $info.Description)</span>" } else { '' }
        $index.Add("<li$class><span class=`"lt-map-name`"><a href=`"$($dir.Name)/$($info.Landing)`">$([System.Net.WebUtility]::HtmlEncode($info.Display))</a>$status</span>$(Get-Separator ': ')<span class=`"lt-map-meta`">$meta</span>$desc</li>")
    }
    $index.Add('</ul>')
    $index.Add('')
}
Set-Content -Path (Join-Path $docsRoot 'index.md') -Value $index -Encoding utf8
$origins['docs/index.md'] = Get-SourceUrl 'PACKAGES.md'

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

    # A listing too long to read at once becomes one page per file, each with
    # its own source link.
    $sourcePage = "samples/$($sample.Name)/source.md"
    $origins[$sourcePage] = Get-TreeUrl "samples/$($sample.Name)"
    if ((Get-Item (Join-Path $target 'source.md')).Length -gt $pageBudget) {
        Invoke-PageSplit $sourcePage @{}
        if ($splits.ContainsKey($sourcePage)) {
            foreach ($entry in $splits[$sourcePage].Pages) {
                $listed = "samples/$($sample.Name)/$($entry.Title)"
                if (Test-Path -LiteralPath (Join-Path $RepoRoot $listed) -PathType Leaf) { $origins[$entry.Path] = Get-SourceUrl $listed }
            }
        }
    }
}

# --- Samples index and TOC, grouped by concern from FEATURES.md ---
# FEATURES.md groups every capability by concern with a link to its sample; the
# grouping is parsed from it rather than hand-maintained here, so a sample added
# to the catalogue is grouped in the navigation automatically.
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

$sampleEntries = New-Object System.Collections.Generic.List[object]
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
                $sourceItems = New-Object System.Collections.Generic.List[string]
                Add-SplitTocItems $sourceItems "samples/$($sample.Name)/source.md" 'samples' '    '
                foreach ($item in $sourceItems) { $tocEntries.Add($item) }
            }

            $landing = if ($hasReadme) { "$($sample.Name)/README.md" } else { "$($sample.Name)/source.md" }
            $summary = if ($hasReadme) { Get-FirstSentence (Get-FirstParagraph (Join-Path $staged 'README.md')) } else { $null }
            $desc = if ($summary) { (Get-Separator ': ') + "<span class=`"lt-map-desc`">$(ConvertTo-HtmlText $summary)</span>" } else { '' }
            $source = if ($hasReadme -and $hasSource) { (Get-Separator ' (') + "<span class=`"lt-map-meta`"><a href=`"$($sample.Name)/source.md`">Source</a></span>" + (Get-Separator ')') } else { '' }
            $indexEntries.Add("<li><span class=`"lt-map-name`"><a href=`"$landing`">$($sample.Name)</a></span>$source$desc</li>")
            $sampleEntries.Add([pscustomobject]@{ Name = $sample.Name; Section = $section; Readme = $hasReadme; Source = $hasSource; Summary = $summary })
        }
        if ($tocEntries.Count -eq 0) { continue }

        $samplesToc.Add("- name: $(ConvertTo-YamlString $section)")
        $samplesToc.Add('  items:')
        foreach ($entry in $tocEntries) { $samplesToc.Add($entry) }

        $samplesIndex.Add("## $section")
        $samplesIndex.Add('')
        $samplesIndex.Add('<ul class="lt-map-list">')
        foreach ($entry in $indexEntries) { $samplesIndex.Add($entry) }
        $samplesIndex.Add('</ul>')
        $samplesIndex.Add('')
    }

    Set-Content -Path (Join-Path $samplesStaged 'toc.yml') -Value $samplesToc -Encoding utf8
    Set-Content -Path (Join-Path $samplesStaged 'index.md') -Value $samplesIndex -Encoding utf8
    $origins['samples/index.md'] = Get-SourceUrl 'FEATURES.md'
}

# --- Videos: the tab, its index, and the home page's introduction ---
# Published episodes only, grouped by path in a fixed order and ordered within a
# path by `order`. Each is listed with its title, its idea (the first sentence
# of its companion page), and its length. The index draws each path as a chain
# of episodes, as the home page draws each way in as a chain of pages. With
# nothing published there is no Videos tab and no index at all.
foreach ($episode in $episodes) {
    $episode.Idea = Get-FirstSentence (Get-FirstParagraph (Join-Path $videosStaged $episode.Page))
}

if ($episodes.Count -gt 0) {
    $videosToc = New-Object System.Collections.Generic.List[string]
    $videosIndex = New-Object System.Collections.Generic.List[string]
    $videosToc.Add('- name: Videos')
    $videosToc.Add('  href: index.md')
    $videosIndex.Add('---')
    $videosIndex.Add('title: Videos')
    $videosIndex.Add('---')
    $videosIndex.Add('')
    $videosIndex.Add('# Videos')
    $videosIndex.Add('')
    $videosIndex.Add('Each video explains one idea about Orleans.Lattice in a few minutes, with English')
    $videosIndex.Add('captions. Its companion page has the full transcript, and any code on screen is')
    $videosIndex.Add('compiled with the rest of the documentation.')
    $videosIndex.Add('')

    foreach ($path in $videoPaths.Keys) {
        $members = @($episodes | Where-Object { $_.Path -eq $path } | Sort-Object Order)
        if ($members.Count -eq 0) { continue }
        $label = $videoPaths[$path]
        $videosToc.Add("- name: $(ConvertTo-YamlString $label)")
        $videosToc.Add('  items:')
        $videosIndex.Add("## $label")
        $videosIndex.Add('')
        $videosIndex.Add('<ol class="lt-episodes">')
        foreach ($episode in $members) {
            $videosToc.Add("  - name: $(ConvertTo-YamlString $episode.Title)")
            $videosToc.Add("    href: $($episode.Page)")
            # The poster is a second way to the same page, so it is kept out of
            # the tab order and the accessibility tree; the title is the link.
            $poster = "<a class=`"lt-episode-poster`" href=`"$($episode.Page)`" tabindex=`"-1`" aria-hidden=`"true`"><img src=`"../../media/$($episode.Slug)-$($episode.Cut).jpg`" alt=`"`" width=`"1920`" height=`"1080`" loading=`"lazy`"></a>"
            $idea = if ($episode.Idea) { (Get-Separator ': ') + "<span class=`"lt-episode-idea`">$(ConvertTo-HtmlText $episode.Idea)</span>" } else { '' }
            $length = (Get-Separator ' (') + "<span class=`"lt-episode-length`"><time datetime=`"$(ConvertTo-IsoDuration $episode.Length)`">$($episode.Length)</time></span>" + (Get-Separator ')')
            $videosIndex.Add("<li>$poster<div class=`"lt-episode-body`"><a class=`"lt-episode-title`" href=`"$($episode.Page)`">$(Get-EncodedHtml $episode.Title)</a>$idea$length</div></li>")
        }
        $videosIndex.Add('</ol>')
        $videosIndex.Add('')
    }

    Set-Content -Path (Join-Path $videosStaged 'toc.yml') -Value $videosToc -Encoding utf8
    Set-Content -Path (Join-Path $videosStaged 'index.md') -Value $videosIndex -Encoding utf8
    $origins['docs/videos/index.md'] = Get-TreeUrl 'docs/videos'
}
elseif (Test-Path $videosStaged) {
    Remove-Item $videosStaged -Recurse -Force
}

# The home page's introduction: the front door's first published episode, placed
# after the first viewport, or nothing at all when none is published.
function Get-IntroductionSection {
    $front = @($episodes | Where-Object { $_.Path -eq 'front-door' } | Sort-Object Order)
    if ($front.Count -eq 0) { return '' }
    $episode = $front[0]
    $companion = "docs/videos/$($episode.Page)"
    $transcript = if ($episode.HasTranscript) { "$companion#transcript" } else { $companion }
    $lines = @(
        '<section class="lt-watch" aria-labelledby="lt-watch-title">',
        "<h2 id=`"lt-watch-title`">$(Get-EncodedHtml $episode.Title)</h2>",
        "<p class=`"lt-section-lede`">$(ConvertTo-HtmlText $episode.Idea)</p>",
        '<div class="lt-watch-grid">',
        (Get-VideoPlayer $episode 'media' ''),
        '<div class="lt-watch-notes">',
        '<dl class="lt-watch-facts">',
        "<dt>Length</dt><dd><time datetime=`"$(ConvertTo-IsoDuration $episode.Length)`">$($episode.Length)</time></dd>",
        '<dt>Captions</dt><dd>English</dd>',
        "<dt>Transcript</dt><dd><a href=`"$transcript`">On its companion page</a>, with the code it shows</dd>",
        "<dt>Download</dt><dd><a href=`"media/$($episode.Slug)-$($episode.Cut).mp4`">MP4</a>, $($episode.Size)</dd>",
        '</dl>',
        '<p class="lt-watch-more"><a href="docs/videos/index.md">All videos</a></p>',
        '</div>',
        '</div>',
        '</section>'
    )
    return $lines -join "`n"
}

# --- Site root TOC, which becomes the navbar ---
# "href: docs/" is a FOLDER reference, so DocFX treats docs/toc.yml as a separate
# navigation scope that drives the sidebar. Pointing at "docs/toc.yml" instead
# would MERGE all 47 package TOCs into the root TOC, and the modern template
# renders a root node with children as a navbar dropdown - producing a single
# dropdown taller than the viewport.
#
# The home page (index.md, authored under docs-site/pages) is reached from the
# brand mark, so the navbar starts at the overview.
#
# Videos is a folder reference too, so docs/videos/toc.yml drives its sidebar,
# and it appears only when at least one episode is published.
$rootToc = New-Object System.Collections.Generic.List[string]
$rootToc.AddRange([string[]]@(
    '- name: Overview',
    '  href: README.md',
    '- name: Docs',
    '  href: docs/',
    '- name: Samples',
    '  href: samples/'
))
if ($episodes.Count -gt 0) {
    $rootToc.Add('- name: Videos')
    $rootToc.Add('  href: docs/videos/')
}
$rootToc.AddRange([string[]]@(
    '- name: Features',
    '  href: FEATURES.md',
    '- name: Packages',
    '  href: PACKAGES.md',
    '- name: Architecture',
    '  href: reference-architecture.md',
    '- name: Changelog',
    '  href: CHANGELOG.md'
))
Set-Content -Path (Join-Path $Staging 'toc.yml') -Value $rootToc -Encoding utf8

# --- The site's own pages, staged last ---
# docs-site/pages holds pages that exist only on the site - the home page. They
# are copied AFTER the github.com rewrite above, so their links are never
# redirected: a link that does not resolve stays broken and fails the
# zero-warning gate, which is what a curated route through the corpus needs.
#
# A page may carry generated sections, marked "<!-- lattice:NAME -->" on a line
# of their own, filled here from the same catalogue as the navigation.
function Get-SeamSummary {
    $html = New-Object System.Collections.Generic.List[string]
    $html.Add('<ul class="lt-seam-list">')
    foreach ($section in $catalogue) {
        if ($section.Rows.Count -eq 0) { continue }
        $names = @($section.Rows | ForEach-Object {
            if ($_.Id -eq 'Orleans.Lattice') { $_.Id } else { $_.Id -replace '^Orleans\.Lattice\.', '' }
        })
        $shown = ($names | Select-Object -First 4) -join ', '
        if ($names.Count -gt 4) { $shown += ", and $($names.Count - 4) more" }
        $count = if ($names.Count -eq 1) { '1 package' } else { "$($names.Count) packages" }
        $status = if ($section.InProgress) { Get-StatusBadge 'in progress' } else { '' }
        $html.Add("<li><a class=`"lt-seam-name`" href=`"docs/index.md#$(ConvertTo-Slug $section.Name)`">$([System.Net.WebUtility]::HtmlEncode($section.Name))</a>$status$(Get-Separator ': ')<span class=`"lt-seam-count`">$count</span>$(Get-Separator ' - ')<span class=`"lt-seam-members`">$([System.Net.WebUtility]::HtmlEncode($shown))</span></li>")
    }
    $html.Add('</ul>')
    return $html -join "`n"
}

# The authored home page's own labels get the same separators as the generated
# lists. Applied to the staged copy only: docs-site/pages/index.md keeps the
# exact markup the video series reads it by (videos/tools/lib/home.js).
function Add-HomeSeparators([string]$Text) {
    $Text = [regex]::Replace($Text, '<span class="lt-way-name">(?<name>[^<]*)</span><span class="lt-way-for">(?<for>.*?)</span></a>', {
        param($match)
        "<span class=`"lt-way-name`">$($match.Groups['name'].Value)</span>$(Get-Separator ': ')<span class=`"lt-way-for`">$($match.Groups['for'].Value)</span>$(Get-Separator '.')</a>"
    })
    $Text = [regex]::Replace($Text, '(?<label><span class="lt-invariant-label">[^<]*</span>)(?<gap1>\s*)(?<code><code>[^<]*</code>)(?<gap2>\s*)(?=<span class="lt-invariant-note">)', {
        param($match)
        $match.Groups['label'].Value + (Get-Separator ': ') + $match.Groups['gap1'].Value + $match.Groups['code'].Value + (Get-Separator ', ') + $match.Groups['gap2'].Value
    })
    return [regex]::Replace($Text, '<span class="lt-status">(?<status>[^<]*)</span>', { param($match) Get-StatusBadge $match.Groups['status'].Value })
}

$pagesSource = Join-Path $PSScriptRoot 'pages'
if (Test-Path $pagesSource) {
    # The home page draws the G-Counter figure, captioned with where it comes from.
    $homeSpec = $figureSpecs | Where-Object { $_.id -eq 'gcounter' } | Select-Object -First 1
    if (-not $homeSpec) { throw 'docs-site/figures/join-figures.json has no gcounter figure, which the home page draws.' }
    $homeCaption = 'The <a href="docs/crdt/gcounter.md">G-Counter</a> example from the CRDT guide. ' + (Get-EncodedHtml $homeSpec.rule)
    $generated = @{
        'seams'        = (Get-SeamSummary)
        'join-figure'  = (Get-JoinFigure -Spec $homeSpec -Width 460 -CaptionHtml $homeCaption)
        'introduction' = (Get-IntroductionSection)
    }
    Get-ChildItem $pagesSource -Recurse -File | ForEach-Object {
        $relative = ConvertTo-SiteRelative $_.FullName $pagesSource
        $destination = Join-Path $Staging $relative
        New-Item -ItemType Directory -Path (Split-Path $destination) -Force | Out-Null
        if ($_.Extension -ne '.md') { Copy-Item $_.FullName $destination; return }
        $text = Get-Content $_.FullName -Raw
        foreach ($name in $generated.Keys) {
            $text = $text.Replace("<!-- lattice:$name -->", $generated[$name])
        }
        if ($text -match '<!-- lattice:([a-z-]+) -->') {
            throw "$($_.Name) asks for a generated section 'lattice:$($Matches[1])' that stage.ps1 does not produce."
        }
        $text = Add-HomeSeparators $text
        Set-Content -Path $destination -Value $text -NoNewline -Encoding utf8
        $origins[$relative] = Get-SourceUrl "docs-site/pages/$relative"
    }
}

# --- Links to the headings of split pages, from every page ---
# Only a page that names a split page and an anchor can need this, which is
# checked cheaply first.
$splitTargets = @($splits.Keys | ForEach-Object { [System.IO.Path]::GetFileName($_) + '#' } | Sort-Object -Unique)
$retargeted = 0
foreach ($file in @(Get-ChildItem $Staging -Recurse -Filter *.md)) {
    $text = [System.IO.File]::ReadAllText($file.FullName)
    if (-not ($splitTargets | Where-Object { $text.Contains($_) } | Select-Object -First 1)) { continue }
    Update-SplitPageLinks $Staging (ConvertTo-SiteRelative $file.FullName $Staging) $splits
    if ([System.IO.File]::ReadAllText($file.FullName) -ne $text) { $retargeted++ }
}
Write-Host "Pointed links on $retargeted page(s) at the pages split sections moved to"

# --- Every page's source link ---
# DocFX writes docurl from the front matter when a page sets it, and from the
# staged file's own path otherwise, which is docs-site/src/... and 404s. A page
# staged without a recorded origin fails the build here, rather than shipping
# that broken link.
$stagedPages = @(Get-ChildItem $Staging -Recurse -Filter *.md | ForEach-Object { ConvertTo-SiteRelative $_.FullName $Staging } | Sort-Object)
$utf8 = New-Object System.Text.UTF8Encoding $false
foreach ($relative in $stagedPages) {
    $url = $origins[$relative]
    if (-not $url) { throw "stage.ps1 staged $relative without recording its source. Set `$origins['$relative'] where the page is staged, so its source link names a file in the repository." }
    $file = Join-Path $Staging $relative
    $text = [System.IO.File]::ReadAllText($file)
    [System.IO.File]::WriteAllText($file, (Set-FrontMatterValues $text ([ordered]@{ docurl = $url })), $utf8)
}

# --- The markdown alternates ---
# Beside every rendered page the site publishes its markdown, so a reader that
# simplifies HTML - and drops a table, a list, or a link as it does - can read
# the page itself instead (build.ps1 copies these into the site and links each
# page to its alternate). The page's own markdown is kept as it is; the HTML the
# site adds is turned back into markdown (lib/agent.ps1); and a header says what
# the page is, where its source is, and which release it documents - and, on a
# package's page, which version of that package, since a package can be on a
# different patch from the release the site is named for.
$agentRoot = Join-Path $Intermediate 'agent'
$pageInfo = @{}
$pageByLower = @{}
foreach ($relative in $stagedPages) {
    $text = [System.IO.File]::ReadAllText((Join-Path $Staging $relative))
    $front = Split-FrontMatter $text
    $title = Get-FrontMatterValue $front.Lines 'title'
    if (-not $title) {
        # Assigned before it is filtered: the function returns its list as one
        # object, so piping its output would filter the list, not its headings.
        $headings = Get-MarkdownHeadings @($front.Body -split '\r?\n')
        $h1 = $headings | Where-Object { $_.Level -eq 1 } | Select-Object -First 1
        $title = if ($h1) { Get-HeadingPlainText $h1.Text } else { [System.IO.Path]::GetFileNameWithoutExtension($relative) }
    }
    $pageInfo[$relative] = [pscustomobject]@{ Title = $title; Body = (ConvertTo-AgentMarkdown $front.Body) }
    $pageByLower[$relative.ToLowerInvariant()] = $relative
}

# The package a page documents, if it documents one: a page under docs/<dir>/,
# with the version this build documents (see $documentedVersions) or the status
# that stands in for one.
function Get-PagePackage([string]$Relative) {
    if ($Relative -notmatch '^docs/(?<dir>[^/]+)/') { return $null }
    $info = $dirInfo[$Matches.dir]
    if (-not $info -or -not $info.Id) { return $null }
    $version = if ($info.Exact -and -not $info.Status) { Get-DocumentedVersion $info.Id } else { $null }
    return [pscustomobject]@{ Id = $info.Id; Exact = $info.Exact; Version = $version; Status = $info.Status }
}

# The page a page sits under, for its "Part of" line: a package page under its
# package's landing page, a landing page under the documentation map, and any
# other page under the nearest index or README above it, up to the home page.
function Get-ParentPage([string]$Relative) {
    if ($Relative -eq 'index.md') { return $null }
    if ($Relative -match '^docs/(?<dir>[^/]+)/[^/]+$' -and $dirInfo.ContainsKey($Matches.dir)) {
        $info = $dirInfo[$Matches.dir]
        $landing = if ($info.Landing) { "docs/$($Matches.dir)/$($info.Landing)" } else { $null }
        if ($landing -and $landing -ne $Relative -and $pageInfo.ContainsKey($landing)) {
            return [pscustomobject]@{ Path = $landing; Text = "the [$($info.Display) documentation]" }
        }
        return [pscustomobject]@{ Path = 'docs/index.md'; Text = 'the [documentation map]' }
    }
    $directory = Get-SiteDirectory $Relative
    while ($true) {
        foreach ($name in @('index.md', 'README.md')) {
            $key = if ($directory) { "$directory/$name" } else { $name }
            $candidate = $pageByLower[$key.ToLowerInvariant()]
            if (-not $candidate -or $candidate -eq $Relative) { continue }
            $text = if ($candidate -eq 'index.md') { 'the [Orleans.Lattice documentation]' }
                elseif ($candidate -eq 'docs/index.md') { 'the [documentation map]' }
                else { '[' + $pageInfo[$candidate].Title + ']' }
            return [pscustomobject]@{ Path = $candidate; Text = $text }
        }
        if (-not $directory) { return $null }
        $directory = Get-SiteDirectory $directory
    }
}

# A "Part of" line under a markdown body's title, as split pages already carry.
function Add-PartOfLine([string]$Body, [string]$Line) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.AddRange([string[]]@($Body -split "`n"))
    $h1 = -1
    for ($n = 0; $n -lt $lines.Count; $n++) {
        if ($lines[$n] -match '^#\s') { $h1 = $n; break }
        if (Get-FenceCloser $lines[$n]) { break }
    }
    if ($h1 -lt 0) { return "$Line`n`n$Body" }
    $next = $h1 + 1
    while ($next -lt $lines.Count -and $lines[$next].Trim() -eq '') { $next++ }
    if ($next -lt $lines.Count -and $lines[$next].StartsWith('Part of ')) { return $Body }
    $insert = @('', $Line)
    if ($h1 + 1 -ge $lines.Count -or $lines[$h1 + 1].Trim() -ne '') { $insert += '' }
    $lines.InsertRange($h1 + 1, [string[]]$insert)
    return ($lines -join "`n")
}

# What a page's note says: which release, and which package version, the page
# documents, and where its markdown and the index of every page are. build.ps1
# places it under the page's title, visually hidden, for the readers that never
# see the page's head, where the alternate link is, or its footer, where the
# version is: a screen reader, and any tool that reads a page as text. It is a
# span, not a paragraph: those tools score paragraphs to find a page's main
# content, and a note that moved that choice could cost a page some of its text.
# Its links are out of the tab order, as the video posters' are: the note cannot
# be seen, so a keyboard user tabbing onto them would lose sight of the focus
# for two stops on every page. A screen reader still reads and follows them.
function Get-PageNote([string]$Relative, $Package) {
    $scope = "the documentation for $($site.Label), built $($site.BuiltDate)"
    $opening = if ($Package -and $Package.Exact -and $Package.Id -ne 'Orleans.Lattice') {
        if ($Package.Version) { "This page documents $($Package.Id) $($Package.Version), in $scope." }
        elseif ($Package.Status) { "This page documents $($Package.Id), which is $($Package.Status), in $scope." }
        else { "This page documents $($Package.Id), in $scope." }
    }
    elseif ($Package -and -not $Package.Exact -and $Package.Status) { "This page documents the $($Package.Id) packages, which are $($Package.Status), in $scope." }
    else { "This page is part of $scope." }
    $markdownUrl = Get-EncodedHtml ($site.Url + $Relative)
    $name = Get-EncodedHtml ([System.IO.Path]::GetFileName($Relative))
    return '<span class="visually-hidden lt-page-note">' + (Get-EncodedHtml $opening) +
        ' It is also published as markdown, with every table and list, at <a href="' + $markdownUrl + '" tabindex="-1">' + $name +
        '</a>, and <a href="' + (Get-EncodedHtml ($site.Url + 'llms.txt')) + '" tabindex="-1">llms.txt</a> lists every page.</span>'
}

$splitPages = New-Object 'System.Collections.Generic.HashSet[string]'
foreach ($result in $splits.Values) { foreach ($entry in $result.Pages) { [void]$splitPages.Add($entry.Path) } }

# The file that holds every page of the documentation set a page belongs to - a
# package's, or the CRDT guide's - written with llms.txt, which lists it with
# the set.
function Get-BundlePath([string]$Relative) {
    if ($Relative -notmatch '^docs/(?<dir>[^/]+)/' -or -not $dirInfo.ContainsKey($Matches.dir)) { return $null }
    return 'docs/' + $Matches.dir + '/llms-full.txt'
}

$pageNotes = [ordered]@{}
foreach ($relative in $stagedPages) {
    $info = $pageInfo[$relative]
    $package = Get-PagePackage $relative
    $header = New-Object System.Collections.Generic.List[string]
    $header.Add('---')
    $header.Add("title: $(ConvertTo-QuotedYaml $info.Title)")
    $header.Add("url: $(ConvertTo-QuotedYaml ($site.Url + [System.IO.Path]::ChangeExtension($relative, '.html')))")
    $header.Add("source: $(ConvertTo-QuotedYaml $origins[$relative])")
    if ($package) {
        $packageName = if ($package.Exact) { $package.Id } else { $package.Id + '.*' }
        $header.Add('package: ' + (ConvertTo-QuotedYaml $packageName))
        if ($package.Version) { $header.Add('version: ' + (ConvertTo-QuotedYaml $package.Version)) }
        if ($package.Status) { $header.Add('status: ' + (ConvertTo-QuotedYaml $package.Status)) }
    }
    $header.Add("documents: $(ConvertTo-QuotedYaml $site.Label)")
    $header.Add("built: $(ConvertTo-QuotedYaml $site.BuiltDate)")
    $header.Add("all-pages: $(ConvertTo-QuotedYaml ($site.Url + 'llms.txt'))")
    $bundlePath = Get-BundlePath $relative
    if ($bundlePath) { $header.Add('bundle: ' + (ConvertTo-QuotedYaml ($site.Url + $bundlePath))) }
    $header.Add('---')
    $header.Add('')

    # A split page opens with where it belongs already; every other page gains
    # the same line, since the rendered page's sidebar is not in its markdown.
    $body = $info.Body
    $parent = if ($splitPages.Contains($relative)) { $null } else { Get-ParentPage $relative }
    if ($parent) {
        $link = Get-SiteRelativeLink (Get-SiteDirectory $relative) $parent.Path
        $body = Add-PartOfLine $body ('Part of ' + $parent.Text + '(' + $link + ').')
    }

    $target = Join-Path $agentRoot $relative
    New-Item -ItemType Directory -Path (Split-Path $target) -Force | Out-Null
    [System.IO.File]::WriteAllText($target, ($header -join "`n") + $body, $utf8)
    $pageNotes[$relative] = Get-PageNote $relative $package
}
[System.IO.File]::WriteAllText((Join-Path $Intermediate 'page-notes.json'), ($pageNotes | ConvertTo-Json -Compress), $utf8)
Write-Host "Wrote $($stagedPages.Count) markdown alternate(s) under $agentRoot, and each page's note"

# --- llms.txt: the site's entry point for agents and LLM tooling ---
# Generated from the same catalogue as the documentation map and the sidebar
# (PACKAGES.md, each package's pages, FEATURES.md's samples), so it cannot
# drift from them. It follows https://llmstxt.org: a title, a summary, then
# sections of links to each page's markdown alternate, grouped the way the site
# groups them, with the history and the contributor material under "Optional".
# Every page on the site is listed, by title, and the build fails if one is not.
# A package's landing page also carries the package's description from
# PACKAGES.md, and a page that is part of another - a section of a split page, a
# release, a sample's source - follows the page it belongs to; the release
# history, the samples' source and the pages beyond the documentation are under
# "Optional", apart from a sample whose source is its only page.
#
# Each documentation set - a package's pages, or the CRDT guide's - is also
# written to one file of its own, docs/<package>/llms-full.txt, listed with the
# set, so an agent can take one package's documentation in a fetch or two
# rather than the whole site's in llms-full.txt.
function Get-PageDescription([string]$Relative, [int]$Max = 110) {
    $file = Join-Path $Staging $Relative
    if (-not (Test-Path -LiteralPath $file)) { return $null }
    $body = @((Split-FrontMatter ([System.IO.File]::ReadAllText($file))).Body -split '\r?\n')
    $h1 = [array]::FindIndex($body, [Predicate[string]]{ param($l) $l -match '^#\s' })
    return Get-RangeDescription $body ($h1 + 1) $body.Count $Max
}

$llms = New-Object System.Collections.Generic.List[string]
$listed = New-Object 'System.Collections.Generic.HashSet[string]'
$bundleSizes = [ordered]@{}
# A description is taken from the page it describes, so a link in it that is
# relative to that page would resolve from the site root here; it keeps its text.
function ConvertTo-LlmsDescription([string]$Text) {
    if (-not $Text) { return $Text }
    return [regex]::Replace($Text, '(?<!!)\[(?<text>[^\]]+)\]\((?<target>[^)\s]+)\)', {
        param($match)
        if ($match.Groups['target'].Value -match '^[a-z][a-z0-9+.-]*:') { return $match.Value }
        return $match.Groups['text'].Value
    })
}
function Add-LlmsLink([string]$Relative, [string]$Name, [string]$Description) {
    if (-not $pageInfo.ContainsKey($Relative) -or -not $listed.Add($Relative)) { return }
    $item = "- [$Name]($($site.Url)$Relative)"
    if ($Description) { $item += ': ' + (ConvertTo-LlmsDescription $Description) }
    $llms.Add($item)
}
# What a part of a split page holds. A run of subsections lists their names:
# its title names only the first and the last, in the page's order, which is not
# alphabetical, and a run of one is named by its title already. Any other part
# gives its first sentence.
function Get-LlmsPartDescription($Entry) {
    if ($Entry.Chunk) {
        if ($Entry.Headings.Count -lt 2) { return $null }
        $shown = @($Entry.Headings | Select-Object -First 60)
        $description = 'Sections: ' + ($shown -join ', ')
        if ($Entry.Headings.Count -gt $shown.Count) { $description += ", and $($Entry.Headings.Count - $shown.Count) more" }
        return $description + '.'
    }
    if ($Entry.Description) { return Get-ShortDescription $Entry.Description 160 }
    return $null
}
# A split page's parts, each named after the page it belongs to.
function Add-LlmsSplit([string]$Relative, [string]$Prefix) {
    if (-not $splits.ContainsKey($Relative)) { return }
    foreach ($entry in $splits[$Relative].Pages) {
        Add-LlmsLink $entry.Path "${Prefix}: $($entry.Title)" (Get-LlmsPartDescription $entry)
    }
}
# The page a line of llms.txt links to, site-relative, or $null for any other line.
function Get-LlmsLinePage([string]$Line) {
    if ($Line -notmatch '^- \[[^\]]*\]\((?<url>[^)]+)\)') { return $null }
    $url = $Matches.url
    if (-not $url.StartsWith($site.Url)) { return $null }
    $relative = ($url.Substring($site.Url.Length) -split '#')[0]
    if (-not $pageInfo.ContainsKey($relative)) { return $null }
    return $relative
}
# A file's size as a reader weighs a fetch.
function Format-FileSize([long]$Bytes) {
    $invariant = [Globalization.CultureInfo]::InvariantCulture
    if ($Bytes -ge 1MB) { return ($Bytes / 1MB).ToString('0.0', $invariant) + ' MB' }
    return ([Math]::Max(1, [Math]::Round($Bytes / 1KB))).ToString($invariant) + ' KB'
}
# Pages one after another in one file, each preceded by its address. Returns the
# file's size in bytes.
function Write-LlmsBundle([string]$Path, [string]$Heading, [string]$Intro, [string[]]$Pages) {
    $text = New-Object System.Text.StringBuilder
    [void]$text.Append("# $Heading`n`n$Intro`n")
    foreach ($page in $Pages) {
        [void]$text.Append("`n" + ('-' * 80) + "`n`n")
        [void]$text.Append("URL: $($site.Url)$page`n`n")
        [void]$text.Append($pageInfo[$page].Body)
    }
    $target = Join-Path $Staging $Path
    New-Item -ItemType Directory -Path (Split-Path $target) -Force | Out-Null
    [System.IO.File]::WriteAllText($target, $text.ToString(), $utf8)
    return (Get-Item -LiteralPath $target).Length
}

# The README's opening paragraphs are the platform's own summary.
$readmeLines = @(Get-Content (Join-Path $RepoRoot 'README.md'))
$summary = New-Object System.Collections.Generic.List[string]
$paragraph = New-Object System.Collections.Generic.List[string]
foreach ($readmeLine in ($readmeLines | Select-Object -Skip 1)) {
    if ($readmeLine -match '^\s*(#|\||!\[|\[!\[)') { if ($summary.Count) { break }; continue }
    if ($readmeLine.Trim() -eq '') {
        if ($paragraph.Count) { $summary.Add(($paragraph -join ' ')); $paragraph.Clear() }
        if ($summary.Count -ge 2) { break }
        continue
    }
    $paragraph.Add($readmeLine.Trim())
}

$llms.Add('# Orleans.Lattice')
$llms.Add('')
$llms.Add("> $($summary -join ' ')")
$llms.Add('')
$commitNote = if ($site.ShortCommit) { ' at commit `' + $site.ShortCommit + '`' } else { '' }
# Written at the end, once the files it describes, and their sizes, exist.
$llmsIntro = $llms.Count
$llms.Add('')
$llms.Add('')

$llms.Add('## Start here')
$llms.Add('')
Add-LlmsLink 'index.md' 'Home' 'The platform in one page, the three reading paths (Build, Evaluate, Operate), and the deployment journey from one machine to many regions.'
Add-LlmsLink 'README.md' 'Overview' (Get-PageDescription 'README.md')
Add-LlmsLink 'FEATURES.md' 'Features' 'Every capability, grouped by concern, with its documentation and a runnable sample where one exists.'
Add-LlmsLink 'PACKAGES.md' 'Packages' 'Every package, grouped by the seam it fills, with its published version and its documentation.'
Add-LlmsLink 'docs/index.md' 'Documentation map' 'Every package''s documentation, grouped by seam, with its status and page count.'
Add-LlmsLink 'samples/index.md' 'Samples' 'Runnable projects exercising the platform, grouped by concern.'
Add-LlmsLink 'reference-architecture.md' 'Reference architecture' (Get-PageDescription 'reference-architecture.md')
$llms.Add('')

# The home page's three reading paths, in their own order.
$homeSource = Join-Path $PSScriptRoot 'pages/index.md'
if (Test-Path $homeSource) {
    $homeText = Get-Content $homeSource -Raw
    foreach ($path in [regex]::Matches($homeText, '(?s)<div class="lt-path" id="(?<id>[a-z-]+)">\s*<h3>(?<name>[^<]+)</h3>\s*<p class="lt-path-for">(?<for>.*?)</p>(?<list>.*?)</div>')) {
        $llms.Add("## Reading path: $($path.Groups['name'].Value)")
        $llms.Add('')
        foreach ($step in [regex]::Matches($path.Groups['list'].Value, '(?m)^\d+\.\s+\[(?<text>[^\]]+)\]\((?<href>[^)#]+)(?<anchor>#[^)]*)?\)\s*(?<note>.*?)\s*$')) {
            # A step's note, less its cross-reference to the other path ("Also on
            # Operate"), with its status pill read as the page reads it.
            $note = [regex]::Replace($step.Groups['note'].Value, '<span class="lt-shared">[^<]*</span>', '')
            $note = [regex]::Replace($note, '<span class="lt-status">(?<status>[^<]*)</span>', '(${status})')
            $note = [regex]::Replace($note, '<[^>]+>', '').Trim()
            $llms.Add("- [$($step.Groups['text'].Value)]($($site.Url)$($step.Groups['href'].Value)$($step.Groups['anchor'].Value)): $note")
        }
        $llms.Add('')
    }
}

# Every package's pages, grouped as the documentation map groups them.
foreach ($section in $sectionOrder) {
    $members = @($packageDirs | Where-Object { $dirInfo[$_.Name].Section -eq $section })
    if ($members.Count -eq 0) { continue }
    $llms.Add("## $section")
    $llms.Add('')
    foreach ($dir in $members) {
        $info = $dirInfo[$dir.Name]
        $name = if ($info.Status) { "$($info.Display) ($($info.Status))" } else { $info.Display }
        $first = $llms.Count
        if ($info.Landing) {
            $landing = "docs/$($dir.Name)/$($info.Landing)"
            # A package opens with its id and the version these pages document.
            $lead = ''
            if ($info.Id) {
                $lead = "``$($info.Id)``"
                $documented = if ($info.Exact -and -not $info.Status) { Get-DocumentedVersion $info.Id } else { $null }
                if ($documented) { $lead += " $documented" }
                $lead += '. '
            }
            Add-LlmsLink $landing $name ($lead + $(if ($info.Description) { Get-ShortDescription $info.Description 200 } else { '' })).Trim()
            Add-LlmsSplit $landing $name
        }
        foreach ($entry in $packagePages[$dir.Name]) {
            $relative = "docs/$($dir.Name)/$($entry.Name)"
            Add-LlmsLink $relative "${name}: $($entry.Title)" $null
            Add-LlmsSplit $relative "${name}: $($entry.Title)"
        }

        # The set's pages, in the order just listed, as one file, listed after
        # its first page. Every page under its directory is in it, or the file
        # would claim more than it holds.
        $bundlePages = @($llms.GetRange($first, $llms.Count - $first) | ForEach-Object { Get-LlmsLinePage $_ } | Where-Object { $_ })
        $prefix = "docs/$($dir.Name)/"
        $expected = @($stagedPages | Where-Object { $_.StartsWith($prefix) }).Count
        if ($bundlePages.Count -eq 0 -or $bundlePages.Count -ne $expected) {
            throw "The one-file copy of $prefix would hold $($bundlePages.Count) of its $expected page(s); every page of a package must be listed in its llms.txt section."
        }
        $about = if ($info.Id -and $info.Exact) { "``$($info.Id)``" } elseif ($info.Id) { "the ``$($info.Id).*`` packages" } else { $info.Display }
        $documented = if ($info.Exact -and -not $info.Status) { Get-DocumentedVersion $info.Id } else { $null }
        if ($documented) { $about += " $documented" }
        elseif ($info.Status) { $about += $(if ($info.Exact) { ', which is ' } else { ', which are ' }) + $info.Status }
        $pageCount = if ($bundlePages.Count -eq 1) { '1 page' } else { "$($bundlePages.Count) pages" }
        $bundleIntro = "The documentation for ${about}: $pageCount, in the order $($site.Url)llms.txt lists them, each preceded by its address. It is part of the documentation site for $($site.Label), built $($site.BuiltDate) from ``$($site.Ref)``; llms.txt lists every page of the site."
        $bundlePath = "${prefix}llms-full.txt"
        $bundleBytes = Write-LlmsBundle $bundlePath "${name}: every page in one file" $bundleIntro $bundlePages
        $bundleSizes[$bundlePath] = $bundleBytes
        $llms.Insert($first + 1, "- [${name}: every page in one file]($($site.Url)$bundlePath): $pageCount, $(Format-FileSize $bundleBytes).")
    }
    $llms.Add('')
}

if ($sampleEntries.Count -gt 0) {
    $llms.Add('## Samples')
    $llms.Add('')
    foreach ($sample in $sampleEntries) {
        $sampleDescription = if ($sample.Summary) { Get-ShortDescription $sample.Summary 160 } else { $null }
        if ($sample.Readme) {
            Add-LlmsLink "samples/$($sample.Name)/README.md" $sample.Name $sampleDescription
            Add-LlmsSplit "samples/$($sample.Name)/README.md" $sample.Name
        }
        else {
            Add-LlmsLink "samples/$($sample.Name)/source.md" "$($sample.Name): source" $sampleDescription
            Add-LlmsSplit "samples/$($sample.Name)/source.md" "$($sample.Name): source"
        }
    }
    $llms.Add('')
}

if ($episodes.Count -gt 0) {
    $llms.Add('## Videos')
    $llms.Add('')
    Add-LlmsLink 'docs/videos/index.md' 'Videos' 'Every published episode, grouped by reading path.'
    foreach ($episode in ($episodes | Sort-Object Path, Order)) {
        Add-LlmsLink "docs/videos/$($episode.Page)" $episode.Title "$($episode.Idea) Transcript and on-screen code, $($episode.Length)."
    }
    $llms.Add('')
}

$llms.Add('## Optional')
$llms.Add('')
Add-LlmsLink 'CHANGELOG.md' 'Changelog' 'Release history for the package family, newest first, with one page per release.'
# Every release, newest first, each with the versions it shipped, and any part
# a long release is split into.
$releases = $splits['CHANGELOG.md']
if ($releases) {
    $newest = if ($releases.Children.Count -gt 0) { $releases.Children[0].Path } else { $null }
    foreach ($entry in $releases.Pages) {
        $releaseName = if ($entry.Path -eq $newest) { $entry.Title + ' (newest)' } else { $entry.Title }
        Add-LlmsLink $entry.Path $releaseName (Get-LlmsPartDescription $entry)
    }
}
Add-LlmsLink 'docs/RELEASING.md' 'Releasing' (Get-PageDescription 'docs/RELEASING.md')
# Every sample's source, and the page per file a long listing is split into.
foreach ($sample in $sampleEntries) {
    if (-not $sample.Source) { continue }
    Add-LlmsLink "samples/$($sample.Name)/source.md" "$($sample.Name): source" $null
    Add-LlmsSplit "samples/$($sample.Name)/source.md" "$($sample.Name): source"
}
# Anything else on the site - the specification pages the
# documentation links into - each followed by its parts, and the parts of any
# split page listed above whose parts were not.
foreach ($relative in $stagedPages) {
    if ($splitPages.Contains($relative)) { continue }
    Add-LlmsLink $relative $pageInfo[$relative].Title $null
    Add-LlmsSplit $relative $pageInfo[$relative].Title
}
$llms.Add("- [AGENTS.md]($repositoryUrl/blob/$($site.Ref)/AGENTS.md): Build and test commands, conventions, and hygiene gates for agents changing the repository.")
$llms.Add("- [Repository conventions]($repositoryUrl/blob/$($site.Ref)/.github/copilot-instructions.md): Naming, serialization, branching, and pull-request rules for contributors.")

# Every page is listed: llms.txt says so.
$missing = @($stagedPages | Where-Object { -not $listed.Contains($_) })
if ($missing.Count -gt 0) { throw "llms.txt leaves out $($missing.Count) page(s) the site publishes: $($missing -join ', ')" }

# llms-full.txt: every documentation page's markdown, in llms.txt's order. The
# release history and the samples' source listings are left out - they are the
# bulk of the site, and llms.txt lists each of their pages.
$fullPages = New-Object System.Collections.Generic.List[string]
$included = New-Object 'System.Collections.Generic.HashSet[string]'
foreach ($line in $llms) {
    $relative = Get-LlmsLinePage $line
    if (-not $relative) { continue }
    if ($relative -like 'changelog/*' -or $relative -eq 'CHANGELOG.md' -or $relative -match '^samples/[^/]+/source(/|\.md$)') { continue }
    if ($included.Add($relative)) { $fullPages.Add($relative) }
}
$fullIntro = "Every documentation page of the site for $($site.Label), built $($site.BuiltDate) from ``$($site.Ref)``, in the order $($site.Url)llms.txt lists them, each preceded by its address. The release history and the samples' source are left out; llms.txt lists each of their pages. Each package's documentation is also in one file of its own, docs/<package>/llms-full.txt, which llms.txt lists with the package."
$fullBytes = Write-LlmsBundle 'llms-full.txt' 'Orleans.Lattice documentation, in full' $fullIntro $fullPages

$llms[$llmsIntro] = "This index lists every page of the documentation site for $($site.Label), built $($site.BuiltDate) from ``$($site.Ref)``$commitNote. It is generated from the same catalogue as the site's [documentation map]($($site.Url)docs/index.md), so it lists every package and every page. The release history, the samples' source and the pages beyond the documentation are listed page by page under Optional, apart from a sample whose source is its only page, which is listed under Samples. Each page's link is to its markdown; the rendered page is at the same address ending in ``.html``. Each package's documentation is also in one file, ``docs/<package>/llms-full.txt``, listed with the package below, and [llms-full.txt]($($site.Url)llms-full.txt) holds every documentation page in one file of $(Format-FileSize $fullBytes). [sitemap.xml]($($site.Url)sitemap.xml) lists every rendered page."
[System.IO.File]::WriteAllText((Join-Path $Staging 'llms.txt'), ($llms -join "`n") + "`n", $utf8)
$largest = $bundleSizes.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 1
Write-Host ("Wrote llms.txt ({0} pages, {1}), llms-full.txt ({2} pages, {3}), and {4} package file(s), the largest {5} ({6})" -f $listed.Count, (Format-FileSize (Get-Item (Join-Path $Staging 'llms.txt')).Length), $fullPages.Count, (Format-FileSize $fullBytes), $bundleSizes.Count, $largest.Key, (Format-FileSize $largest.Value))

# --- The footer's version line ---
# docfx.json's footer carries a marker where this goes, and the rendered footer
# comes from this file: DocFX gives globalMetadataFiles precedence over the
# globalMetadata written inline, and build.ps1 checks the line reached the page.
$footer = [string]$docfxConfig.build.globalMetadata._appFooter
$marker = '<!-- lattice:docs-version -->'
if (-not $footer.Contains($marker)) { throw "docfx.json's _appFooter has no $marker, which is where the docs version is written." }
$refHtml = "<a href=`"$repositoryUrl/tree/$($site.Ref)`">$(Get-EncodedHtml $site.Ref)</a>"
$commitHtml = if ($site.Commit) { " at commit <a href=`"$repositoryUrl/commit/$($site.Commit)`">$($site.ShortCommit)</a>" } else { '' }
$versionHtml = "<span class=`"lt-footer-version`">Documents $(Get-EncodedHtml $site.Label). Built <time datetime=`"$($site.Built.ToString('yyyy-MM-ddTHH:mm:ssZ', [Globalization.CultureInfo]::InvariantCulture))`">$($site.BuiltDate)</time> from $refHtml$commitHtml.</span>"
$metadata = [ordered]@{ _appFooter = $footer.Replace($marker, $versionHtml) }
[System.IO.File]::WriteAllText((Join-Path $Intermediate 'site-metadata.json'), ($metadata | ConvertTo-Json -Depth 3), $utf8)

$mdCount = (Get-ChildItem $Staging -Recurse -Filter *.md | Measure-Object).Count
$tocCount = (Get-ChildItem $Staging -Recurse -Filter toc.yml | Measure-Object).Count
Write-Host "Staged $mdCount markdown files and generated $tocCount TOC files under $Staging"
