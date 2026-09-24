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
#      them under, so the nav cannot drift as packages are added;
#   5. stages the site's own authored pages (docs-site/pages: the home page)
#      AFTER step 3, so a broken link in them is never rewritten away and fails
#      the zero-warning link gate instead.
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

# Site branding - the mark, favicon, fonts, and theme - lives in the DocFX
# template (docs-site/template/public), which DocFX copies into the output.

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
# Each scenario mirrors the Behaviour example on its page, and the figure is
# inserted at the top of that section of the STAGED page only. The tracked
# document is untouched, so it still renders on github.com exactly as before.
$figureSource = Join-Path $PSScriptRoot 'figures/join-figures.json'
if (-not (Test-Path $figureSource)) { throw "Missing $figureSource, which the home page and the CRDT explainers draw from." }
$figureSpecs = @((Get-Content $figureSource -Raw | ConvertFrom-Json).figures)

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
function Get-JoinFigure {
    param(
        [Parameter(Mandatory = $true)] $Spec,
        [int]$Width = 560,
        [string]$CaptionHtml,
        [switch]$Inline
    )

    $id = [string]$Spec.id
    $cx = [int]($Width / 2)
    $nodes = $Spec.nodes
    $edgeText = $Spec.edges
    $svg = New-Object System.Collections.Generic.List[string]
    $segments = New-Object System.Collections.Generic.List[object]
    $routes = @{}

    function Add-Text([string]$Class, [int]$X, [int]$Y, [string]$Value, [string]$Anchor) {
        if (-not $Value) { return }
        $anchorAttribute = if ($Anchor -and $Anchor -ne 'start') { " text-anchor=`"$Anchor`"" } else { '' }
        $svg.Add("<text class=`"$Class`" x=`"$X`" y=`"$Y`"$anchorAttribute>$(Get-EncodedHtml $Value)</text>")
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
    $html.Add("<title id=`"lt-join-title-$id`">$(Get-EncodedHtml $Spec.title)</title>")
    $html.Add("<desc id=`"lt-join-desc-$id`">$(Get-EncodedHtml $Spec.description)</desc>")
    foreach ($line in $svg) { $html.Add($line) }
    $html.Add('</svg>')
    $html.Add('<figcaption class="lt-join-caption">')
    $html.Add("<p class=`"lt-join-status`" data-lt-join-status aria-live=`"polite`">$(Get-EncodedHtml $Spec.settled)</p>")
    $html.Add('<div class="lt-join-controls">')
    $html.Add('<button type="button" class="lt-button" data-lt-join-replay>Replay the merge</button>')
    $html.Add("<button type=`"button`" class=`"lt-button lt-button-quiet`" data-lt-join-redeliver>$(Get-EncodedHtml $redeliver.label)</button>")
    $html.Add('</div>')
    $html.Add("<p class=`"lt-join-source`">$caption</p>")
    $html.Add('</figcaption>')
    $html.Add('</figure>')
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
    $figure = (Get-JoinFigure -Spec $spec -Inline) -replace "`n", $newline
    $text = $heading.Replace($text, { param($match) $match.Value + $newline + $newline + $figure }, 1)
    Set-Content -LiteralPath $page -Value $text -NoNewline -Encoding utf8
    $placedFigures++
}
Write-Host "Placed $placedFigures join figure(s) on CRDT explainer pages"

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
$packageDirs = Get-ChildItem $docsRoot -Directory | Sort-Object Name

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
# "TTL" rather than "Ttl". README first, then alphabetical by that title.
foreach ($dir in $packageDirs) {
    $info = $dirInfo[$dir.Name]
    $entries = Get-ChildItem -LiteralPath $dir.FullName -Filter *.md | ForEach-Object {
        $isReadme = $_.Name -ieq 'README.md'
        $title = if ($isReadme) { 'Overview' } else { Get-DocTitle $_.FullName $info.Id }
        if (-not $title) { $title = (Get-Culture).TextInfo.ToTitleCase(($_.BaseName -replace '-', ' ')) }
        [pscustomobject]@{ Name = $_.Name; Title = $title; Order = if ($isReadme) { '0' } else { '1' + $title.ToLowerInvariant() } }
    } | Sort-Object Order

    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($entry in $entries) {
        $lines.Add("- name: $(ConvertTo-YamlString $entry.Title)")
        $lines.Add("  href: $($entry.Name)")
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
        $status = if ($info.Status) { "<span class=`"lt-status`">$($info.Status)</span>" } else { '' }
        $count = if ($info.Count -eq 1) { '1 document' } else { "$($info.Count) documents" }
        $meta = if ($info.Id) { "<code>$($info.Id)</code> &middot; $count" } else { $count }
        $desc = if ($info.Description) { "<span class=`"lt-map-desc`">$(ConvertTo-HtmlText $info.Description)</span>" } else { '' }
        $index.Add("<li$class><span class=`"lt-map-name`"><a href=`"$($dir.Name)/$($info.Landing)`">$([System.Net.WebUtility]::HtmlEncode($info.Display))</a>$status</span><span class=`"lt-map-meta`">$meta</span>$desc</li>")
    }
    $index.Add('</ul>')
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
            $summary = if ($hasReadme) { Get-FirstSentence (Get-FirstParagraph (Join-Path $staged 'README.md')) } else { $null }
            $desc = if ($summary) { "<span class=`"lt-map-desc`">$(ConvertTo-HtmlText $summary)</span>" } else { '' }
            $source = if ($hasReadme -and $hasSource) { "<span class=`"lt-map-meta`"><a href=`"$($sample.Name)/source.md`">Source</a></span>" } else { '' }
            $indexEntries.Add("<li><span class=`"lt-map-name`"><a href=`"$landing`">$($sample.Name)</a></span>$source$desc</li>")
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
$rootToc = @(
    '- name: Overview',
    '  href: README.md',
    '- name: Docs',
    '  href: docs/',
    '- name: Samples',
    '  href: samples/',
    '- name: Features',
    '  href: FEATURES.md',
    '- name: Packages',
    '  href: PACKAGES.md',
    '- name: Architecture',
    '  href: reference-architecture.md',
    '- name: Changelog',
    '  href: CHANGELOG.md'
)
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
        $status = if ($section.InProgress) { '<span class="lt-status">in progress</span>' } else { '' }
        $html.Add("<li><a class=`"lt-seam-name`" href=`"docs/index.md#$(ConvertTo-Slug $section.Name)`">$([System.Net.WebUtility]::HtmlEncode($section.Name))</a>$status<span class=`"lt-seam-count`">$count</span><span class=`"lt-seam-members`">$([System.Net.WebUtility]::HtmlEncode($shown))</span></li>")
    }
    $html.Add('</ul>')
    return $html -join "`n"
}

$pagesSource = Join-Path $PSScriptRoot 'pages'
if (Test-Path $pagesSource) {
    # The home page draws the G-Counter figure, captioned with where it comes from.
    $homeSpec = $figureSpecs | Where-Object { $_.id -eq 'gcounter' } | Select-Object -First 1
    if (-not $homeSpec) { throw 'docs-site/figures/join-figures.json has no gcounter figure, which the home page draws.' }
    $homeCaption = 'The <a href="docs/crdt/gcounter.md">G-Counter</a> example from the CRDT guide. ' + (Get-EncodedHtml $homeSpec.rule)
    $generated = @{
        'seams'       = (Get-SeamSummary)
        'join-figure' = (Get-JoinFigure -Spec $homeSpec -Width 460 -CaptionHtml $homeCaption)
    }
    Get-ChildItem $pagesSource -Recurse -File | ForEach-Object {
        $destination = Join-Path $Staging (ConvertTo-SiteRelative $_.FullName $pagesSource)
        New-Item -ItemType Directory -Path (Split-Path $destination) -Force | Out-Null
        if ($_.Extension -ne '.md') { Copy-Item $_.FullName $destination; return }
        $text = Get-Content $_.FullName -Raw
        foreach ($name in $generated.Keys) {
            $text = $text.Replace("<!-- lattice:$name -->", $generated[$name])
        }
        if ($text -match '<!-- lattice:([a-z-]+) -->') {
            throw "$($_.Name) asks for a generated section 'lattice:$($Matches[1])' that stage.ps1 does not produce."
        }
        Set-Content -Path $destination -Value $text -NoNewline -Encoding utf8
    }
}

$mdCount = (Get-ChildItem $Staging -Recurse -Filter *.md | Measure-Object).Count
$tocCount = (Get-ChildItem $Staging -Recurse -Filter toc.yml | Measure-Object).Count
Write-Host "Staged $mdCount markdown files and generated $tocCount TOC files under $Staging"
