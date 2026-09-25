# The markdown alternate of every page: the page's own markdown, with the HTML
# the site adds - the home page's sections, the generated overview lists, the
# join figures and the video players - turned back into markdown, so a reader
# that fetches <page>.md gets every table, list and link the page carries.
#
# Dot-sourced by stage.ps1, after lib/markdown.ps1. The conversion covers the
# HTML this site writes, not HTML in general: headings, paragraphs, lists,
# definition lists, links, code, emphasis, images, and three special cases - an
# SVG figure becomes its title and description, a video becomes links to its
# file and captions, and controls (buttons) and anything aria-hidden are left
# out. Plain ASCII only (the repository's hygiene gates scan every tracked file).

$script:AgentTagPattern = [regex]::new('<!--.*?-->|<(?<close>/)?(?<tag>[A-Za-z][A-Za-z0-9-]*)(?<attrs>(?:\s+[^\s=/>]+(?:\s*=\s*(?:"[^"]*"|''[^'']*''|[^\s>]+))?)*)\s*(?<self>/)?>', 'Compiled, Singleline')
$script:AgentAttrPattern = [regex]::new('(?<name>[^\s=/>]+)(?:\s*=\s*(?:"(?<v>[^"]*)"|''(?<v>[^'']*)''|(?<v>[^\s>]+)))?', 'Compiled')
$script:AgentBlockTags = @('address', 'article', 'aside', 'blockquote', 'details', 'dd', 'div', 'dl', 'dt', 'figcaption', 'figure', 'footer', 'form', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'header', 'hr', 'li', 'main', 'nav', 'ol', 'p', 'section', 'summary', 'table', 'tr', 'ul', 'video', 'svg')
$script:AgentSkipTags = @('button', 'script', 'style', 'template', 'noscript', 'select', 'textarea', 'input')
$script:AgentVoidTags = @('area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'source', 'track', 'wbr', 'use', 'circle', 'line', 'path')

function Get-HtmlAttributes([string]$Text) {
    $attributes = @{}
    foreach ($m in $script:AgentAttrPattern.Matches($Text)) {
        $attributes[$m.Groups['name'].Value.ToLowerInvariant()] = [System.Net.WebUtility]::HtmlDecode($m.Groups['v'].Value)
    }
    return $attributes
}

# Writes the text gathered so far as one markdown line: a heading, a list item
# (or a continuation of one), or a paragraph.
function Write-AgentInline($State) {
    $text = ($State.Inline.ToString() -replace '\s+', ' ').Trim()
    [void]$State.Inline.Clear()
    if (-not $text) { return }
    $lists = $State.Lists
    if ($lists.Count -gt 0) {
        $depth = $lists.Count - 1
        $indent = ''
        for ($d = 0; $d -lt $depth; $d++) { $indent += if ($lists[$d].Type -eq 'ol') { '   ' } else { '  ' } }
        $list = $lists[$depth]
        if ($State.ItemPending) {
            $marker = if ($list.Type -eq 'ol') { "$($list.Index). " } else { '- ' }
            $State.Lines.Add($indent + $marker + $text)
            $State.ItemPending = $false
        }
        else {
            $hang = if ($list.Type -eq 'ol') { '   ' } else { '  ' }
            $State.Lines.Add($indent + $hang + $text)
        }
        return
    }
    if ($State.Heading -gt 0) { $text = ('#' * $State.Heading) + ' ' + $text }
    if ($State.Lines.Count -gt 0 -and $State.Lines[$State.Lines.Count - 1] -ne '') { $State.Lines.Add('') }
    $State.Lines.Add($text)
    $State.Lines.Add('')
}

# One block of the site's HTML as markdown.
function ConvertFrom-SiteHtml([string]$Html) {
    $state = @{
        Lines       = New-Object System.Collections.Generic.List[string]
        Inline      = New-Object System.Text.StringBuilder
        Lists       = New-Object System.Collections.Generic.List[object]
        ItemPending = $false
        Heading     = 0
    }
    $links = New-Object System.Collections.Generic.List[object]
    $skip = $null
    $skipDepth = 0
    $figure = $null
    $video = $null
    $position = 0

    $append = {
        param([string]$Text)
        if ($figure) {
            if ($figure.Capture) { [void]$figure[$figure.Capture].Append($Text) }
            return
        }
        if ($video) { return }
        [void]$state.Inline.Append($Text)
    }

    foreach ($m in $script:AgentTagPattern.Matches($Html)) {
        $between = $Html.Substring($position, $m.Index - $position)
        $position = $m.Index + $m.Length
        if (-not $skip -and $between) { & $append ([System.Net.WebUtility]::HtmlDecode($between)) }
        if ($m.Value.StartsWith('<!--')) { continue }

        $tag = $m.Groups['tag'].Value.ToLowerInvariant()
        $closing = $m.Groups['close'].Success
        $void = $m.Groups['self'].Success -or $script:AgentVoidTags -contains $tag

        if ($skip) {
            if ($tag -eq $skip -and -not $void) { if ($closing) { $skipDepth-- } else { $skipDepth++ } }
            if ($skipDepth -eq 0) { $skip = $null }
            continue
        }

        $attributes = if ($closing) { @{} } else { Get-HtmlAttributes $m.Groups['attrs'].Value }
        if (-not $closing -and -not $void -and ($script:AgentSkipTags -contains $tag -or $attributes['aria-hidden'] -eq 'true')) {
            $skip = $tag
            $skipDepth = 1
            continue
        }

        # A figure drawn in SVG is its title and description; nothing it draws.
        if ($tag -eq 'svg') {
            if (-not $closing) { $figure = @{ Capture = $null; title = New-Object System.Text.StringBuilder; desc = New-Object System.Text.StringBuilder }; continue }
            if ($figure) {
                $title = ($figure.title.ToString() -replace '\s+', ' ').Trim()
                $desc = ($figure.desc.ToString() -replace '\s+', ' ').Trim()
                $figure = $null
                Write-AgentInline $state
                if ($title) {
                    $sentence = "Figure: $title" + $(if ($title -match '[.!?]$') { '' } else { '.' })
                    if ($desc) { $sentence += " $desc" }
                    [void]$state.Inline.Append($sentence)
                    Write-AgentInline $state
                }
            }
            continue
        }
        if ($figure) {
            if ($tag -eq 'title' -or $tag -eq 'desc') { $figure.Capture = if ($closing) { $null } else { $tag } }
            continue
        }

        # A video is its file and its captions; the player itself is not text.
        if ($tag -eq 'video') {
            if (-not $closing) { $video = @{ Label = $attributes['aria-label']; Source = $null; Track = $null; TrackLabel = $null }; continue }
            if ($video) {
                $v = $video
                $video = $null
                Write-AgentInline $state
                $label = if ($v.Label) { $v.Label } else { 'Video' }
                $sentence = if ($v.Source) { "Video: [$label]($($v.Source))." } else { "Video: $label." }
                if ($v.Track) { $sentence += " Captions: [$(if ($v.TrackLabel) { $v.TrackLabel } else { 'captions' })]($($v.Track))." }
                [void]$state.Inline.Append($sentence)
                Write-AgentInline $state
            }
            continue
        }
        if ($video) {
            if ($tag -eq 'source' -and -not $video.Source) { $video.Source = $attributes['src'] }
            if ($tag -eq 'track' -and -not $video.Track) { $video.Track = $attributes['src']; $video.TrackLabel = $attributes['label'] }
            continue
        }

        switch -Regex ($tag) {
            '^h([1-6])$' {
                if ($state.Lists.Count -gt 0) {
                    # A heading inside a list item reads as the item's bold lead.
                    [void]$state.Inline.Append('**')
                    if ($closing) { Write-AgentInline $state }
                    break
                }
                Write-AgentInline $state
                $state.Heading = if ($closing) { 0 } else { [int]$Matches[1] }
                break
            }
            '^(ul|ol|dl)$' {
                Write-AgentInline $state
                if ($closing) {
                    if ($state.Lists.Count -gt 0) { $state.Lists.RemoveAt($state.Lists.Count - 1) }
                    if ($state.Lists.Count -eq 0 -and $state.Lines.Count -gt 0 -and $state.Lines[$state.Lines.Count - 1] -ne '') { $state.Lines.Add('') }
                }
                else {
                    if ($state.Lists.Count -eq 0 -and $state.Lines.Count -gt 0 -and $state.Lines[$state.Lines.Count - 1] -ne '') { $state.Lines.Add('') }
                    $state.Lists.Add(@{ Type = $(if ($tag -eq 'ol') { 'ol' } else { 'ul' }); Index = 0 })
                }
                break
            }
            '^li$' {
                Write-AgentInline $state
                if (-not $closing -and $state.Lists.Count -gt 0) {
                    $state.Lists[$state.Lists.Count - 1].Index++
                    $state.ItemPending = $true
                }
                break
            }
            '^dt$' {
                # A term and its description share one item: "- **Term:** text".
                if ($closing) { [void]$state.Inline.Append(':** '); break }
                Write-AgentInline $state
                if ($state.Lists.Count -gt 0) {
                    $state.Lists[$state.Lists.Count - 1].Index++
                    $state.ItemPending = $true
                }
                [void]$state.Inline.Append('**')
                break
            }
            '^dd$' { if ($closing) { Write-AgentInline $state }; break }
            '^a$' {
                if (-not $closing) {
                    $href = $attributes['href']
                    $links.Add(@{ Href = $href; Start = $state.Inline.Length })
                    if ($href -and -not $href.StartsWith('#')) { [void]$state.Inline.Append('[') }
                }
                elseif ($links.Count -gt 0) {
                    $link = $links[$links.Count - 1]
                    $links.RemoveAt($links.Count - 1)
                    if ($link.Href -and -not $link.Href.StartsWith('#')) {
                        $inner = $state.Inline.ToString($link.Start + 1, $state.Inline.Length - $link.Start - 1)
                        if ($inner.Trim()) { [void]$state.Inline.Append("]($($link.Href))") }
                        else { [void]$state.Inline.Remove($link.Start, $state.Inline.Length - $link.Start) }
                    }
                }
                break
            }
            '^code$' { [void]$state.Inline.Append('`'); break }
            '^(strong|b)$' { [void]$state.Inline.Append('**'); break }
            '^(em|i)$' { [void]$state.Inline.Append('*'); break }
            '^br$' { [void]$state.Inline.Append(' '); break }
            '^img$' {
                if ($attributes['alt']) { [void]$state.Inline.Append("![$($attributes['alt'])]($($attributes['src']))") }
                break
            }
            default {
                if ($script:AgentBlockTags -contains $tag) { Write-AgentInline $state }
            }
        }
    }
    if (-not $skip -and $position -lt $Html.Length) { & $append ([System.Net.WebUtility]::HtmlDecode($Html.Substring($position))) }
    Write-AgentInline $state
    return ($state.Lines -join "`n").Trim()
}

# Markdown for a line of prose: presentational spans unwrapped, and a NuGet
# badge written as the version it shows (see the badge note in stage.ps1).
function ConvertTo-AgentInline([string]$Line) {
    if ($Line.IndexOf('<') -lt 0 -and $Line.IndexOf('shields.io') -lt 0) { return $Line }
    $Line = [regex]::Replace($Line, '\[!\[NuGet (?<v>[^\]]+)\]\(https://img\.shields\.io/[^)]*\)\]\((?<link>[^)]+)\)', '[${v}](${link})')
    $Line = [regex]::Replace($Line, '\[!\[(?<alt>[^\]]*)\]\(https://img\.shields\.io/[^)]*\)\]\((?<link>[^)]+)\)', '[${alt}](${link})')
    $Line = [regex]::Replace($Line, '</?(?:span|time)\b[^>]*>', '')
    return $Line
}

# A staged page's body as its markdown alternate's body.
function ConvertTo-AgentMarkdown([string]$Body) {
    $lines = @($Body -split '\r?\n')
    $out = New-Object System.Collections.Generic.List[string]
    $fence = $null
    $i = 0
    while ($i -lt $lines.Count) {
        $line = $lines[$i]
        if ($fence) {
            $out.Add($line)
            if ($line -match $fence) { $fence = $null }
            $i++
            continue
        }
        $closer = Get-FenceCloser $line
        if ($closer) { $fence = $closer; $out.Add($line); $i++; continue }

        # A comment: unwrapped when it is written for the markdown alternate
        # (lattice:agent), dropped otherwise.
        if ($line -match '^ {0,3}<!--') {
            $comment = New-Object System.Collections.Generic.List[string]
            while ($i -lt $lines.Count) {
                $comment.Add($lines[$i])
                if ($lines[$i].Contains('-->')) { $i++; break }
                $i++
            }
            $text = ($comment -join "`n")
            $agent = [regex]::Match($text, '(?s)^\s*<!--\s*lattice:agent\b(?<body>.*?)-->\s*$')
            if ($agent.Success) { $out.Add($agent.Groups['body'].Value.Trim()) }
            elseif ($text -notmatch '(?s)-->\s*\S') { continue }
            else { $out.Add([regex]::Replace($text, '(?s)<!--.*?-->', '').Trim()) }
            continue
        }

        # A block of the site's HTML, up to the blank line that ends it.
        if ($line -match '^ {0,3}</?(?<tag>[A-Za-z][A-Za-z0-9-]*)(?:\s|/?>|$)' -and $script:AgentBlockTags -contains $Matches.tag.ToLowerInvariant()) {
            $block = New-Object System.Collections.Generic.List[string]
            while ($i -lt $lines.Count -and $lines[$i].Trim() -ne '') { $block.Add($lines[$i]); $i++ }
            $markdown = ConvertFrom-SiteHtml ($block -join "`n")
            if ($markdown) {
                if ($out.Count -gt 0 -and $out[$out.Count - 1] -ne '') { $out.Add('') }
                foreach ($converted in ($markdown -split "`n")) { $out.Add($converted) }
                $out.Add('')
            }
            continue
        }

        $out.Add((ConvertTo-AgentInline $line))
        $i++
    }
    $text = ($out -join "`n")
    $text = [regex]::Replace($text, '\n{3,}', "`n`n")
    return $text.Trim() + "`n"
}
