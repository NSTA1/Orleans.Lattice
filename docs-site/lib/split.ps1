# Splits a staged page that is too large to read comfortably - the API reference,
# the configuration reference, the metrics catalogue, the release history - into
# an index page and one page per section, so a reader or an agent fetches the
# part it needs instead of a quarter of a megabyte.
#
# Dot-sourced by stage.ps1, after lib/markdown.ps1. Only the STAGED copy is
# split; the tracked document is untouched and still renders on github.com as one
# page. Every heading keeps the id DocFX gives it, so the split is invisible to a
# link: Update-SplitPageLinks points each link at the page its heading moved to,
# and the zero-warning link gate proves every one still resolves.
#
# Plain ASCII only (the repository's hygiene gates scan every tracked file).

# Characters in a line range, newlines included.
function Measure-LineRange([string[]]$Lines, [int]$Start, [int]$End) {
    $size = 0
    for ($i = $Start; $i -lt $End; $i++) { $size += $Lines[$i].Length + 1 }
    return $size
}

# A description short enough for a list: a sentence that enumerates (" - a, b,
# c, d") is cut before its list, anything else at a word boundary, never on a
# dangling "and" or "of". A sentence that has to be cut loses its links first,
# keeping their text, so a cut never lands inside one.
function Get-ShortDescription([string]$Sentence, [int]$Max = 240) {
    if (-not $Sentence -or $Sentence.Length -le $Max) { return $Sentence }
    $Sentence = ConvertTo-LinkText $Sentence
    if ($Sentence.Length -le $Max) { return $Sentence }
    $dash = $Sentence.IndexOf(' - ')
    if ($dash -gt 20 -and $dash -lt $Max -and ([regex]::Matches($Sentence.Substring($dash), ', ')).Count -ge 3) {
        return $Sentence.Substring(0, $dash).TrimEnd(' ', ',', ';', ':') + '.'
    }
    $cut = $Sentence.LastIndexOf(' ', $Max)
    if ($cut -lt 20) { $cut = $Max }
    $short = $Sentence.Substring(0, $cut).TrimEnd(' ', ',', ';', ':', '.', '-')
    $short = [regex]::Replace($short, '\s+(and|or|of|to|the|a|an|in|on|for|with|by|as|at|from)$', '', 'IgnoreCase').TrimEnd(' ', ',', ';', ':', '-')
    return $short + '...'
}

# The first sentence of the first prose paragraph in a line range, skipping
# headings, tables, lists, quotes, fences, and HTML.
function Get-RangeDescription([string[]]$Lines, [int]$Start, [int]$End, [int]$Max = 240) {
    $paragraph = New-Object System.Collections.Generic.List[string]
    $fence = $null
    for ($i = $Start; $i -lt $End; $i++) {
        $line = $Lines[$i]
        if ($fence) { if ($line -match $fence) { $fence = $null }; continue }
        $closer = Get-FenceCloser $line
        if ($closer) { if ($paragraph.Count) { break }; $fence = $closer; continue }
        if ($line.Trim() -eq '') { if ($paragraph.Count) { break }; continue }
        if ($line -match '^\s*(#|>|\||[-*+]\s|\d+\.\s|!\[|\[!\[|<)') { if ($paragraph.Count) { break }; continue }
        $paragraph.Add($line.Trim())
    }
    if ($paragraph.Count -eq 0) { return $null }
    # A paragraph that introduces a code block ends in a colon, not a full stop.
    $sentence = (Get-FirstSentence ($paragraph -join ' ')) -replace ':\.$', '.'
    return Get-ShortDescription $sentence $Max
}

# Heading markdown that can sit inside a link's text: its own links unwrapped.
function ConvertTo-LinkText([string]$Markdown) {
    return [regex]::Replace($Markdown, '!?\[([^\]]*)\]\([^)]*\)', '$1')
}

<#
Splits $Page (site-relative, under $Staging) at its level-2 headings. Returns
$null when the page has no level-2 heading, and otherwise:

  Page      the page itself, which becomes an index: its introduction, a list of
            its sections, and any section named in -Keep
  Title     the page's title, as plain text
  Pages     every page split out of it, in reading order, each with a Path,
            Title, TocName, Heading, Description, Parent (the page it belongs
            under), Start and End (its lines in the staged body), Children,
            and, for a run of level-3 subsections, the Headings it holds
  Children  the pages listed on the index, each with its own Children
  Anchors   every old heading id -> @{ Path; Id; IsTop }, for rewriting links

A section larger than -SectionBudget is split again at its level-3 headings into
consecutive runs that each fit the budget, under a page for the section itself.
A section smaller than -InlineBelow stays on the index, listed in its contents
with the rest, so a page's short overview sections are not scattered across
pages of a few lines each.

A section's description in the contents is its first sentence; -Describe, given
(heading, lines, first line, end line), writes it instead.

Links written into the new pages are relative to the ORIGINAL page's directory,
like the content moved with them, and are rebased together at the end.
#>
function Split-LargePage {
    param(
        [Parameter(Mandatory = $true)] [string]$Staging,
        [Parameter(Mandatory = $true)] [string]$Page,
        [int]$SectionBudget = 60000,
        [string]$Directory,
        [scriptblock]$Title,
        [scriptblock]$Describe,
        [int]$InlineBelow = 0,
        [string[]]$Keep = @(),
        [string[]]$Drop = @(),
        [string]$ContentsHeading = 'Contents',
        [string]$ContentsLede,
        [string]$PartOf,
        [string]$PreviousLabel = 'Previous',
        [string]$NextLabel = 'Next'
    )

    $file = Join-Path $Staging $Page
    $text = [System.IO.File]::ReadAllText($file)
    $newline = if ($text.Contains("`r`n")) { "`r`n" } else { "`n" }
    $front = Split-FrontMatter $text
    $lines = [string[]]($front.Body -split '\r?\n')
    $headings = Get-MarkdownHeadings $lines
    Set-HeadingIds $headings
    $h2 = @($headings | Where-Object { $_.Level -eq 2 })
    if ($h2.Count -eq 0) { return $null }

    $pageDirectory = Get-SiteDirectory $Page
    if (-not $Directory) {
        $Directory = Join-SitePath $pageDirectory ([System.IO.Path]::GetFileNameWithoutExtension($Page).ToLowerInvariant())
    }
    $h1 = $headings | Where-Object { $_.Level -eq 1 -and $_.Line -lt $h2[0].Line } | Select-Object -First 1
    $pageTitle = if ($h1) { Get-HeadingPlainText $h1.Text } else { Get-FrontMatterValue $front.Lines 'title' }
    if (-not $pageTitle) { $pageTitle = [System.IO.Path]::GetFileNameWithoutExtension($Page) }
    $pageName = [System.IO.Path]::GetFileName($Page)
    if (-not $PartOf) { $PartOf = "Part of [$pageTitle]({0})." }
    # A link from the original page's directory, rebased with the moved content.
    $from = { param([string]$To) Get-SiteRelativeLink $pageDirectory $To }

    # --- Plan the pages ---
    $slugs = New-Object 'System.Collections.Generic.HashSet[string]'
    $claim = {
        param([string]$Base)
        $slug = $Base
        $n = 1
        while (-not $slugs.Add($slug)) { $n++; $slug = "$Base-$n" }
        return $slug
    }
    $planned = New-Object System.Collections.Generic.List[object]
    $kept = New-Object System.Collections.Generic.List[object]
    $contents = New-Object System.Collections.Generic.List[object]
    for ($k = 0; $k -lt $h2.Count; $k++) {
        $heading = $h2[$k]
        $start = $heading.Line
        $end = if ($k + 1 -lt $h2.Count) { $h2[$k + 1].Line } else { $lines.Count }
        $plain = Get-HeadingPlainText $heading.Text
        if ($Drop -contains $plain) { continue }
        if ($Keep -contains $plain) { $kept.Add(@{ Start = $start; End = $end }); continue }
        $size = Measure-LineRange $lines $start $end
        $description = if ($Describe) { & $Describe $plain $lines ($start + 1) $end } else { Get-RangeDescription $lines ($start + 1) $end }
        if ($size -lt $InlineBelow) {
            $kept.Add(@{ Start = $start; End = $end })
            $contents.Add([pscustomobject]@{ Heading = $heading.Text; Link = "#$($heading.Id)"; Description = $description; Children = @() })
            continue
        }

        $name = if ($Title) { [string](& $Title $plain) } else { $plain }
        $slug = & $claim (ConvertTo-FileSlug $plain)
        $h3 = @($headings | Where-Object { $_.Level -eq 3 -and $_.Line -gt $start -and $_.Line -lt $end })
        $section = [pscustomobject]@{
            Path        = "$Directory/$slug.md"
            Title       = $name
            TocName     = $name
            Heading     = if ($Title) { $name } else { $heading.Text }
            Description = $description
            Parent      = $Page
            Start       = $start
            End         = $end
            Children    = New-Object System.Collections.Generic.List[object]
            Chunk       = $false
            Headings    = @()
        }
        $planned.Add($section)
        $contents.Add([pscustomobject]@{ Heading = $section.Heading; Link = (& $from $section.Path); Description = $section.Description; Children = $section.Children })
        if ($size -le $SectionBudget -or $h3.Count -eq 0) { continue }

        # Too large for one page: the section keeps its introduction, and its
        # level-3 subsections are packed into consecutive runs that each fit.
        $section.End = $h3[0].Line
        $runs = New-Object System.Collections.Generic.List[object]
        $run = New-Object System.Collections.Generic.List[object]
        $runSize = 0
        for ($j = 0; $j -lt $h3.Count; $j++) {
            $blockEnd = if ($j + 1 -lt $h3.Count) { $h3[$j + 1].Line } else { $end }
            $blockSize = Measure-LineRange $lines $h3[$j].Line $blockEnd
            if ($run.Count -gt 0 -and ($runSize + $blockSize) -gt $SectionBudget) {
                $runs.Add($run)
                $run = New-Object System.Collections.Generic.List[object]
                $runSize = 0
            }
            $run.Add([pscustomobject]@{ Heading = $h3[$j]; Start = $h3[$j].Line; End = $blockEnd })
            $runSize += $blockSize
        }
        if ($run.Count -gt 0) { $runs.Add($run) }

        $part = 0
        foreach ($run in $runs) {
            $part++
            $first = ConvertTo-LinkText $run[0].Heading.Text
            $last = ConvertTo-LinkText $run[$run.Count - 1].Heading.Text
            $range = if ($run.Count -eq 1) { $first } else { "$first to $last" }
            $rangePlain = if ($run.Count -eq 1) { Get-HeadingPlainText $first } else { "$(Get-HeadingPlainText $first) to $(Get-HeadingPlainText $last)" }
            $chunk = [pscustomobject]@{
                Path        = "$Directory/$slug-$part.md"
                Title       = "${name}: $rangePlain"
                TocName     = $rangePlain
                Heading     = "$(if ($Title) { $name } else { ConvertTo-LinkText $heading.Text }): $range"
                Description = $null
                Parent      = $section.Path
                Start       = $run[0].Start
                End         = $run[$run.Count - 1].End
                Children    = New-Object System.Collections.Generic.List[object]
                Chunk       = $true
                # What the run holds, in its own order: a title can only name
                # the run's first and last subsections.
                Headings    = @($run | ForEach-Object { ConvertTo-LinkText $_.Heading.Text })
            }
            $section.Children.Add($chunk)
            $planned.Add($chunk)
        }
    }
    if ($planned.Count -eq 0) { return $null }
    $top = @($planned | Where-Object { -not $_.Chunk })

    # --- Compose every page, remembering where each original heading went ---
    # A page's lines are paired with the original line each came from (-1 when
    # generated), so the ids DocFX will render on the new page can be mapped
    # back to the ids the whole page had.
    $origin = @{}
    foreach ($heading in $headings) { $origin[$heading.Line] = $heading }
    $anchors = @{}
    $record = {
        param([string]$Path, [string[]]$PageLines, [int[]]$Sources)
        $newHeadings = Get-MarkdownHeadings $PageLines
        Set-HeadingIds $newHeadings
        $first = $true
        foreach ($newHeading in $newHeadings) {
            $isTop = $first -and $newHeading.Level -eq 1 -and $Path -ne $Page
            $first = $false
            $source = $Sources[$newHeading.Line]
            if ($source -lt 0 -or -not $origin.ContainsKey($source)) { continue }
            $anchors[$origin[$source].Id] = @{ Path = $Path; Id = $newHeading.Id; IsTop = $isTop }
        }
    }
    $trim = {
        param($Lines, $Sources)
        while ($Lines.Count -gt 0 -and $Lines[$Lines.Count - 1].Trim() -eq '') { $Lines.RemoveAt($Lines.Count - 1); $Sources.RemoveAt($Sources.Count - 1) }
    }

    $raised = Step-HeadingLevels $lines $headings 0 $lines.Count
    $composed = @{}
    for ($p = 0; $p -lt $planned.Count; $p++) {
        $entry = $planned[$p]
        $out = New-Object System.Collections.Generic.List[string]
        $src = New-Object System.Collections.Generic.List[int]
        $add = { param([string]$Line, [int]$Source = -1) $out.Add($Line); $src.Add($Source) }

        & $add ('# ' + $entry.Heading) $(if ($entry.Chunk) { -1 } else { $entry.Start })
        & $add ''
        if ($entry.Chunk) {
            $parent = $planned | Where-Object { $_.Path -eq $entry.Parent } | Select-Object -First 1
            & $add "Part of [$($parent.Title)]($(& $from $parent.Path)), in [$pageTitle]($(& $from $Page))."
        }
        else {
            & $add ($PartOf -f (& $from $Page))
        }
        & $add ''
        $bodyStart = if ($entry.Chunk) { $entry.Start } else { $entry.Start + 1 }
        while ($bodyStart -lt $entry.End -and $lines[$bodyStart].Trim() -eq '') { $bodyStart++ }
        for ($i = $bodyStart; $i -lt $entry.End; $i++) { & $add $raised[$i] $i }
        & $trim $out $src

        if ($entry.Children.Count -gt 0) {
            & $add ''
            & $add "## $ContentsHeading"
            & $add ''
            foreach ($child in $entry.Children) { & $add "- [$($child.TocName)]($(& $from $child.Path))" }
        }

        # Where to read next, for the markdown alternate. The rendered page has
        # DocFX's own previous and next links, so this sits in a comment that
        # only the markdown alternate unwraps (see lib/agent.ps1).
        $nav = New-Object System.Collections.Generic.List[string]
        if ($p -gt 0) { $nav.Add("${PreviousLabel}: [$($planned[$p - 1].Title)]($(& $from $planned[$p - 1].Path)).") }
        if ($p + 1 -lt $planned.Count) { $nav.Add("${NextLabel}: [$($planned[$p + 1].Title)]($(& $from $planned[$p + 1].Path)).") }
        $nav.Add("Contents: [$pageTitle]($(& $from $Page)).")
        & $add ''
        & $add ('<!-- lattice:agent ' + ($nav -join ' ') + ' -->')

        & $record $entry.Path $out.ToArray() $src.ToArray()
        $composed[$entry.Path] = $out.ToArray()
    }

    # The index: the introduction, the contents, and any kept sections.
    $out = New-Object System.Collections.Generic.List[string]
    $src = New-Object System.Collections.Generic.List[int]
    for ($i = 0; $i -lt $h2[0].Line; $i++) { $out.Add($lines[$i]); $src.Add($i) }
    & $trim $out $src
    foreach ($line in @('', "## $ContentsHeading", '')) { $out.Add($line); $src.Add(-1) }
    if ($ContentsLede) { $out.Add($ContentsLede); $src.Add(-1); $out.Add(''); $src.Add(-1) }
    foreach ($entry in $contents) {
        $item = "- [$(ConvertTo-LinkText $entry.Heading)]($($entry.Link))"
        if ($entry.Description) { $item += ": $($entry.Description)" }
        $out.Add($item); $src.Add(-1)
        foreach ($child in $entry.Children) { $out.Add("  - [$($child.TocName)]($(& $from $child.Path))"); $src.Add(-1) }
    }
    foreach ($section in ($kept | Sort-Object { $_.Start })) {
        $out.Add(''); $src.Add(-1)
        for ($i = $section.Start; $i -lt $section.End; $i++) { $out.Add($lines[$i]); $src.Add($i) }
        & $trim $out $src
    }
    & $record $Page $out.ToArray() $src.ToArray()
    $composed[$Page] = $out.ToArray()

    # A heading in a dropped section has nowhere to go but the top of the index.
    foreach ($heading in $headings) {
        if (-not $anchors.ContainsKey($heading.Id)) { $anchors[$heading.Id] = @{ Path = $Page; Id = $null; IsTop = $true } }
    }

    # --- Write the pages, pointing their links at the right places ---
    # A relative link is rebased from the original page's directory to the new
    # page's; a link to a heading of this page goes to wherever it now lives.
    $utf8 = New-Object System.Text.UTF8Encoding $false
    foreach ($path in $composed.Keys) {
        $here = Get-SiteDirectory $path
        $body = Update-MarkdownLinks ($composed[$path] -join $newline) {
            param($target, $anchor)
            if (-not $target) {
                if ($anchor.Length -lt 2) { return $null }
                $moved = $anchors[$anchor.Substring(1)]
                if (-not $moved) { return $null }
                if ($moved.Path -eq $path) { if ($moved.Id) { return '#' + $moved.Id } else { return $null } }
                $link = Get-SiteRelativeLink $here $moved.Path
                if ($moved.IsTop -or -not $moved.Id) { return $link }
                return "$link#$($moved.Id)"
            }
            if ($here -eq $pageDirectory -or -not (Test-RelativeLinkTarget $target)) { return $null }
            $resolved = Join-SitePath $pageDirectory $target
            if ($null -eq $resolved) { return $null }
            return (Get-SiteRelativeLink $here $resolved) + $anchor
        }
        $frontLines = if ($path -eq $Page) { @($front.Lines) } else {
            $entry = $planned | Where-Object { $_.Path -eq $path } | Select-Object -First 1
            @('title: ' + (ConvertTo-QuotedYaml "$($entry.Title) - $pageTitle"))
        }
        $content = if ($frontLines.Count -gt 0) { "---$newline" + ($frontLines -join $newline) + "$newline---$newline$newline" + $body + $newline } else { $body + $newline }
        $target = Join-Path $Staging $path
        New-Item -ItemType Directory -Path (Split-Path $target) -Force | Out-Null
        [System.IO.File]::WriteAllText($target, $content, $utf8)
    }

    return [pscustomobject]@{
        Page     = $Page
        Title    = $pageTitle
        Pages    = $planned
        Children = $top
        Anchors  = $anchors
    }
}

# Raises every heading in a line range by one level (## to #, ### to ##).
function Step-HeadingLevels([string[]]$Lines, $Headings, [int]$Start, [int]$End) {
    $raised = [string[]]$Lines.Clone()
    foreach ($heading in $Headings) {
        if ($heading.Line -lt $Start -or $heading.Line -ge $End -or $heading.Level -lt 2) { continue }
        $raised[$heading.Line] = [regex]::Replace($Lines[$heading.Line], '^( {0,3})#', '$1')
    }
    return , $raised
}

# Points links at a split page's headings to the pages those headings moved to.
# $Splits maps each split page's path to what Split-LargePage returned.
function Update-SplitPageLinks([string]$Staging, [string]$Page, [hashtable]$Splits) {
    if ($Splits.Count -eq 0) { return }
    $file = Join-Path $Staging $Page
    $text = [System.IO.File]::ReadAllText($file)
    $here = Get-SiteDirectory $Page
    $updated = Update-MarkdownLinks $text {
        param($target, $anchor)
        if ($anchor.Length -lt 2 -or -not (Test-RelativeLinkTarget $target)) { return $null }
        $resolved = Join-SitePath $here $target
        if (-not $resolved -or -not $Splits.ContainsKey($resolved)) { return $null }
        $moved = $Splits[$resolved].Anchors[$anchor.Substring(1)]
        if (-not $moved) { return $null }
        $link = Get-SiteRelativeLink $here $moved.Path
        if ($moved.IsTop -or -not $moved.Id) { return $link }
        return "$link#$($moved.Id)"
    }
    if ($updated -ne $text) {
        [System.IO.File]::WriteAllText($file, $updated, (New-Object System.Text.UTF8Encoding $false))
    }
}
