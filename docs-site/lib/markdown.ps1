# Markdown helpers shared by stage.ps1: headings outside fenced code, the heading
# ids DocFX gives them, site-relative paths, links, and front matter.
#
# Dot-sourced by stage.ps1. Documents are scanned as whole strings with compiled
# expressions rather than line by line, because a loop over a quarter of a
# megabyte of markdown is slow in PowerShell. Plain ASCII only (the repository's
# hygiene gates scan every tracked file).

$script:MdOptions = [System.Text.RegularExpressions.RegexOptions]::Compiled -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
$script:MdFenceLine = [regex]::new('^ {0,3}(?<run>`{3,}|~{3,})(?<rest>[^\n]*)$', $script:MdOptions)
$script:MdHeadingLine = [regex]::new('^ {0,3}(?<h>#{1,6})(?:[ \t]+(?<t>[^\n]*?))?(?:[ \t]+#+)?[ \t]*\r?$', $script:MdOptions)
$script:MdCodeSpan = [regex]::new('(?<!`)(?<run>`+)(?!`)(?<code>[^\n]+?)(?<!`)\k<run>(?!`)', $script:MdOptions)
$script:MdLink = [regex]::new(
    '(?<open>\]\()(?<target>[^)\s#]*)(?<anchor>#[^)\s]*)?(?<rest>(?:\s+"[^"]*")?\))' +
    '|(?<open>\b(?:href|src)=")(?<target>[^"#]*)(?<anchor>#[^"]*)?(?<rest>")' +
    '|(?<open>^ {0,3}\[[^\]\n]+\]:[ \t]*)(?<target>[^\s#]*)(?<anchor>#[^\s]*)?(?<rest>)',
    $script:MdOptions)
$script:MdPlainImage = [regex]::new('!\[([^\]]*)\]\([^)]*\)', $script:MdOptions)
$script:MdPlainLink = [regex]::new('\[([^\]]+)\]\([^)]*\)', $script:MdOptions)
$script:MdPlainReference = [regex]::new('\[([^\]]+)\]\[[^\]]*\]', $script:MdOptions)
$script:MdPlainHtml = [regex]::new('</?[A-Za-z][^>]*>', $script:MdOptions)
$script:MdPlainStrong = [regex]::new('(?<![A-Za-z0-9_])(\*\*|__)(?=\S)(.+?)(?<=\S)\1(?![A-Za-z0-9_])', $script:MdOptions)
$script:MdPlainEmphasis = [regex]::new('(?<![A-Za-z0-9_*])([*_])(?=\S)(.+?)(?<=\S)\1(?![A-Za-z0-9_*])', $script:MdOptions)
$script:MdPlainEscape = [regex]::new('\\([!-/:-@\[-`{-~])', $script:MdOptions)
$script:MdPlainEmoji = [regex]::new(':[a-z0-9_+-]+:', $script:MdOptions)
$script:MdIdDrop = [regex]::new('[^\p{L}\p{Nd} _-]', [System.Text.RegularExpressions.RegexOptions]::Compiled)

# The expression that closes a fenced code block opened by $Line, or $null when
# $Line does not open one, for the few callers that walk a document line by line.
function Get-FenceCloser([string]$Line) {
    if ($Line -notmatch '^ {0,3}(?<f>`{3,}|~{3,})') { return $null }
    $run = $Matches.f
    return '^ {0,3}' + [regex]::Escape([string]$run[0]) + '{' + $run.Length + ',}[ \t]*$'
}

# The character ranges [Start, End) of a document's fenced code blocks. A fence
# closes on a run of its own character at least as long as the one that opened
# it, with nothing else on the line; an unclosed fence runs to the end.
function Get-FenceRanges([string]$Text) {
    $ranges = New-Object System.Collections.Generic.List[int[]]
    $open = $null
    foreach ($m in $script:MdFenceLine.Matches($Text)) {
        $run = $m.Groups['run'].Value
        if ($null -eq $open) {
            $open = @{ Start = $m.Index; Char = $run[0]; Length = $run.Length }
            continue
        }
        if ($run[0] -eq $open.Char -and $run.Length -ge $open.Length -and $m.Groups['rest'].Value.Trim() -eq '') {
            $ranges.Add([int[]]@($open.Start, ($m.Index + $m.Length)))
            $open = $null
        }
    }
    if ($null -ne $open) { $ranges.Add([int[]]@($open.Start, $Text.Length)) }
    return , $ranges
}

# The ranges fenced code and inline code spans cover, sorted by start.
function Get-CodeRanges([string]$Text) {
    $ranges = Get-FenceRanges $Text
    foreach ($m in $script:MdCodeSpan.Matches($Text)) { $ranges.Add([int[]]@($m.Index, ($m.Index + $m.Length))) }
    $sorted = New-Object System.Collections.Generic.List[int[]]
    foreach ($range in ($ranges | Sort-Object { $_[0] })) { $sorted.Add($range) }
    return , $sorted
}

# Whether $Index falls inside one of $Ranges, which are sorted by start. $Cursor
# is a one-element array holding the scan position, so a caller walking indexes
# in increasing order pays for each range once.
function Test-InRanges([int]$Index, $Ranges, [int[]]$Cursor) {
    while ($Cursor[0] -lt $Ranges.Count -and $Ranges[$Cursor[0]][1] -le $Index) { $Cursor[0]++ }
    for ($k = $Cursor[0]; $k -lt $Ranges.Count -and $Ranges[$k][0] -le $Index; $k++) {
        if ($Index -lt $Ranges[$k][1]) { return $true }
    }
    return $false
}

# The ATX headings of a document, skipping fenced code blocks. Each carries the
# zero-based line it sits on, its level, and its raw inline text.
function Get-MarkdownHeadings([string[]]$Lines) {
    $text = $Lines -join "`n"
    $fences = Get-FenceRanges $text
    $starts = New-Object System.Collections.Generic.List[int]
    $starts.Add(0)
    $at = $text.IndexOf("`n")
    while ($at -ge 0) { $starts.Add($at + 1); $at = $text.IndexOf("`n", $at + 1) }
    $headings = New-Object System.Collections.Generic.List[object]
    $cursor = [int[]]@(0)
    foreach ($m in $script:MdHeadingLine.Matches($text)) {
        if (Test-InRanges $m.Index $fences $cursor) { continue }
        $line = $starts.BinarySearch($m.Index)
        if ($line -lt 0) { $line = (-bnot $line) - 1 }
        $headings.Add([pscustomobject]@{
            Line  = $line
            Level = $m.Groups['h'].Value.Length
            Text  = $m.Groups['t'].Value.TrimEnd("`r")
            Id    = $null
        })
    }
    return , $headings
}

# The plain text markdig renders for a heading's inline content: code spans keep
# their content, links and images keep their text, emphasis markers, inline
# HTML and backslash escapes are dropped, and entities are decoded.
function Get-HeadingPlainText([string]$Text) {
    if ($Text -notmatch '[`\[<*_\\&:!]') { return $Text.Trim() }
    $codes = New-Object System.Collections.Generic.List[string]
    $builder = New-Object System.Text.StringBuilder
    $last = 0
    foreach ($m in $script:MdCodeSpan.Matches($Text)) {
        [void]$builder.Append($Text, $last, $m.Index - $last)
        $content = $m.Groups['code'].Value
        if ($content.Length -ge 2 -and $content.StartsWith(' ') -and $content.EndsWith(' ') -and $content.Trim()) {
            $content = $content.Substring(1, $content.Length - 2)
        }
        [void]$builder.Append([char]0).Append($codes.Count).Append([char]0)
        $codes.Add($content)
        $last = $m.Index + $m.Length
    }
    [void]$builder.Append($Text, $last, $Text.Length - $last)
    $t = $builder.ToString()
    $t = $script:MdPlainImage.Replace($t, '$1')
    $t = $script:MdPlainLink.Replace($t, '$1')
    $t = $script:MdPlainReference.Replace($t, '$1')
    $t = $script:MdPlainHtml.Replace($t, '')
    $t = $script:MdPlainStrong.Replace($t, '$2')
    $t = $script:MdPlainEmphasis.Replace($t, '$2')
    $t = $script:MdPlainEscape.Replace($t, '$1')
    # DocFX renders an emoji shortcode (":warning:") as the emoji itself, which
    # is not a letter, so it contributes nothing to the id.
    $t = $script:MdPlainEmoji.Replace($t, [string][char]0x2022)
    for ($k = 0; $k -lt $codes.Count; $k++) { $t = $t.Replace([string][char]0 + $k + [char]0, $codes[$k]) }
    return [System.Net.WebUtility]::HtmlDecode($t).Trim()
}

# markdig's GitHub-style identifier, which DocFX uses: letters and digits
# lower-cased, spaces to hyphens, hyphens and underscores kept, the rest dropped.
function ConvertTo-HeadingId([string]$PlainText) {
    $id = $script:MdIdDrop.Replace($PlainText, '').ToLowerInvariant().Replace(' ', '-')
    if (-not $id) { $id = 'section' }
    return $id
}

# Gives each heading the id DocFX will render, deduplicated in document order
# the way markdig does it ("example", "example-1", "example-2").
function Set-HeadingIds($Headings) {
    $used = New-Object 'System.Collections.Generic.HashSet[string]'
    foreach ($heading in $Headings) {
        $base = ConvertTo-HeadingId (Get-HeadingPlainText $heading.Text)
        $id = $base
        $n = 0
        while (-not $used.Add($id)) { $n++; $id = "$base-$n" }
        $heading.Id = $id
    }
}

# A readable file name for a heading: lower-case words joined by single hyphens.
function ConvertTo-FileSlug([string]$PlainText) {
    $slug = ($PlainText.ToLowerInvariant() -replace '[^a-z0-9]+', '-').Trim('-')
    if (-not $slug) { $slug = 'section' }
    if ($slug.Length -gt 60) { $slug = $slug.Substring(0, 60).TrimEnd('-') }
    return $slug
}

# --- Site-relative paths: always forward slashes, relative to the staging root ---

function Join-SitePath([string]$Directory, [string]$Relative) {
    $parts = New-Object System.Collections.Generic.List[string]
    $combined = if ($Directory) { "$Directory/$Relative" } else { $Relative }
    foreach ($part in $combined.Split('/')) {
        if ($part -eq '' -or $part -eq '.') { continue }
        if ($part -eq '..') {
            if ($parts.Count -eq 0) { return $null }
            $parts.RemoveAt($parts.Count - 1)
            continue
        }
        $parts.Add($part)
    }
    return ($parts -join '/')
}

function Get-SiteDirectory([string]$SitePath) {
    $index = $SitePath.LastIndexOf('/')
    if ($index -lt 0) { return '' }
    return $SitePath.Substring(0, $index)
}

# The relative link from a page in $FromDirectory to $ToPath, both site-relative.
function Get-SiteRelativeLink([string]$FromDirectory, [string]$ToPath) {
    $from = if ($FromDirectory) { @($FromDirectory.Split('/')) } else { @() }
    $to = @($ToPath.Split('/'))
    $common = 0
    while ($common -lt $from.Count -and $common -lt ($to.Count - 1) -and $from[$common] -eq $to[$common]) { $common++ }
    $parts = New-Object System.Collections.Generic.List[string]
    for ($i = $common; $i -lt $from.Count; $i++) { $parts.Add('..') }
    for ($i = $common; $i -lt $to.Count; $i++) { $parts.Add($to[$i]) }
    return ($parts -join '/')
}

function Test-RelativeLinkTarget([string]$Target) {
    if (-not $Target) { return $false }
    if ($Target.StartsWith('#') -or $Target.StartsWith('/')) { return $false }
    return -not ($Target -match '^([a-z][a-z0-9+.-]*:|//)')
}

# Applies $Rewrite to every link target in a markdown document - inline links and
# images, reference definitions, and href/src attributes in raw HTML - outside
# fenced code and inline code spans. $Rewrite takes (target, anchor), where the
# anchor keeps its '#', and returns the replacement (target plus any anchor), or
# $null to leave the link as it is.
function Update-MarkdownLinks([string]$Text, [scriptblock]$Rewrite) {
    # Locals carry a prefix: $Rewrite runs in a child of this scope, so a local
    # sharing a name with one of its caller's variables would shadow it.
    $umlCode = Get-CodeRanges $Text
    $umlCursor = [int[]]@(0)
    $umlEvaluator = [System.Text.RegularExpressions.MatchEvaluator]{
        param($umlMatch)
        if (Test-InRanges $umlMatch.Index $umlCode $umlCursor) { return $umlMatch.Value }
        $umlResult = & $Rewrite $umlMatch.Groups['target'].Value $umlMatch.Groups['anchor'].Value
        if ($null -eq $umlResult) { return $umlMatch.Value }
        return $umlMatch.Groups['open'].Value + $umlResult + $umlMatch.Groups['rest'].Value
    }
    return $script:MdLink.Replace($Text, $umlEvaluator)
}

# --- Front matter ---------------------------------------------------------------

# Splits a document into its YAML front matter lines (without the fences) and
# its body. A document with none returns no lines and the whole text as body.
function Split-FrontMatter([string]$Text) {
    $match = [regex]::Match($Text, '\A---[ \t]*\r?\n(?<yaml>.*?)\r?\n---[ \t]*(?:\r?\n|\z)', 'Singleline')
    if (-not $match.Success) { return [pscustomobject]@{ Lines = @(); Body = $Text } }
    $lines = @($match.Groups['yaml'].Value -split '\r?\n')
    return [pscustomobject]@{ Lines = $lines; Body = $Text.Substring($match.Length) }
}

function Get-FrontMatterValue([string[]]$Lines, [string]$Key) {
    foreach ($line in $Lines) {
        if ($line -match "^$([regex]::Escape($Key)):[ \t]*(?<v>.*)$") {
            $value = $Matches.v.Trim()
            if ($value.Length -ge 2 -and $value.StartsWith('"') -and $value.EndsWith('"')) {
                $value = $value.Substring(1, $value.Length - 2) -replace '\\"', '"' -replace '\\\\', '\'
            }
            return $value
        }
    }
    return $null
}

# A double-quoted YAML scalar.
function ConvertTo-QuotedYaml([string]$Value) {
    return '"' + ($Value -replace '\\', '\\' -replace '"', '\"') + '"'
}

# Sets top-level keys in a document's front matter, adding the block if it has
# none. Values are written as double-quoted YAML scalars.
function Set-FrontMatterValues([string]$Text, [System.Collections.IDictionary]$Values) {
    $newline = if ($Text.Contains("`r`n")) { "`r`n" } else { "`n" }
    $parts = Split-FrontMatter $Text
    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($line in $parts.Lines) {
        $key = if ($line -match '^([A-Za-z_][A-Za-z0-9_-]*):') { $Matches[1] } else { $null }
        if ($key -and $Values.Contains($key)) { continue }
        $lines.Add($line)
    }
    foreach ($key in $Values.Keys) { $lines.Add("${key}: " + (ConvertTo-QuotedYaml ([string]$Values[$key]))) }
    $body = $parts.Body
    if ($parts.Lines.Count -eq 0) { $body = $newline + $body }
    return "---$newline" + ($lines -join $newline) + "$newline---$newline" + $body
}
