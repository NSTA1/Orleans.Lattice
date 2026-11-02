#Requires -Version 7.0
<#
.SYNOPSIS
    Downloads a journey's screenshots from an Explorer Journeys run and prints
    the pull request's screenshot section: the ordered captions and the
    provenance marker (#5363, epic #5401 D10).

.DESCRIPTION
    Every member pull request of the Explorer journeys epic carries a
    `## Journey screenshots` section: one captioned image per manifest entry, in
    order, and a marker naming the head commit, the run and the manifest's
    sha256. The lane's provenance check (`journey-screenshot-provenance.py`)
    verifies that marker against the run's `journey-screenshots-<jid>` artifact.

    This script downloads that artifact (or, when the run has none, finds the
    journey in the run's leg artifacts), prints each image's file and caption in
    order, and prints a section skeleton ending in the marker line. Upload the
    images as GitHub attachments (the github-pr-media skill), replace each
    placeholder URL, and paste the section into the pull request body. Editing
    the body re-runs the lane's provenance check.

    Needs the GitHub CLI signed in (`gh auth status`), or GH_TOKEN set.

.PARAMETER Run
    The Explorer Journeys run id the screenshots come from: a run on the pull
    request's head commit.

.PARAMETER Journey
    The journey id: the lower-case journey folder name, such as `a3`.

.PARAMETER Repository
    The repository, owner/name. Defaults to the current checkout's.

.PARAMETER OutputDirectory
    Where to download the artifact. Defaults to `.scratch/journey-screenshots/<jid>-<run>`.

.EXAMPLE
    pwsh tools/Get-JourneyScreenshots.ps1 -Run 41300000000 -Journey a3
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [long] $Run,

    [Parameter(Mandatory)]
    [ValidatePattern('^[a-z0-9][a-z0-9-]*$')]
    [string] $Journey,

    [string] $Repository,

    [string] $OutputDirectory
)

$ErrorActionPreference = 'Stop'

function Invoke-Gh {
    param([string[]] $Arguments)
    $output = & gh @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "gh $($Arguments -join ' ') failed: $output"
    }
    return $output
}

if (-not $Repository) {
    $Repository = (Invoke-Gh @('repo', 'view', '--json', 'nameWithOwner', '--jq', '.nameWithOwner') | Out-String).Trim()
}

if (-not $OutputDirectory) {
    $root = (& git rev-parse --show-toplevel 2>$null)
    if (-not $root) { $root = (Get-Location).Path }
    $OutputDirectory = Join-Path $root ".scratch/journey-screenshots/$Journey-$Run"
}

$runInfo = Invoke-Gh @('api', "repos/$Repository/actions/runs/$Run") | Out-String | ConvertFrom-Json
if ($runInfo.path -notlike '.github/workflows/explorer-journeys.yml*') {
    Write-Warning "Run $Run is a run of '$($runInfo.path)', not explorer-journeys.yml; the provenance check will refuse it."
}

$names = @(Invoke-Gh @('api', '--paginate', "repos/$Repository/actions/runs/$Run/artifacts?per_page=100", '--jq', '.artifacts[] | select(.expired | not) | .name'))
$exact = "journey-screenshots-$Journey"
$tiers = if ($names -contains $exact) {
    , @($exact)
} else {
    @(, @($names | Where-Object { $_ -like 'journey-screenshots-leg-*' } | Sort-Object)) + @(, @($names | Where-Object { $_ -eq 'explorer-journeys' }))
}

$found = @()
foreach ($tier in $tiers) {
    foreach ($name in $tier) {
        $target = Join-Path $OutputDirectory $name
        if (-not (Test-Path $target)) {
            Invoke-Gh @('run', 'download', "$Run", '--repo', $Repository, '--name', $name, '--dir', $target) | Out-Null
        }

        if ($name -eq $exact) {
            $manifest = Join-Path $target 'manifest.json'
            if (Test-Path $manifest) { $found += Get-Item $manifest }
        } else {
            $found += @(Get-ChildItem -Path $target -Recurse -Filter manifest.json | Where-Object { $_.Directory.Name -eq $Journey })
        }
    }

    if ($found.Count -gt 0) { break }
}

if ($found.Count -eq 0) {
    throw "Run $Run holds no manifest for journey '$Journey'."
}
if ($found.Count -gt 1) {
    throw "Run $Run holds $($found.Count) manifests for journey '$Journey': $($found.FullName -join ', ')"
}

$manifestFile = $found[0]
$digest = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifestFile.FullName).Hash.ToLowerInvariant()
$manifest = Get-Content -Raw -LiteralPath $manifestFile.FullName | ConvertFrom-Json
$entries = @($manifest.entries | Sort-Object { [int] $_.index })

if ($manifest.journey -ne $Journey) {
    Write-Warning "The manifest names journey '$($manifest.journey)', not '$Journey'."
}
if ($manifest.commit -ne $runInfo.head_sha) {
    Write-Warning "The manifest records commit $($manifest.commit); run $Run ran on $($runInfo.head_sha)."
}
$failed = @($entries | Where-Object { -not $_.passed })
if ($failed.Count -gt 0) {
    Write-Warning "$($failed.Count) checkpoint(s) failed in this run; the provenance check refuses a manifest with a failed checkpoint."
}

Write-Host "Journey '$Journey', run $Run, commit $($manifest.commit), $($manifest.viewport) $($manifest.width)x$($manifest.height)."
Write-Host "Images, in order (upload each, then replace its placeholder URL):"
foreach ($entry in $entries) {
    $file = Join-Path $manifestFile.DirectoryName $entry.file
    Write-Host ("  {0,3}. {1}" -f $entry.index, $file)
}

Write-Host ''
Write-Host '----- paste below this line -----'
Write-Host '## Journey screenshots'
Write-Host ''
Write-Host "From [run $Run](https://github.com/$Repository/actions/runs/$Run) on head ``$($manifest.commit.Substring(0, 9))``, $($manifest.viewport) $($manifest.width)x$($manifest.height), the ``$Journey`` manifest in order."
Write-Host ''
foreach ($entry in $entries) {
    Write-Host ("**{0}. ``{1}`` / ``{2}`` ({3})** - {4}" -f $entry.index, $entry.step, $entry.substep, $entry.persona, $entry.title)
    Write-Host ''
    Write-Host ("![{0}](<upload {1}>)" -f $entry.title, $entry.file)
    Write-Host ''
}
Write-Host ("<!-- journey-screenshots: journey={0} sha={1} run={2} manifest={3} count={4} -->" -f $Journey, $manifest.commit, $Run, $digest, $entries.Count)
