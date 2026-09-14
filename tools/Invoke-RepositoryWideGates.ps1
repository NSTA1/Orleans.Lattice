<#
.SYNOPSIS
    Runs every repository-wide metric gate, one filter per gate, and reports the
    EXECUTED count for each.

.DESCRIPTION
    The repository-wide gates do not all live in one test project, so a per-package
    pre-PR run is structurally blind to them. The population is stated once, in the
    gate table in .github/instructions/testing.instructions.md, and enforced against
    a source scan by RepositoryWideGateEnrolmentTests.

    This script DERIVES its run list from that same table. It contains no fixture
    names of its own, so adding a row to the table adds it to the run with no second
    edit, and the run list cannot drift from the enforced population.

    It exists because a correct document is not a correct run. A worker deliberately
    verifying the gates ran six of eleven and missed every gate outside test/lattice/,
    which is the specific blindness the table already warns about. See issue #3020.

    Every gate is run as its own --filter. OR-ing them into one filter crashes the
    vstest host and misattributes the failure to whichever fixture was running.

    The EXECUTED count is the point of the report, not the verdict. A filter that
    matches nothing prints "No test matches the given testcase filter" and EXITS 0 -
    and under --verbosity quiet it prints nothing at all - so a gate that does not
    exist is byte-identical to a gate that passed. See issue #3017. This script
    refuses any gate whose EXECUTED count is zero.

.PARAMETER Emit
    Resolve and print the run list as TSV (fixture, project, project file, filter) and
    exit without running anything. Used by the cross-check fixture to compare this
    script's derivation against the one computed from source.

.PARAMETER Fixture
    Run an explicitly named fixture set instead of the derived repository-wide gates.
    The reporting mechanism is identical, so this is also how the deliberately-bogus-name
    control is driven through the real reader rather than through a synthetic seam. A run
    using this parameter is NOT the repository-wide run and never claims to be.

.PARAMETER Project
    The test project directory an explicitly named fixture set lives in. Only meaningful
    with -Fixture. Defaults to the core test project.

.PARAMETER NoBuild
    Pass --no-build to each dotnet test invocation. Only safe when the test projects
    are already built from current sources. MSBuild keys off timestamps, not content,
    so do not use this after restoring a file in a way that preserves its mtime.

.EXAMPLE
    pwsh tools/Invoke-RepositoryWideGates.ps1

.EXAMPLE
    pwsh tools/Invoke-RepositoryWideGates.ps1 -Emit

.EXAMPLE
    pwsh tools/Invoke-RepositoryWideGates.ps1 -Fixture ThisFixtureDoesNotExistControl
#>
[CmdletBinding()]
param(
    [switch] $Emit,
    [string[]] $Fixture,
    [string] $Project = 'test/lattice',
    [switch] $NoBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$InstructionsRelativePath = '.github/instructions/testing.instructions.md'
$TableHeaderPrefix = '| fixture | project |'

function Find-RepoRoot {
    $dir = $PSScriptRoot
    while ($null -ne $dir -and $dir -ne '') {
        if (Test-Path -LiteralPath (Join-Path $dir 'Orleans.Lattice.slnx')) {
            return $dir
        }

        $parent = Split-Path -Parent $dir
        if ($parent -eq $dir) {
            break
        }

        $dir = $parent
    }

    throw "Could not locate the repository root (no Orleans.Lattice.slnx above $PSScriptRoot)."
}

function Get-DocumentedGate {
    param([Parameter(Mandatory)] [string] $RepoRoot)

    $path = Join-Path $RepoRoot ($InstructionsRelativePath -replace '/', [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $path)) {
        throw "The gate table source is missing: $path"
    }

    $lines = [IO.File]::ReadAllLines($path)

    $headerIndex = -1
    for ($i = 0; $i -lt $lines.Length; $i++) {
        if ($lines[$i].StartsWith($TableHeaderPrefix, [StringComparison]::Ordinal)) {
            $headerIndex = $i
            break
        }
    }

    if ($headerIndex -lt 0) {
        throw ("$InstructionsRelativePath no longer contains the repository-wide gate table " +
            "header '$TableHeaderPrefix'. Without it this script would derive an empty run " +
            'list, run nothing, and report success.')
    }

    $rows = @()
    for ($i = $headerIndex + 1; $i -lt $lines.Length; $i++) {
        $line = $lines[$i]
        if (-not $line.StartsWith('|', [StringComparison]::Ordinal)) {
            break
        }

        $cells = $line.Split('|')
        if ($cells.Length -lt 3) {
            continue
        }

        $fixture = $cells[1].Trim().Trim('`').Trim()
        $project = $cells[2].Trim().Trim('`').Trim().TrimEnd('/')

        if ($fixture.Length -eq 0) {
            continue
        }

        # The markdown separator row, which is all hyphens.
        if ($fixture -match '^-+$') {
            continue
        }

        $rows += [pscustomobject]@{
            Fixture = $fixture
            Project = $project
        }
    }

    if ($rows.Count -eq 0) {
        throw ("Parsed zero gates from the table in $InstructionsRelativePath. A run list of " +
            'zero would execute nothing and report success, which is the exact defect this ' +
            'script exists to remove.')
    }

    return $rows
}

function Resolve-ProjectFile {
    param(
        [Parameter(Mandatory)] [string] $RepoRoot,
        [Parameter(Mandatory)] [string] $Project
    )

    $dir = Join-Path $RepoRoot ($Project -replace '/', [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $dir)) {
        throw "The gate table names project directory '$Project', which does not exist at $dir."
    }

    $found = @(Get-ChildItem -LiteralPath $dir -Filter '*.csproj' -File)
    if ($found.Count -ne 1) {
        throw ("Expected exactly one .csproj in '$Project' but found $($found.Count). " +
            'The run list maps a gate to a project by directory, so this must be unambiguous.')
    }

    return $found[0].FullName
}

function Resolve-FixtureFilter {
    param(
        [Parameter(Mandatory)] [string] $ProjectDirectory,
        [Parameter(Mandatory)] [string] $FixtureName
    )

    # FullyQualifiedName is matched by substring, so a bare fixture name also selects any
    # longer identifier containing it. Qualifying with the declaring namespace and a
    # trailing dot anchors the filter to members of exactly this type.
    #
    # Deriving the namespace rather than assuming one is load-bearing: the gates do not
    # share a namespace, and a namespace-shaped filter is not a safe proxy for the gate
    # set in either direction. Tightening a loose filter to a namespace silently drops
    # every gate declared outside it.
    $candidates = @(
        Get-ChildItem -LiteralPath $ProjectDirectory -Filter '*.cs' -File -Recurse |
            Where-Object {
                $base = [IO.Path]::GetFileNameWithoutExtension($_.Name)
                $dot = $base.IndexOf('.')
                $owner = if ($dot -lt 0) { $base } else { $base.Substring(0, $dot) }
                $owner -eq $FixtureName
            })

    foreach ($candidate in $candidates) {
        foreach ($line in [IO.File]::ReadAllLines($candidate.FullName)) {
            if ($line -match '^\s*namespace\s+([\w\.]+)') {
                return "FullyQualifiedName~$($Matches[1]).$FixtureName."
            }
        }
    }

    # No source file, so nothing to qualify with. This is the shape a deliberately bogus
    # name takes, and it must still produce a runnable filter so the reader reports a zero
    # executed count rather than the run being skipped before it starts.
    return "FullyQualifiedName~$FixtureName"
}

$repoRoot = Find-RepoRoot

if ($Fixture -and $Fixture.Count -gt 0) {
    $gates = @($Fixture | ForEach-Object {
            [pscustomobject]@{
                Fixture = $_
                Project = $Project.TrimEnd('/')
            }
        })
    $derived = $false
}
else {
    $gates = @(Get-DocumentedGate -RepoRoot $repoRoot)
    $derived = $true
}

foreach ($gate in $gates) {
    $projectFile = Resolve-ProjectFile -RepoRoot $repoRoot -Project $gate.Project
    $gate | Add-Member -NotePropertyName ProjectFile -NotePropertyValue $projectFile
    $gate | Add-Member -NotePropertyName Filter -NotePropertyValue (
        Resolve-FixtureFilter -ProjectDirectory (Split-Path -Parent $projectFile) -FixtureName $gate.Fixture)
}

if ($Emit) {
    foreach ($gate in $gates) {
        $relative = $gate.ProjectFile.Substring($repoRoot.Length).TrimStart([IO.Path]::DirectorySeparatorChar, [char]'/')
        $relative = $relative -replace '\\', '/'
        "$($gate.Fixture)`t$($gate.Project)`t$relative`t$($gate.Filter)"
    }

    exit 0
}

if ($derived) {
    Write-Host "Repository-wide gates: $($gates.Count), derived from $InstructionsRelativePath"
}
else {
    Write-Host "NAMED FIXTURE RUN - this is NOT the repository-wide gate run."
    Write-Host "Fixtures: $($gates.Count), supplied on the command line, in $Project"
}

Write-Host ''

$summaryPattern = '(?<verdict>Passed|Failed)!\s+-\s+Failed:\s+(?<failed>\d+),\s+Passed:\s+(?<passed>\d+),\s+Skipped:\s+(?<skipped>\d+),\s+Total:\s+(?<total>\d+)'

$results = @()
foreach ($gate in $gates) {
    $arguments = @(
        'test'
        $gate.ProjectFile
        '--filter'
        $gate.Filter
    )

    if ($NoBuild) {
        $arguments += '--no-build'
    }

    Write-Host "==> $($gate.Fixture)  [$($gate.Project)]"

    $output = & dotnet @arguments 2>&1
    $text = ($output | Out-String)

    $executed = 0
    $failed = 0
    $skipped = 0
    $sawSummary = $false

    # The reader must be able to represent zero, which is the one value it exists to read.
    # A no-match run prints NO summary line at all and exits 0, so absence is tracked
    # separately from the count rather than being allowed to fall through as a number.
    # Counting result nodes instead is the trap: an empty node set wraps to a one-element
    # array containing an empty node, so an absent gate reports as a small passing one -
    # correct at every non-zero value and wrong precisely at zero.
    foreach ($line in ($text -split "`r?`n")) {
        if ($line -match $summaryPattern) {
            $sawSummary = $true
            $executed = [int]$Matches['total']
            $failed = [int]$Matches['failed']
            $skipped = [int]$Matches['skipped']
        }
    }

    $status = if (-not $sawSummary) {
        'NO-SUMMARY'
    }
    elseif ($executed -eq 0) {
        'ZERO-EXECUTED'
    }
    elseif ($failed -gt 0) {
        'FAILED'
    }
    else {
        'OK'
    }

    if ($status -ne 'OK') {
        Write-Host $text
    }

    $results += [pscustomobject]@{
        Fixture  = $gate.Fixture
        Project  = $gate.Project
        Executed = $executed
        Failed   = $failed
        Skipped  = $skipped
        Status   = $status
    }

    Write-Host "    EXECUTED=$executed FAILED=$failed SKIPPED=$skipped $status"
    Write-Host ''
}

$label = if ($derived) { 'REPOSITORY-WIDE GATES' } else { 'NAMED FIXTURE RUN' }

Write-Host ''
Write-Host "--- $($label.ToLowerInvariant()) summary ---"
foreach ($result in $results) {
    '{0,-40} {1,-26} EXECUTED={2,-5} FAILED={3,-4} {4}' -f
        $result.Fixture, $result.Project, $result.Executed, $result.Failed, $result.Status |
        Write-Host
}

$projects = @($results | ForEach-Object { $_.Project } | Sort-Object -Unique)
Write-Host ''
Write-Host "gates: $($results.Count)  projects: $($projects.Count) ($($projects -join ', '))"

$bad = @($results | Where-Object { $_.Status -ne 'OK' })
if ($bad.Count -gt 0) {
    Write-Host ''
    Write-Host "$label FAILED: $($bad.Count) of $($results.Count)"
    foreach ($result in $bad) {
        Write-Host "  $($result.Fixture): $($result.Status)"
    }

    exit 1
}

Write-Host ''
Write-Host "$label OK: $($results.Count) gates, all with a non-zero executed count"
exit 0
