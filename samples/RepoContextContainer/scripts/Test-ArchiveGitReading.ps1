#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Git-dependent coverage for the archive durability check's IMPURE half: the
	conformance between the pinned git-message constants and the git that is
	actually installed, and the wiring of `Get-ArchiveGitReading` itself.

.DESCRIPTION
	`Test-ContainerProvenance.ps1` is deliberately git-free: it drives the pure
	predicate `Test-GitReadingIsExaminable` against LITERAL stderr strings
	captured once, by hand, from a specific git. That is the right way to build a
	fast pure test, and it is complete on its own terms - but it has one weakness
	its author named rather than hid: the eight fixtures encode an external
	program's wording AS FACT, and nothing in that suite would notice if git
	changed it. If a future git rephrases

		not a git repository (or any of the parent directories)

	every pure fixture stays green while the production predicate silently stops
	recognising the genuine search-exhausted case.

	Note the DIRECTION of that failure, because it decides what this file is for.
	A predicate that no longer recognises the "outside every checkout" answer
	reports UNEXAMINABLE, and the archive durability check refuses an unexaminable
	reading. So git rephrasing that message does not wrongly certify a bad archive
	as durable - it makes the check refuse EVERY archive. This file is therefore
	an OUTAGE-DIAGNOSIS guard, not a correctness gate: it converts a baffling "why
	is every archive suddenly unexaminable" incident into a named test failure
	that points straight at git's wording. Do not delete it as redundant with the
	pure suite; the pure suite is exactly what cannot see this.

	This script SHELLS OUT to real git and constructs real filesystem states, so
	unlike its pure sibling it needs a git binary. It is gated in CI by
	RepoContextArchiveGitReadingScriptTests, which runs it only where git and a
	PowerShell host are present and reports a LOUD, visible skip otherwise - a
	conformance guard that skips silently is counted as coverage, which is the
	exact defect class this bucket exists to catalogue.

	Every git call whose stderr is matched pins LC_ALL=C and LANG=C and restores
	the prior values in a finally, because otherwise the comparison is a test of
	the runner's locale rather than of git's wording: it would pass on one machine
	and mis-match on another. The production reader already does this; this script
	matches it so it compares against the same git production sees.

	Exits 0 when every assertion passes, the failing-assertion count when one or
	more fail, and 3 when git is absent (a loud, distinct skip the C# gate turns
	into an NUnit Ignore rather than a pass).

.EXAMPLE
	cd samples/RepoContextContainer/scripts
	pwsh -File Test-ArchiveGitReading.ps1

.NOTES
	Falsifiability is demonstrated by perturbing the production functions and
	recording which assertion fires. The conformance arm's assertions about LIVE
	git text are, by design, NOT falsifiable by perturbing the predicate - that is
	what makes it a conformance guard rather than a second copy of the predicate.
	They are falsifiable by (a) mutating a pinned constant, or (b) git rephrasing;
	the pinned-constant perturbation is the one demonstrated in the item's report.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_provenance.ps1')

# ---------------------------------------------------------------------------
# The production reader under test, Get-ArchiveGitReading, lives in
# Assert-ContainerProvenance.ps1 - an acquisition script that runs `docker
# inspect` the moment it is dot-sourced, so it cannot be imported for its
# functions. Its text is extracted here and defined into this session verbatim,
# so the ACTUAL production function is exercised rather than a copy. The pure
# predicate it delegates to, Test-GitReadingIsExaminable, is dot-sourced above
# with the rest of _provenance.ps1.
#
# The extraction keys on the function's own header and its single column-0
# closing brace, which the file's one-type-per-line style guarantees. If the
# header is not found the script throws here rather than silently testing
# nothing.
# ---------------------------------------------------------------------------
function Get-ProductionFunctionBody {
	param(
		[Parameter(Mandatory)] [string] $SourceFile,
		[Parameter(Mandatory)] [string] $FunctionName
	)

	$lines = Get-Content -LiteralPath $SourceFile
	$startIndex = -1
	for ($i = 0; $i -lt $lines.Count; $i++) {
		if ($lines[$i] -eq "function $FunctionName {") { $startIndex = $i; break }
	}
	if ($startIndex -lt 0) {
		throw "could not find 'function $FunctionName {' in $SourceFile; the extraction anchor has drifted"
	}

	$endIndex = -1
	for ($i = $startIndex + 1; $i -lt $lines.Count; $i++) {
		if ($lines[$i] -eq '}') { $endIndex = $i; break }
	}
	if ($endIndex -lt 0) {
		throw "could not find the column-0 closing brace of $FunctionName in $SourceFile"
	}

	return ($lines[$startIndex..$endIndex] -join [Environment]::NewLine)
}

# Dot-source the extracted bodies at SCRIPT scope so the functions are defined
# here, not trapped inside a helper's local scope.
#
# Get-ArchiveGitReading does not stand alone: it runs its git probe through
# Invoke-GitProbe, the single funnel that clears $LASTEXITCODE after every
# deliberately-swallowed git failure (issue #2718). Extracting the reader
# without its funnel produced a command-not-found at the first call, which is at
# least loud - but it is loud in Arm 2 only, and it named the wrong culprit. The
# closure check below turns that into a named failure AT EXTRACTION TIME, and
# more importantly catches the case where a future dependency is added and
# nobody remembers this file exists.
$_productionSource = Join-Path $here 'Assert-ContainerProvenance.ps1'
$_extractedNames = @('Invoke-GitProbe', 'Get-ArchiveGitReading')
foreach ($_name in $_extractedNames) {
	. ([scriptblock]::Create((Get-ProductionFunctionBody -SourceFile $_productionSource -FunctionName $_name)))
}

# The closure check. Every function the production script declares is a
# candidate dependency; if the extracted text calls one that was not itself
# extracted, the reader is being tested with a hole in it. Fail here, by name,
# rather than at the first call site.
$_productionLines = Get-Content -LiteralPath $_productionSource
$_declaredNames = @(
	$_productionLines |
		ForEach-Object { if ($_ -match '^function\s+([A-Za-z]+-[A-Za-z]+)\s*\{') { $Matches[1] } }
)
if ($_declaredNames.Count -eq 0) {
	throw "no 'function Verb-Noun {' declarations found in $_productionSource; the closure check would be vacuous"
}
$_extractedText = (
	$_extractedNames |
		ForEach-Object { Get-ProductionFunctionBody -SourceFile $_productionSource -FunctionName $_ }
) -join [Environment]::NewLine
$_missing = @(
	$_declaredNames |
		Where-Object { $_extractedNames -notcontains $_ } |
		Where-Object { $_extractedText -match ("(?m)^[^#]*\b" + [regex]::Escape($_) + "\b") }
)
if ($_missing.Count -gt 0) {
	throw ("the extracted functions call production functions that were NOT extracted: " +
		($_missing -join ', ') + ". Add them to `$_extractedNames in this file.")
}

# ---------------------------------------------------------------------------
# The pinned constants are read out of the pure suite rather than re-declared,
# so this really is a DIFF against the strings the eight fixtures assert on. A
# rename or edit of a constant surfaces here as a thrown error (loud), not as a
# silent divergence between two independent copies.
# ---------------------------------------------------------------------------
$suiteText = Get-Content -Raw -LiteralPath (Join-Path $here 'Test-ContainerProvenance.ps1')

function Get-PinnedConstant {
	param([Parameter(Mandatory)] [string] $Name)

	$pattern = "(?m)^\`$$Name\s*=\s*'([^']*)'"
	$match = [regex]::Match($suiteText, $pattern)
	if (-not $match.Success) {
		throw "pinned constant `$$Name not found in Test-ContainerProvenance.ps1; the conformance guard is diffing against nothing"
	}
	return $match.Groups[1].Value
}

$pinnedMiss = Get-PinnedConstant -Name '_gitMissStderr'
$pinnedOrphan = Get-PinnedConstant -Name '_gitOrphanStderr'
$pinnedCorrupt = Get-PinnedConstant -Name '_gitCorruptStderr'

# The load-bearing substrings, matched with .Contains (literal) so the
# parentheses need no regex escaping. These are what the recognition turns on.
$anchorParenthetical = 'not a git repository (or any of the parent directories)'
$anchorBare = 'not a git repository:'
$anchorCorrupt = 'invalid gitfile format'

# ---------------------------------------------------------------------------
$script:_PassCount = 0
$script:_FailCount = 0

function _Assert {
	param(
		[Parameter(Mandatory)] [string] $Name,
		[Parameter(Mandatory)] [bool] $Condition,
		[string] $Detail = ''
	)
	if ($Condition) {
		$script:_PassCount++
		Write-Host ("  PASS  {0}" -f $Name) -ForegroundColor Green
	}
	else {
		$script:_FailCount++
		Write-Host ("  FAIL  {0}  {1}" -f $Name, $Detail) -ForegroundColor Red
	}
}

function _Section {
	param([Parameter(Mandatory)] [string] $Name)
	Write-Host ''
	Write-Host $Name -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------
# git must be present. A silent skip on an absent git would be counted as
# coverage, so absence is announced loudly and exits with a distinct code the
# gate reads as "ignored", never as "passed".
# ---------------------------------------------------------------------------
$gitPresent = $null -ne (Get-Command git -ErrorAction SilentlyContinue)
if (-not $gitPresent) {
	Write-Host ''
	Write-Host 'SKIPPED: git was not found on PATH.' -ForegroundColor Yellow
	Write-Host 'This conformance guard needs a real git to compare its wording against.' -ForegroundColor Yellow
	Write-Host 'A skip is NOT a pass; the CI gate reports this as an ignored test.' -ForegroundColor Yellow
	exit 3
}

# ---------------------------------------------------------------------------
# Helpers that build real git states and take the reading the way the pure
# predicate expects it. State construction and the toplevel probe are separate:
# the probe is the thing whose stderr is matched, so only it pins the locale.
# ---------------------------------------------------------------------------
function New-ScratchDir {
	$dir = Join-Path ([System.IO.Path]::GetTempPath()) ("argr_" + [guid]::NewGuid().ToString('N'))
	New-Item -ItemType Directory -Path $dir | Out-Null
	return $dir
}

$script:_ScratchRoots = [System.Collections.Generic.List[string]]::new()
function Register-Scratch { param([string] $Dir) $script:_ScratchRoots.Add($Dir) }

function Invoke-Git {
	param([Parameter(Mandatory)] [string] $Dir, [Parameter(Mandatory)] [string[]] $GitArgs)
	# Quiet construction commands; their stderr is not under test.
	& git -C $Dir @GitArgs 2>&1 | Out-Null
	return $LASTEXITCODE
}

function New-GitRepo {
	param([switch] $Detached, [switch] $WithLinkedWorktree)

	$dir = New-ScratchDir
	Register-Scratch $dir
	Invoke-Git -Dir $dir -GitArgs @('init', '-q') | Out-Null
	Invoke-Git -Dir $dir -GitArgs @('-c', 'user.email=t@example.invalid', '-c', 'user.name=t', 'commit', '--allow-empty', '-q', '-m', 'root') | Out-Null

	if ($Detached) {
		$sha = (& git -C $dir rev-parse HEAD).Trim()
		Invoke-Git -Dir $dir -GitArgs @('checkout', '-q', '--detach', $sha) | Out-Null
	}

	if ($WithLinkedWorktree) {
		$wt = Join-Path ([System.IO.Path]::GetTempPath()) ("argrwt_" + [guid]::NewGuid().ToString('N'))
		Register-Scratch $wt
		Invoke-Git -Dir $dir -GitArgs @('worktree', 'add', '-q', $wt) | Out-Null
		return [pscustomobject] @{ Main = $dir; Worktree = $wt }
	}

	return $dir
}

# Run `git rev-parse --show-toplevel` exactly as the reader's probe does, under
# a pinned C locale, and return the raw reading. This is the conformance arm's
# own probe; it must match the locale the production reader uses or it compares
# against a different git.
function Get-ToplevelProbe {
	param([Parameter(Mandatory)] [string] $Dir)

	$savedLcAll = $env:LC_ALL
	$savedLang = $env:LANG
	$stderrFile = [System.IO.Path]::GetTempFileName()
	try {
		$env:LC_ALL = 'C'
		$env:LANG = 'C'
		$out = & git -C $Dir rev-parse --show-toplevel 2>$stderrFile
		$code = $LASTEXITCODE
		$err = [string] (Get-Content -Raw -LiteralPath $stderrFile -ErrorAction SilentlyContinue)
		return [pscustomobject] @{ ExitCode = [int] $code; StdOut = [string] ($out -join "`n"); StdErr = $err }
	}
	finally {
		$env:LC_ALL = $savedLcAll
		$env:LANG = $savedLang
		Remove-Item -LiteralPath $stderrFile -Force -ErrorAction SilentlyContinue
	}
}

# ===========================================================================
_Section 'Arm 1 of 2: the pinned git-message constants still describe live git'
# ===========================================================================
# For each state: prove the state was actually constructed (a precondition that
# fails LOUDLY if setup did not enter the intended path), prove live git still
# carries the load-bearing anchor the pinned constant carries, and prove live
# and pinned classify identically through the production predicate.

# --- Genuine miss: discovery walked to the root and found nothing -----------
$missDir = New-ScratchDir; Register-Scratch $missDir
$miss = Get-ToplevelProbe -Dir $missDir

_Assert -Name 'PRECONDITION genuine-miss state built: exit 128, empty stdout' `
	-Condition ($miss.ExitCode -eq 128 -and [string]::IsNullOrWhiteSpace($miss.StdOut)) `
	-Detail ("exit={0} stdout=[{1}] stderr=[{2}]" -f $miss.ExitCode, $miss.StdOut, $miss.StdErr)

_Assert -Name 'live git still emits the parenthetical form for a genuine miss' `
	-Condition ($miss.StdErr.Contains($anchorParenthetical)) `
	-Detail ("live stderr=[{0}]" -f $miss.StdErr)

_Assert -Name 'the pinned _gitMissStderr constant still carries the parenthetical anchor' `
	-Condition ($pinnedMiss.Contains($anchorParenthetical)) `
	-Detail ("pinned=[{0}]" -f $pinnedMiss)

_Assert -Name 'live and pinned genuine-miss agree: both examinable' `
	-Condition ((Test-GitReadingIsExaminable -ExitCode $miss.ExitCode -StandardError $miss.StdErr -Toplevel '') `
			-and (Test-GitReadingIsExaminable -ExitCode 128 -StandardError $pinnedMiss -Toplevel '')) `
	-Detail ("live=[{0}] pinned=[{1}]" -f $miss.StdErr, $pinnedMiss)

# --- Orphaned linked worktree: the case that makes the parenthetical matter --
$orphanBuild = New-GitRepo -WithLinkedWorktree
Remove-Item -Recurse -Force (Join-Path $orphanBuild.Main '.git' 'worktrees')
$orphan = Get-ToplevelProbe -Dir $orphanBuild.Worktree

_Assert -Name 'PRECONDITION orphaned-worktree state built: exit 128, empty stdout' `
	-Condition ($orphan.ExitCode -eq 128 -and [string]::IsNullOrWhiteSpace($orphan.StdOut)) `
	-Detail ("exit={0} stdout=[{1}] stderr=[{2}]" -f $orphan.ExitCode, $orphan.StdOut, $orphan.StdErr)

_Assert -Name 'live orphaned-worktree stderr carries the BARE phrase but NOT the parenthetical' `
	-Condition ($orphan.StdErr.Contains($anchorBare) -and -not $orphan.StdErr.Contains($anchorParenthetical)) `
	-Detail ("live stderr=[{0}]" -f $orphan.StdErr)

_Assert -Name 'the pinned _gitOrphanStderr constant matches that shape: bare, no parenthetical' `
	-Condition ($pinnedOrphan.Contains($anchorBare) -and -not $pinnedOrphan.Contains($anchorParenthetical)) `
	-Detail ("pinned=[{0}]" -f $pinnedOrphan)

_Assert -Name 'live and pinned orphaned-worktree agree: both NOT examinable' `
	-Condition ((-not (Test-GitReadingIsExaminable -ExitCode $orphan.ExitCode -StandardError $orphan.StdErr -Toplevel '')) `
			-and (-not (Test-GitReadingIsExaminable -ExitCode 128 -StandardError $pinnedOrphan -Toplevel ''))) `
	-Detail ("live=[{0}] pinned=[{1}]" -f $orphan.StdErr, $pinnedOrphan)

# --- Corrupt .git file ------------------------------------------------------
$corruptDir = New-ScratchDir; Register-Scratch $corruptDir
Set-Content -LiteralPath (Join-Path $corruptDir '.git') -Value 'garbage' -NoNewline
$corrupt = Get-ToplevelProbe -Dir $corruptDir

_Assert -Name 'PRECONDITION corrupt-gitfile state built: exit 128, empty stdout' `
	-Condition ($corrupt.ExitCode -eq 128 -and [string]::IsNullOrWhiteSpace($corrupt.StdOut)) `
	-Detail ("exit={0} stdout=[{1}] stderr=[{2}]" -f $corrupt.ExitCode, $corrupt.StdOut, $corrupt.StdErr)

_Assert -Name 'live corrupt-gitfile stderr carries the invalid-gitfile-format anchor' `
	-Condition ($corrupt.StdErr.Contains($anchorCorrupt)) `
	-Detail ("live stderr=[{0}]" -f $corrupt.StdErr)

_Assert -Name 'the pinned _gitCorruptStderr constant carries the invalid-gitfile-format anchor' `
	-Condition ($pinnedCorrupt.Contains($anchorCorrupt)) `
	-Detail ("pinned=[{0}]" -f $pinnedCorrupt)

_Assert -Name 'live and pinned corrupt-gitfile agree: both NOT examinable' `
	-Condition ((-not (Test-GitReadingIsExaminable -ExitCode $corrupt.ExitCode -StandardError $corrupt.StdErr -Toplevel '')) `
			-and (-not (Test-GitReadingIsExaminable -ExitCode 128 -StandardError $pinnedCorrupt -Toplevel ''))) `
	-Detail ("live=[{0}] pinned=[{1}]" -f $corrupt.StdErr, $pinnedCorrupt)

# ===========================================================================
_Section 'Arm 2 of 2: Get-ArchiveGitReading wires real git output through faithfully'
# ===========================================================================

# --- Genuine miss: examinable, no toplevel. This single reading proves BOTH
#     that the reader did not swallow the non-zero exit AND that it did not
#     truncate the stderr: the predicate returns true here ONLY when it is
#     handed the real 128 together with the full parenthetical text, so a reader
#     that passed 0, or a clipped stderr, would flip Examinable to false.
$rMiss = Get-ArchiveGitReading -Path $missDir
_Assert -Name 'reader: genuine miss is Exists=true, Examinable=true, Toplevel empty' `
	-Condition ($rMiss.Exists -and $rMiss.Examinable -and [string]::IsNullOrWhiteSpace($rMiss.Toplevel)) `
	-Detail ("exists={0} examinable={1} toplevel=[{2}]" -f $rMiss.Exists, $rMiss.Examinable, $rMiss.Toplevel)

# --- Orphaned linked worktree: NOT examinable, though its stderr carries the
#     bare phrase. This is the fail-open the whole check exists to prevent.
$rOrphan = Get-ArchiveGitReading -Path $orphanBuild.Worktree
_Assert -Name 'reader: orphaned linked worktree is Exists=true, Examinable=FALSE' `
	-Condition ($rOrphan.Exists -and -not $rOrphan.Examinable) `
	-Detail ("exists={0} examinable={1} toplevel=[{2}]" -f $rOrphan.Exists, $rOrphan.Examinable, $rOrphan.Toplevel)

# --- Corrupt .git file: NOT examinable.
$rCorrupt = Get-ArchiveGitReading -Path $corruptDir
_Assert -Name 'reader: corrupt gitfile is Exists=true, Examinable=FALSE' `
	-Condition ($rCorrupt.Exists -and -not $rCorrupt.Examinable) `
	-Detail ("exists={0} examinable={1}" -f $rCorrupt.Exists, $rCorrupt.Examinable)

# --- Healthy primary checkout: examinable, toplevel set, NOT a linked worktree.
$healthyDir = New-GitRepo
$rHealthy = Get-ArchiveGitReading -Path $healthyDir
_Assert -Name 'reader: healthy primary checkout is Examinable=true with a toplevel' `
	-Condition ($rHealthy.Examinable -and -not [string]::IsNullOrWhiteSpace($rHealthy.Toplevel)) `
	-Detail ("examinable={0} toplevel=[{1}]" -f $rHealthy.Examinable, $rHealthy.Toplevel)

_Assert -Name 'reader: a primary checkout is NOT reported as a linked worktree' `
	-Condition (-not $rHealthy.IsLinkedWorktree) `
	-Detail ("isLinkedWorktree={0}" -f $rHealthy.IsLinkedWorktree)

# --- Detached HEAD: the corpus is pinned detached and this path runs against
#     it, so it must read exactly like an attached checkout.
$detachedDir = New-GitRepo -Detached
$rDetached = Get-ArchiveGitReading -Path $detachedDir
_Assert -Name 'reader: detached HEAD is Examinable=true with a toplevel' `
	-Condition ($rDetached.Examinable -and -not [string]::IsNullOrWhiteSpace($rDetached.Toplevel)) `
	-Detail ("examinable={0} toplevel=[{1}]" -f $rDetached.Examinable, $rDetached.Toplevel)

# --- Healthy LINKED worktree: examinable, and reported AS a linked worktree so
#     the operator is told the worktree remedy rather than the checkout one.
$linked = New-GitRepo -WithLinkedWorktree
$rLinked = Get-ArchiveGitReading -Path $linked.Worktree
_Assert -Name 'reader: a healthy linked worktree is Examinable=true AND IsLinkedWorktree=true' `
	-Condition ($rLinked.Examinable -and $rLinked.IsLinkedWorktree) `
	-Detail ("examinable={0} isLinkedWorktree={1}" -f $rLinked.Examinable, $rLinked.IsLinkedWorktree)

# --- A path that does not exist on this host: not examinable, and reported as
#     not present rather than as a clean outside-every-checkout answer.
$absentPath = Join-Path ([System.IO.Path]::GetTempPath()) ("argr_absent_" + [guid]::NewGuid().ToString('N'))
$rAbsent = Get-ArchiveGitReading -Path $absentPath
_Assert -Name 'reader: a non-existent path is Exists=FALSE and Examinable=FALSE' `
	-Condition (-not $rAbsent.Exists -and -not $rAbsent.Examinable) `
	-Detail ("exists={0} examinable={1}" -f $rAbsent.Exists, $rAbsent.Examinable)

# --- LC_ALL / LANG are actually IN FORCE at the invocation site, not merely
#     intended: the reader must set them for its git call and restore whatever
#     was there before. A sentinel is planted, the reader is invoked, and the
#     sentinel must survive - which it can only do if the reader saved, set, and
#     restored it around the call. Perturbing away the finally restore turns
#     these two red.
$savedLcAll = $env:LC_ALL
$savedLang = $env:LANG
try {
	$env:LC_ALL = 'argr-sentinel-lc'
	$env:LANG = 'argr-sentinel-lang'
	$null = Get-ArchiveGitReading -Path $healthyDir
	_Assert -Name 'reader: restores the caller LC_ALL after pinning C for its git call' `
		-Condition ($env:LC_ALL -eq 'argr-sentinel-lc') `
		-Detail ("LC_ALL after call=[{0}]" -f $env:LC_ALL)
	_Assert -Name 'reader: restores the caller LANG after pinning C for its git call' `
		-Condition ($env:LANG -eq 'argr-sentinel-lang') `
		-Detail ("LANG after call=[{0}]" -f $env:LANG)
}
finally {
	$env:LC_ALL = $savedLcAll
	$env:LANG = $savedLang
}

# ---------------------------------------------------------------------------
# Best-effort cleanup of the scratch trees. A worktree admin dir may hold a
# handle, so failures here are ignored; the OS temp sweep is the backstop.
foreach ($root in $script:_ScratchRoots) {
	try { if (Test-Path -LiteralPath $root) { Remove-Item -Recurse -Force -LiteralPath $root -ErrorAction SilentlyContinue } } catch { }
}

# ---------------------------------------------------------------------------
Write-Host ''
Write-Host ('  Total {0}   Passed {1}   Failed {2}' -f ($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount)
Write-Host ''

exit $script:_FailCount
