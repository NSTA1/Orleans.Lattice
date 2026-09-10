#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Regression tests for the RepoContext container provenance check.

.DESCRIPTION
	Exercises the pure adjudication functions in _provenance.ps1 against literal
	fixtures, so the check's correctness can be verified without Docker, without
	a running container, and without a second checkout. Pure pwsh; no Pester
	dependency, matching benchmark/coldstart-rig/scripts/Test-RigHelpers.ps1.

	EVERY CHECK IS TESTED IN BOTH DIRECTIONS, and that is the point of this file
	rather than a courtesy. A check only ever observed passing is
	indistinguishable from one that cannot fail: both are consistent with the
	mechanism working and with the mechanism being unwired, and only a case that
	is supposed to REFUSE separates them. So each of the four checks has at least
	one fixture it accepts and at least one it refuses, and the refusals assert
	on the violation TEXT, since a violation that does not name the disagreeing
	values leaves the operator to go and find them.

	Two fixtures reconstruct the real defect. `Compose provenance REFUSES the
	gate run 1/2 shape` and `Composite REFUSES the reconstructed gate run 2
	readings` use the actual observed values: a container whose compose label
	resolved to the main checkout while the operator stood in the candidate
	worktree, with LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD absent from its
	environment despite being merged on the candidate branch.

	Exits with code 0 when every assertion passes and a non-zero count (equal to
	the number of failed assertions) when one or more fails:

		pwsh -File ./Test-ContainerProvenance.ps1
		if ($LASTEXITCODE -ne 0) { throw "container provenance tests failed" }

.EXAMPLE
	cd samples/RepoContextContainer/scripts
	pwsh -File Test-ContainerProvenance.ps1

.NOTES
	Deliberately not a CI gate, matching the script it tests. Run it by hand
	after changing _provenance.ps1.

	The suite is worth exactly as much as its own discrimination, which is not
	established by it passing. To measure that, break the code and count: commit
	first, then make one function return no violations unconditionally, re-run,
	and confirm the failures are the ones that check that function and NOT the
	others. Identical Totals across the two arms prove both runs executed the
	same set of assertions rather than one arm silently running fewer.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_provenance.ps1')

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

# The checkout the operator believes they deployed, and the one the container
# actually resolved to. These are the two real paths from the epic #2368 gate
# runs, and the entire defect is that nothing in `docker compose up`'s output
# distinguishes them.
$candidateCheckout = 'C:\dev\copilot-worktrees\lattice\bucket4\samples\RepoContextContainer'
$mainCheckout = 'C:\dev\lattice\samples\RepoContextContainer'

$candidateCommit = '9fcaaa998e1d4b7a2c5f0839ab61de7740c2b1a5'
$mainCommit = '3ab77c10f4e2d9856bb0143fcae62d99017b4e8a'

function New-AgreeingReadings {
	return @{
		ContainerName           = 'repocontext'
		ComposeWorkingDirectory = $candidateCheckout
		ComposeConfigFiles      = @((Join-Path $candidateCheckout 'docker-compose.yml'))
		ResolvedCommit          = $candidateCommit
		ExpectedCommit          = $candidateCommit
		RunningImageId          = 'sha256:11112222333344445555666677778888999900001111222233334444555566667'
		ExpectedImageId         = 'sha256:11112222333344445555666677778888999900001111222233334444555566667'
		ImageReference          = 'orleans-lattice/repocontext:local'
		ContainerEnvironment    = @('PATH=/usr/bin', 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=120s')
	}
}

$expectedSettings = @{ 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD' = '120s' }

# ---------------------------------------------------------------------------
_Section 'Path normalisation'
# ---------------------------------------------------------------------------

_Assert -Name 'separators and trailing separator are normalised away' `
	-Condition ((ConvertTo-ProvenancePath -Path 'C:/dev/lattice/samples/RepoContextContainer/') -eq 'C:\dev\lattice\samples\RepoContextContainer')

_Assert -Name 'a drive root keeps its separator' `
	-Condition ((ConvertTo-ProvenancePath -Path 'C:\') -eq 'C:\')

_Assert -Name 'case-insensitive comparison ACCEPTS a case-only difference' `
	-Condition (Test-ProvenancePathsEqual -Left 'C:\Dev\Lattice' -Right 'c:\dev\lattice' -CaseSensitive $false)

# The dangerous direction. Comparing case-insensitively where the filesystem is
# case-sensitive makes two DIFFERENT directories compare equal, which converts a
# real disagreement into a pass in the one check whose only job is to notice a
# disagreement.
_Assert -Name 'case-sensitive comparison REFUSES a case-only difference' `
	-Condition (-not (Test-ProvenancePathsEqual -Left '/srv/Lattice' -Right '/srv/lattice' -CaseSensitive $true))

_Assert -Name 'an empty path is never equal to anything, including another empty path' `
	-Condition (-not (Test-ProvenancePathsEqual -Left '' -Right '' -CaseSensitive $false))

# ---------------------------------------------------------------------------
_Section 'Check 1 of 4: compose provenance'
# ---------------------------------------------------------------------------

$accepted = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout `
	-ConfigFiles @((Join-Path $candidateCheckout 'docker-compose.yml')) `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'ACCEPTS a container composed from the expected checkout' `
	-Condition ($accepted.Count -eq 0) -Detail ($accepted -join '; ')

# The real gate run 1/2 shape: the operator stood in the candidate worktree and
# the container had been composed from the main checkout.
$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $mainCheckout `
	-ConfigFiles @((Join-Path $mainCheckout 'docker-compose.yml')) `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES the gate run 1/2 shape (composed from a different checkout)' `
	-Condition ($refused.Count -eq 1)
_Assert -Name 'and names BOTH the resolved and the expected directory' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains($mainCheckout) -and $refused[0].Contains($candidateCheckout)) `
	-Detail ($refused -join '; ')

$refused = Get-ComposeProvenanceViolation -WorkingDirectory '' -ConfigFiles @() `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES a container carrying no compose working_dir label' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('working_dir'))

$refused = Get-ComposeProvenanceViolation -WorkingDirectory $candidateCheckout -ConfigFiles $null `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES a container carrying no compose config_files label' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('config_files'))

# An override file merged in from outside the project directory is how the
# resolved document stops matching the tracked one while every path still looks
# plausible.
$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout `
	-ConfigFiles @((Join-Path $candidateCheckout 'docker-compose.yml'), 'C:\tmp\docker-compose.override.yml') `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES a config file merged in from outside the project directory' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('override'))

# ---------------------------------------------------------------------------
_Section 'Check 2 of 4: git provenance'
# ---------------------------------------------------------------------------

_Assert -Name 'ACCEPTS a checkout sitting on the expected commit' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit $candidateCommit).Count -eq 0)

_Assert -Name 'ACCEPTS a short commit that prefixes the resolved one' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit '9fcaaa9').Count -eq 0)

$refused = Get-GitProvenanceViolation -ResolvedCommit $mainCommit -ExpectedCommit $candidateCommit
_Assert -Name 'REFUSES a checkout sitting on a different commit' -Condition ($refused.Count -eq 1)
_Assert -Name 'and names BOTH commits' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains($mainCommit) -and $refused[0].Contains($candidateCommit)) `
	-Detail ($refused -join '; ')

_Assert -Name 'REFUSES a working directory that is not a git worktree' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit '' -ExpectedCommit $candidateCommit).Count -eq 1)

_Assert -Name 'REFUSES an expected commit that was never supplied' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit '').Count -eq 1)

# A prefix short enough to match commits it did not mean is refused rather than
# quietly accepted, because a loose match here reports as a verified revision.
_Assert -Name 'REFUSES a commit prefix shorter than seven characters' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit '9fcaa').Count -eq 1)

# ---------------------------------------------------------------------------
_Section 'Check 3 of 4: image provenance'
# ---------------------------------------------------------------------------

_Assert -Name 'ACCEPTS a container running the image its reference resolves to' `
	-Condition ((Get-ImageProvenanceViolation -RunningImageId 'sha256:aaaa' -ExpectedImageId 'sha256:aaaa' -ImageReference 'repocontext:local').Count -eq 0)

$refused = Get-ImageProvenanceViolation -RunningImageId 'sha256:aaaa' -ExpectedImageId 'sha256:bbbb' -ImageReference 'repocontext:local'
_Assert -Name 'REFUSES a stale container left in place across a rebuild' -Condition ($refused.Count -eq 1)
_Assert -Name 'and names both image ids and the reference' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('sha256:aaaa') -and $refused[0].Contains('sha256:bbbb') -and $refused[0].Contains('repocontext:local')) `
	-Detail ($refused -join '; ')

_Assert -Name 'REFUSES a reference that resolves to no image on this host' `
	-Condition ((Get-ImageProvenanceViolation -RunningImageId 'sha256:aaaa' -ExpectedImageId '' -ImageReference 'repocontext:local').Count -eq 1)

_Assert -Name 'REFUSES a container reporting no image id' `
	-Condition ((Get-ImageProvenanceViolation -RunningImageId '' -ExpectedImageId 'sha256:bbbb' -ImageReference 'repocontext:local').Count -eq 1)

# ---------------------------------------------------------------------------
_Section 'Check 4 of 4: environment provenance'
# ---------------------------------------------------------------------------

_Assert -Name 'ACCEPTS a setting present in the container with the expected value' `
	-Condition ((Get-EnvironmentProvenanceViolation `
			-ContainerEnvironment @('PATH=/usr/bin', 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=120s') `
			-ExpectedSettings $expectedSettings).Count -eq 0)

# The gate run 1/2 reading: merged on the candidate branch, absent from the
# process. This is the case no comparison between tracked files could catch.
$refused = Get-EnvironmentProvenanceViolation `
	-ContainerEnvironment @('PATH=/usr/bin') `
	-ExpectedSettings $expectedSettings
_Assert -Name 'REFUSES a setting ABSENT from the container environment' -Condition ($refused.Count -eq 1)
_Assert -Name 'and reports it as ABSENT, naming the setting and the expected value' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('ABSENT') -and $refused[0].Contains('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD') -and $refused[0].Contains('120s')) `
	-Detail ($refused -join '; ')

# Absent and present-but-different have different causes and different remedies.
# Collapsing them would reproduce inside this check the exact defect the check
# exists to catch, so the distinctness is asserted rather than assumed.
$refused = Get-EnvironmentProvenanceViolation `
	-ContainerEnvironment @('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=10s') `
	-ExpectedSettings $expectedSettings
_Assert -Name 'REFUSES a setting present with the wrong value' -Condition ($refused.Count -eq 1)
_Assert -Name 'and does NOT report a wrong value as ABSENT' `
	-Condition ($refused.Count -ge 1 -and -not $refused[0].Contains('ABSENT') -and $refused[0].Contains('10s') -and $refused[0].Contains('120s')) `
	-Detail ($refused -join '; ')

# A check with nothing to assert would pass against any container at all, which
# is the false green this whole item exists to prevent.
_Assert -Name 'REFUSES being asked to assert no settings at all' `
	-Condition ((Get-EnvironmentProvenanceViolation -ContainerEnvironment @('PATH=/usr/bin') -ExpectedSettings @{}).Count -eq 1)

_Assert -Name 'a setting whose value contains "=" is read whole' `
	-Condition ((Get-EnvironmentProvenanceViolation `
			-ContainerEnvironment @('CONNECTION=Host=db;Port=5432') `
			-ExpectedSettings @{ 'CONNECTION' = 'Host=db;Port=5432' }).Count -eq 0)

# ---------------------------------------------------------------------------
_Section 'Composite report'
# ---------------------------------------------------------------------------

$report = Get-ContainerProvenanceReport -Readings (New-AgreeingReadings) `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'ACCEPTS readings on which all four checks agree' `
	-Condition ($report.IsSatisfied -and $report.Violations.Count -eq 0) -Detail ($report.Violations -join '; ')

_Assert -Name 'and returns every reading it was given, so the operator reads the values' `
	-Condition ($report.Readings.ComposeWorkingDirectory -eq $candidateCheckout -and $report.Readings.RunningImageId.StartsWith('sha256:'))

# The reconstructed gate run 2 container: composed from the main checkout, on
# the main commit, and missing the setting that was merged on the candidate.
$gateRun2 = New-AgreeingReadings
$gateRun2.ComposeWorkingDirectory = $mainCheckout
$gateRun2.ComposeConfigFiles = @((Join-Path $mainCheckout 'docker-compose.yml'))
$gateRun2.ResolvedCommit = $mainCommit
$gateRun2.ContainerEnvironment = @('PATH=/usr/bin')

$report = Get-ContainerProvenanceReport -Readings $gateRun2 `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES the reconstructed gate run 2 readings' -Condition (-not $report.IsSatisfied)

# Three separate checks disagree on this container, and all three must be
# reported. A composite that stopped at the first would leave the operator
# fixing the working directory and re-running to discover the next one.
_Assert -Name 'and reports all three disagreements, not merely the first' `
	-Condition ($report.Violations.Count -eq 3) -Detail ($report.Violations -join '; ')

_Assert -Name 'and the environment disagreement is among them' `
	-Condition (@($report.Violations | Where-Object { $_.Contains('ABSENT') }).Count -eq 1) `
	-Detail ($report.Violations -join '; ')

# Each check is independently reachable through the composite: a container that
# is otherwise impeccable but stale on ONE input is refused for that input
# alone. Without these, a composite could satisfy every fixture above while
# three of its four checks were never wired in.
$imageOnly = New-AgreeingReadings
$imageOnly.RunningImageId = 'sha256:9999'
$report = Get-ContainerProvenanceReport -Readings $imageOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on image provenance alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

$commitOnly = New-AgreeingReadings
$commitOnly.ResolvedCommit = $mainCommit
$report = Get-ContainerProvenanceReport -Readings $commitOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on git provenance alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

$environmentOnly = New-AgreeingReadings
$environmentOnly.ContainerEnvironment = @('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=10s')
$report = Get-ContainerProvenanceReport -Readings $environmentOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on environment provenance alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

# ---------------------------------------------------------------------------
Write-Host ''
Write-Host ('  Total {0}   Passed {1}   Failed {2}' -f ($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount)
Write-Host ''

exit $script:_FailCount
