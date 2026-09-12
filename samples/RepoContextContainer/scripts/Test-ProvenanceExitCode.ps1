#!/usr/bin/env pwsh
<#
.SYNOPSIS
	End-to-end coverage for the EXIT STATUS of Assert-ContainerProvenance.ps1:
	that the code a machine reads says what the verdict a person reads says.

.DESCRIPTION
	Its two siblings adjudicate the CONTENT of the provenance verdict.
	Test-ContainerProvenance.ps1 drives the pure functions in _provenance.ps1;
	Test-ArchiveGitReading.ps1 drives the impure git reader. Neither runs the
	script, so neither can see its exit code, and for a whole release the script
	printed a perfect verdict and reported the opposite of it to any caller that
	gated on `$LASTEXITCODE -eq 0` (issue #2718).

	THE DEFECT, and why a naive fix regresses it. The script never called `exit`,
	so its status was whatever the last internal native command left behind. That
	value came from `git rev-parse --show-toplevel` run inside the memory
	archive - a probe that is SUPPOSED to fail, because an archive correctly
	located outside every checkout makes git exit 128, which is the PASS
	condition for check 5. The script's own success condition produced its
	failure status, and the only arrangement that would have left a zero there is
	a MISPLACED archive, which is the defect check 5 exists to reject. A fix that
	appends `exit 0` and stops has not addressed that: the next probe appended
	below it reintroduces the whole thing silently. Hence the structural arm at
	the end of this file, which is about the SHAPE of the production script and
	not about any single run of it.

	THE CHANNEL MATTERS, and this is the part that decides whether this file is a
	detector or a decoration. `pwsh -File script.ps1` DISCARDS $LASTEXITCODE when
	the script ends without calling `exit`: the process exits 0. So a fixture that
	shells out with -File and asserts 0 on the passing path PASSED BEFORE THE FIX
	AND AFTER IT, while an operator at a prompt - and any wrapper .ps1, which is
	how the runbook invokes this - read 128 from the same run. One run, two
	contradictory statuses, neither chosen by the script. Every arm below
	therefore reads BOTH channels and asserts they agree, because agreement is
	the property that was actually missing and the in-process channel is the only
	one that can see it.

	WHY THERE IS A DOCKER SHIM. The exit code is a property of the whole script,
	so it cannot be obtained by extracting a function or calling a pure
	predicate: the script has to run end to end. Running it against a real
	container would make this suite need a specific deployment, which is what
	kept its exit code untested in the first place. So a tiny `docker` shim is
	placed first on PATH and serves canned readings from files, and the checkout
	and archive are real directories built here - the archive deliberately
	OUTSIDE any checkout, so the 128-producing probe is genuinely exercised
	rather than simulated.

	Exits 0 when every assertion passes, the failing-assertion count when one or
	more fails, and 3 when git is absent (a loud, distinct skip the C# gate turns
	into an NUnit Ignore rather than a pass).

.EXAMPLE
	cd samples/RepoContextContainer/scripts
	pwsh -File Test-ProvenanceExitCode.ps1

.NOTES
	Falsifiability was demonstrated by perturbation, and the result corrected an
	assumption worth recording, because it is the opposite of what the shape of
	the fix suggests.

	The fix has two halves - an explicit `exit 0` on the success path, and a
	funnel that clears $LASTEXITCODE after every swallowed git probe - and EACH
	ONE ALONE is sufficient to make a healthy run exit 0. So reverting either
	half by itself leaves every behavioural arm here GREEN, and only the
	structural arm for that half goes red:

	  revert `exit 0`, keep the funnel
	      red: structural: the success path ends in an explicit exit ...
	  restore a raw `& git`, keep `exit 0`
	      red: structural: EVERY git invocation is funnelled through Invoke-GitProbe
	  revert BOTH (the pre-fix state)
	      red: healthy: the IN-PROCESS channel exits 0
	           healthy: the two channels AGREE ...
	           gating on -eq 0 admits the healthy deployment and rejects the refused one
	           INVERSION: an outside-git archive exits 0 even though its own git probe exits 128
	           plus both structural arms

	Two consequences follow, and neither is obvious from reading the fix.

	First, the structural arms are not belt-and-braces decoration on top of
	behavioural coverage - they are the ONLY detector for a single-half
	regression, because the surviving half masks it completely. Deleting them as
	redundant would leave half the fix unguarded while every test stayed green.

	Second, the acceptance bar as originally stated - "a passing run exits 0, a
	failing run exits non-zero, and a test asserts the two differ" - is NOT
	sufficient on its own. Under the full pre-fix revert the codes were 128 and
	2: they DIFFER, and the arm asserting only that stayed green, as did the arm
	asserting the four terminal outcomes are four distinct codes. What catches it
	is asserting which way round they are, which is why the arm above tests
	`-eq 0` on the passing run and non-zero on the failing one as one condition.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:AssertScript = Join-Path $here 'Assert-ContainerProvenance.ps1'

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

# The exit codes the production script documents. Restated here as literals
# rather than imported, so a change to the production contract has to be made
# deliberately in two places instead of being absorbed silently by a shared
# constant.
$ExitAllChecksAgree = 0
$ExitProvenanceRefused = 2
$ExitContainerNotInterrogable = 3
$ExitExpectedConfigurationUnreadable = 4

# ---------------------------------------------------------------------------
# git is a hard requirement, not an optional enrichment: the sandbox checkout
# must be a real repository for check 2, and the archive probe must be a real
# 128. A skip here is LOUD and carries its own exit code, because a conformance
# suite that goes quietly green having examined nothing is the failure class
# this whole bucket exists to catch.
# ---------------------------------------------------------------------------
if ($null -eq (Get-Command git -ErrorAction SilentlyContinue)) {
	Write-Host '  SKIPPED: git is not available on this host, so the exit-code suite cannot build its sandbox.' -ForegroundColor Yellow
	exit 3
}

$script:PwshPath = try { [System.Diagnostics.Process]::GetCurrentProcess().MainModule.FileName } catch { 'pwsh' }

# ---------------------------------------------------------------------------
# The sandbox. A real git checkout, a real archive directory outside it, and a
# `docker` shim that serves canned readings.
# ---------------------------------------------------------------------------
$script:Sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("provenance-exit-" + [guid]::NewGuid().ToString('N'))
$script:Checkout = Join-Path $script:Sandbox 'checkout'
$script:BareCheckout = Join-Path $script:Sandbox 'checkout-without-the-setting'
$script:Archive = Join-Path $script:Sandbox 'archive'
$script:InsideArchive = Join-Path $script:Checkout 'archive-in-the-wrong-place'
$script:ShimDirectory = Join-Path $script:Sandbox 'shim'
$script:ShimBin = Join-Path $script:Sandbox 'bin'

function New-SandboxCheckout {
	param(
		[Parameter(Mandatory)] [string] $Path,
		[Parameter(Mandatory)] [bool] $DeclaresGracePeriod
	)

	New-Item -ItemType Directory -Force -Path $Path | Out-Null

	$compose = @(
		'services:'
		'  repocontext:'
		'    image: repocontext-mcp:local'
		'    environment:'
	)
	if ($DeclaresGracePeriod) {
		$compose += '      LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD: 120s'
	}
	Set-Content -LiteralPath (Join-Path $Path 'docker-compose.yml') -Value $compose -Encoding utf8
	Set-Content -LiteralPath (Join-Path $Path 'docker-compose.override.yml') `
		-Value @('services:', '  repocontext:', '    mem_limit: 4g') -Encoding utf8

	# A real repository, because check 2 reads a real HEAD and check 6's
	# chronology arm reads a real commit date. `git init` is given an explicit
	# identity so it works on a runner with no global git config.
	& git -C $Path init --quiet --initial-branch=main 2>&1 | Out-Null
	& git -C $Path config user.email 'provenance@example.invalid' 2>&1 | Out-Null
	& git -C $Path config user.name 'Provenance Exit Code Suite' 2>&1 | Out-Null
	& git -C $Path add -A 2>&1 | Out-Null
	& git -C $Path commit --quiet -m 'sandbox checkout' 2>&1 | Out-Null

	$commit = ("$(& git -C $Path rev-parse HEAD 2>$null)").Trim()
	$global:LASTEXITCODE = 0
	return $commit
}

function New-DockerShim {
	New-Item -ItemType Directory -Force -Path $script:ShimBin | Out-Null

	if ($IsWindows) {
		# Discriminates on the --format argument rather than on the image
		# reference, because the reference differs between fixtures while the
		# format string is fixed by the production script.
		$shim = @(
			'@echo off'
			'if /I "%~1"=="image" goto image'
			'if exist "%PROVENANCE_SHIM_DIR%\fail.txt" ('
			'    type "%PROVENANCE_SHIM_DIR%\fail.txt" 1>&2'
			'    exit /b 1'
			')'
			'type "%PROVENANCE_SHIM_DIR%\container.json"'
			'exit /b 0'
			':image'
			'if "%~5"=="{{json .}}" ('
			'    type "%PROVENANCE_SHIM_DIR%\image.json"'
			') else ('
			'    type "%PROVENANCE_SHIM_DIR%\image-id.txt"'
			')'
			'exit /b 0'
		)
		Set-Content -LiteralPath (Join-Path $script:ShimBin 'docker.cmd') -Value $shim -Encoding ascii
	}
	else {
		$shim = @(
			'#!/bin/sh'
			'if [ "$1" = "image" ]; then'
			'  if [ "$5" = "{{json .}}" ]; then'
			'    cat "$PROVENANCE_SHIM_DIR/image.json"'
			'  else'
			'    cat "$PROVENANCE_SHIM_DIR/image-id.txt"'
			'  fi'
			'  exit 0'
			'fi'
			'if [ -f "$PROVENANCE_SHIM_DIR/fail.txt" ]; then'
			'  cat "$PROVENANCE_SHIM_DIR/fail.txt" >&2'
			'  exit 1'
			'fi'
			'cat "$PROVENANCE_SHIM_DIR/container.json"'
			'exit 0'
		)
		$shimPath = Join-Path $script:ShimBin 'docker'
		Set-Content -LiteralPath $shimPath -Value $shim -Encoding ascii
		& chmod '+x' $shimPath 2>&1 | Out-Null
		$global:LASTEXITCODE = 0
	}
}

# Writes the readings the shim will serve. Every argument has a healthy default
# so an arm names only what it perturbs, which keeps each fixture's departure
# from the passing case visible in one line.
function Set-ShimFixture {
	param(
		[string] $WorkingDirectory = $script:Checkout,
		[string] $ArchiveSource = $script:Archive,
		[string] $ArchiveMountType = 'bind',
		[string] $RevisionLabel = '',
		[string] $GracePeriod = '120s',
		[switch] $OmitGracePeriod,
		[switch] $DockerFails
	)

	if ([string]::IsNullOrWhiteSpace($RevisionLabel)) { $RevisionLabel = $script:CheckoutCommit }

	Remove-Item -LiteralPath (Join-Path $script:ShimDirectory 'fail.txt') -Force -ErrorAction SilentlyContinue
	if ($DockerFails) {
		Set-Content -LiteralPath (Join-Path $script:ShimDirectory 'fail.txt') `
			-Value 'Error response from daemon: No such object: ghost' -Encoding ascii
	}

	$imageId = 'sha256:0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0'
	$environment = @('PATH=/usr/local/bin')
	if (-not $OmitGracePeriod) {
		$environment += "LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=$GracePeriod"
	}

	$configFiles = @(
		(Join-Path $WorkingDirectory 'docker-compose.yml')
		(Join-Path $WorkingDirectory 'docker-compose.override.yml')
	) -join ','

	$container = @{
		Image  = $imageId
		Config = @{
			Image  = 'repocontext-mcp:local'
			Labels = @{
				'com.docker.compose.project.working_dir'  = $WorkingDirectory
				'com.docker.compose.project.config_files' = $configFiles
			}
			Env    = $environment
		}
		Mounts = @(
			@{ Type = $ArchiveMountType; Source = $ArchiveSource; Destination = '/memory-archive' }
		)
	}

	# An hour AHEAD of the sandbox commit, so check 6's chronology arm is
	# admissible and satisfied rather than switched off. An image that postdates
	# the commit it claims can contain it; that is the whole of the arm.
	$image = @{
		Id          = $imageId
		RepoTags    = @('repocontext-mcp:local')
		RepoDigests = @()
		Created     = [datetime]::UtcNow.AddHours(1).ToString('o')
		Config      = @{ Labels = @{ 'org.opencontainers.image.revision' = $RevisionLabel } }
	}

	Set-Content -LiteralPath (Join-Path $script:ShimDirectory 'container.json') `
		-Value (ConvertTo-Json -InputObject @($container) -Depth 10 -AsArray) -Encoding utf8
	Set-Content -LiteralPath (Join-Path $script:ShimDirectory 'image.json') `
		-Value (ConvertTo-Json -InputObject $image -Depth 10) -Encoding utf8
	Set-Content -LiteralPath (Join-Path $script:ShimDirectory 'image-id.txt') -Value $imageId -Encoding ascii
}

# Runs the production script and returns BOTH readings of its status.
#
# `InProcess` is the channel an operator and a wrapper .ps1 see, and the only
# one that can observe a leaked $LASTEXITCODE. `Subprocess` is `pwsh -File`,
# which discards it. $LASTEXITCODE is seeded to 0 first so the value read back
# is attributable to THIS run rather than inherited from the previous arm -
# without that, the suite would be committing a smaller version of the defect it
# is testing for.
function Invoke-AssertScript {
	param(
		[Parameter(Mandatory)] [ValidateSet('InProcess', 'Subprocess')] [string] $Channel,
		[hashtable] $Argument = @{}
	)

	$splat = @{ ContainerName = 'repocontext-exit-code-fixture' }
	foreach ($key in $Argument.Keys) { $splat[$key] = $Argument[$key] }
	if (-not $splat.ContainsKey('ExpectedCheckout')) { $splat['ExpectedCheckout'] = $script:Checkout }

	$global:LASTEXITCODE = 0
	$output = ''

	if ($Channel -eq 'InProcess') {
		try {
			# *>&1 rather than 2>&1: the verdict is written with Write-Host,
			# which lands on the information stream. Note that the refusal
			# messages go to [Console]::Error and bypass the PowerShell streams
			# entirely, so text assertions read the SUBPROCESS output instead -
			# this channel exists to observe the exit CODE, which is the only
			# thing it can see that the subprocess cannot.
			$output = (& $script:AssertScript @splat *>&1 | Out-String)
		}
		catch {
			# Pre-fix, the refusal path THREW. Recorded rather than rethrown so
			# the arm can report what the caller actually saw.
			$output = ($_ | Out-String)
		}
		return @{ ExitCode = [int] $LASTEXITCODE; Output = $output }
	}

	$arguments = @('-NoProfile', '-File', $script:AssertScript)
	foreach ($key in $splat.Keys) {
		$arguments += "-$key"
		$arguments += ,"$($splat[$key])"
	}
	$output = (& $script:PwshPath @arguments 2>&1 | Out-String)
	return @{ ExitCode = [int] $LASTEXITCODE; Output = $output }
}

# A bare probe of the kind the production script runs inside the archive, used
# as a positive control. Returns the exit code git left behind.
function Get-BareGitProbeExitCode {
	param([Parameter(Mandatory)] [string] $Path)

	$global:LASTEXITCODE = 0
	& git -C $Path rev-parse --show-toplevel 2>$null | Out-Null
	$code = [int] $LASTEXITCODE
	$global:LASTEXITCODE = 0
	return $code
}

try {
	New-Item -ItemType Directory -Force -Path $script:Sandbox | Out-Null
	New-Item -ItemType Directory -Force -Path $script:ShimDirectory | Out-Null
	New-Item -ItemType Directory -Force -Path $script:Archive | Out-Null
	$script:CheckoutCommit = New-SandboxCheckout -Path $script:Checkout -DeclaresGracePeriod $true
	New-SandboxCheckout -Path $script:BareCheckout -DeclaresGracePeriod $false | Out-Null
	New-Item -ItemType Directory -Force -Path $script:InsideArchive | Out-Null
	New-DockerShim

	$savedPath = $env:PATH
	$savedShimDirectory = $env:PROVENANCE_SHIM_DIR
	$env:PATH = $script:ShimBin + [System.IO.Path]::PathSeparator + $savedPath
	$env:PROVENANCE_SHIM_DIR = $script:ShimDirectory

	try {
		# -------------------------------------------------------------------
		_Section 'Controls - the sandbox reproduces the conditions the defect needs'
		# -------------------------------------------------------------------

		$archiveProbe = Get-BareGitProbeExitCode -Path $script:Archive
		_Assert -Name 'control: the archive directory lies outside every git checkout' `
			-Condition ($archiveProbe -ne 0) `
			-Detail ("git rev-parse in '$script:Archive' exited $archiveProbe; a temp directory inside a repository would invalidate every arm below")

		_Assert -Name 'control: that probe leaves a NON-ZERO exit code behind, which is the poison' `
			-Condition ($archiveProbe -eq 128) `
			-Detail ("expected git's not-a-repository 128, observed $archiveProbe")

		$checkoutProbe = Get-BareGitProbeExitCode -Path $script:Checkout
		_Assert -Name 'control: the probe DISCRIMINATES - it exits 0 inside the sandbox checkout' `
			-Condition ($checkoutProbe -eq 0) `
			-Detail ("git rev-parse in '$script:Checkout' exited $checkoutProbe; a probe that failed everywhere would make the control above meaningless")

		_Assert -Name 'control: the sandbox checkout resolved a HEAD commit' `
			-Condition ($script:CheckoutCommit -match '^[0-9a-f]{40}$') `
			-Detail ("HEAD=[$script:CheckoutCommit]")

		# -------------------------------------------------------------------
		_Section 'A healthy deployment exits 0 on BOTH channels'
		# -------------------------------------------------------------------

		Set-ShimFixture
		$healthyInProcess = Invoke-AssertScript -Channel InProcess
		$healthySubprocess = Invoke-AssertScript -Channel Subprocess

		_Assert -Name 'healthy: the printed verdict is the all-checks-agree one' `
			-Condition ($healthySubprocess.Output -match 'all six provenance checks agree') `
			-Detail ($healthySubprocess.Output)

		_Assert -Name 'healthy: the IN-PROCESS channel exits 0' `
			-Condition ($healthyInProcess.ExitCode -eq $ExitAllChecksAgree) `
			-Detail ("observed $($healthyInProcess.ExitCode); 128 means the archive git probe leaked its status into the verdict")

		_Assert -Name 'healthy: the SUBPROCESS channel exits 0' `
			-Condition ($healthySubprocess.ExitCode -eq $ExitAllChecksAgree) `
			-Detail ("observed $($healthySubprocess.ExitCode)")

		_Assert -Name 'healthy: the two channels AGREE, which is the property that was missing' `
			-Condition ($healthyInProcess.ExitCode -eq $healthySubprocess.ExitCode) `
			-Detail ("in-process $($healthyInProcess.ExitCode) vs subprocess $($healthySubprocess.ExitCode)")

		# -------------------------------------------------------------------
		_Section 'A refused deployment exits with the refusal code on BOTH channels'
		# -------------------------------------------------------------------

		Set-ShimFixture -OmitGracePeriod
		$refusedInProcess = Invoke-AssertScript -Channel InProcess
		$refusedSubprocess = Invoke-AssertScript -Channel Subprocess

		_Assert -Name 'refused: the printed verdict says REFUSED' `
			-Condition ($refusedSubprocess.Output -match 'provenance REFUSED') `
			-Detail ($refusedSubprocess.Output)

		_Assert -Name 'refused: the IN-PROCESS channel exits NON-ZERO' `
			-Condition ($refusedInProcess.ExitCode -ne 0) `
			-Detail ("observed $($refusedInProcess.ExitCode)")

		_Assert -Name 'refused: the IN-PROCESS channel exits with the refusal code' `
			-Condition ($refusedInProcess.ExitCode -eq $ExitProvenanceRefused) `
			-Detail ("expected $ExitProvenanceRefused, observed $($refusedInProcess.ExitCode)")

		_Assert -Name 'refused: the SUBPROCESS channel exits with the refusal code' `
			-Condition ($refusedSubprocess.ExitCode -eq $ExitProvenanceRefused) `
			-Detail ("expected $ExitProvenanceRefused, observed $($refusedSubprocess.ExitCode)")

		_Assert -Name 'refused: the two channels AGREE' `
			-Condition ($refusedInProcess.ExitCode -eq $refusedSubprocess.ExitCode) `
			-Detail ("in-process $($refusedInProcess.ExitCode) vs subprocess $($refusedSubprocess.ExitCode)")

		# -------------------------------------------------------------------
		_Section 'The exit code DISCRIMINATES pass from fail'
		# -------------------------------------------------------------------

		_Assert -Name 'the passing and failing exit codes DIFFER' `
			-Condition ($healthyInProcess.ExitCode -ne $refusedInProcess.ExitCode) `
			-Detail ("pass $($healthyInProcess.ExitCode), fail $($refusedInProcess.ExitCode); equal codes cannot gate anything")

		_Assert -Name 'gating on -eq 0 admits the healthy deployment and rejects the refused one' `
			-Condition (($healthyInProcess.ExitCode -eq 0) -and ($refusedInProcess.ExitCode -ne 0)) `
			-Detail ("pass $($healthyInProcess.ExitCode), fail $($refusedInProcess.ExitCode)")

		# -------------------------------------------------------------------
		_Section 'The inversion - a CORRECTLY placed archive must not move the exit code'
		# -------------------------------------------------------------------

		# The counter-intuitive arm. The healthy run above already had its
		# archive outside every checkout, which is the PASS condition for check
		# 5 and simultaneously the thing that made git exit 128. Asserting both
		# facts together is what distinguishes this from the healthy arm: an
		# `exit 0` appended without addressing the probe would satisfy the
		# healthy arm today and fail the moment a probe is appended after it.
		_Assert -Name 'INVERSION: an outside-git archive exits 0 even though its own git probe exits 128' `
			-Condition (($healthyInProcess.ExitCode -eq 0) -and ($archiveProbe -eq 128)) `
			-Detail ("script exit $($healthyInProcess.ExitCode), bare probe exit $archiveProbe")

		Set-ShimFixture -ArchiveSource $script:InsideArchive
		$misplacedInProcess = Invoke-AssertScript -Channel InProcess
		$misplacedSubprocess = Invoke-AssertScript -Channel Subprocess

		_Assert -Name 'INVERSION: check 5 is genuinely wired - an archive INSIDE a checkout is refused' `
			-Condition ($misplacedInProcess.ExitCode -eq $ExitProvenanceRefused) `
			-Detail ("expected $ExitProvenanceRefused, observed $($misplacedInProcess.ExitCode)")

		_Assert -Name 'INVERSION: the misplaced-archive refusal names the ARCHIVE, not the compose directory' `
			-Condition ($misplacedSubprocess.Output -match 'archive-in-the-wrong-place') `
			-Detail ($misplacedSubprocess.Output)

		# -------------------------------------------------------------------
		_Section 'Each refusal path carries its OWN code'
		# -------------------------------------------------------------------

		Set-ShimFixture -DockerFails
		$unreachable = Invoke-AssertScript -Channel Subprocess

		_Assert -Name 'a container that cannot be interrogated exits with its own code' `
			-Condition ($unreachable.ExitCode -eq $ExitContainerNotInterrogable) `
			-Detail ("expected $ExitContainerNotInterrogable, observed $($unreachable.ExitCode): $($unreachable.Output)")

		Set-ShimFixture
		$unreadable = Invoke-AssertScript -Channel Subprocess -Argument @{ ExpectedCheckout = $script:BareCheckout }

		_Assert -Name 'an unreadable expected configuration exits with its own code' `
			-Condition ($unreadable.ExitCode -eq $ExitExpectedConfigurationUnreadable) `
			-Detail ("expected $ExitExpectedConfigurationUnreadable, observed $($unreadable.ExitCode): $($unreadable.Output)")

		$observed = @(
			$healthyInProcess.ExitCode
			$refusedInProcess.ExitCode
			$unreachable.ExitCode
			$unreadable.ExitCode
		)
		_Assert -Name 'the four terminal outcomes are four DISTINCT codes' `
			-Condition ((($observed | Sort-Object -Unique) | Measure-Object).Count -eq 4) `
			-Detail ("observed: $($observed -join ', ')")

		# -------------------------------------------------------------------
		_Section 'Structural - a later-appended probe cannot reintroduce the leak'
		# -------------------------------------------------------------------

		# Behavioural arms above prove the script is correct TODAY. They cannot
		# prove it stays correct when the next probe is appended, because such a
		# probe would run after the last one they observe. Stronger than that,
		# and measured rather than assumed: the fix's two halves each suffice on
		# their own for a healthy run, so reverting EITHER leaves every arm above
		# green. These are the only detectors of a single-half regression. See
		# the perturbation table in .NOTES before deleting them as redundant.
		$source = @(Get-Content -LiteralPath $script:AssertScript)

		# Comment lines are excluded before adjudication, because the production
		# script DISCUSSES `& git` in the block that explains why the funnel
		# exists. Counting prose as an invocation would make this arm fail on a
		# correct script, which is the one outcome a structural guard must never
		# produce.
		$code = @($source | Where-Object { $_.TrimStart() -notmatch '^#' })
		$gitInvocations = @($code | Where-Object { $_ -match '&\s+git\b' })
		$funnelled = @($gitInvocations | Where-Object { $_ -match '&\s+git\s+@GitArgument\b' })

		_Assert -Name 'structural: control - the scan actually finds git invocations to adjudicate' `
			-Condition ($gitInvocations.Count -gt 0) `
			-Detail ("found $($gitInvocations.Count); a scan matching nothing would pass the next assertion vacuously")

		_Assert -Name 'structural: EVERY git invocation is funnelled through Invoke-GitProbe' `
			-Condition ($gitInvocations.Count -eq $funnelled.Count) `
			-Detail ("of $($gitInvocations.Count) git invocations, $($funnelled.Count) are the funnel's own; these bypass it and leak their exit code: " + (($gitInvocations | Where-Object { $_ -notmatch '&\s+git\s+@GitArgument\b' }) -join ' | '))

		# Read from the funnel's own body rather than the whole file, so a
		# `finally` belonging to some other function cannot satisfy this.
		$helperStart = [array]::FindIndex($source, [Predicate[string]] { param($line) $line -match '^function Invoke-GitProbe\b' })
		$helperEnd = -1
		if ($helperStart -ge 0) {
			for ($i = $helperStart + 1; $i -lt $source.Count; $i++) {
				if ($source[$i] -eq '}') { $helperEnd = $i; break }
			}
		}
		$helperBody = if ($helperEnd -gt $helperStart) { @($source[$helperStart..$helperEnd]) } else { @() }
		$finallyIndex = [array]::FindIndex($helperBody, [Predicate[string]] { param($line) $line.Trim() -eq 'finally {' })
		$clearIndex = [array]::FindIndex($helperBody, [Predicate[string]] { param($line) $line.Trim() -eq '$global:LASTEXITCODE = 0' })

		_Assert -Name 'structural: control - the funnel body was located and is not empty' `
			-Condition ($helperBody.Count -gt 5) `
			-Detail ("extracted $($helperBody.Count) lines from index $helperStart to $helperEnd")

		_Assert -Name 'structural: the funnel clears $LASTEXITCODE in a finally, so no path can skip it' `
			-Condition (($finallyIndex -ge 0) -and ($clearIndex -gt $finallyIndex)) `
			-Detail ("finally at $finallyIndex, clear at $clearIndex within the funnel body; the clear must sit INSIDE the finally or a throwing probe escapes it")

		_Assert -Name 'structural: the success path ends in an explicit exit rather than a leftover status' `
			-Condition ($source[-1].Trim() -eq 'exit $ExitAllChecksAgree') `
			-Detail ("last line is [$($source[-1])]")
	}
	finally {
		$env:PATH = $savedPath
		$env:PROVENANCE_SHIM_DIR = $savedShimDirectory
	}
}
finally {
	# Best effort. A git admin directory may hold a handle; the OS temp sweep is
	# the backstop.
	try {
		if (Test-Path -LiteralPath $script:Sandbox) {
			Remove-Item -Recurse -Force -LiteralPath $script:Sandbox -ErrorAction SilentlyContinue
		}
	}
	catch { }
}

# ---------------------------------------------------------------------------
Write-Host ''
Write-Host ('  Total {0}   Passed {1}   Failed {2}' -f ($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount)
Write-Host ''

exit $script:_FailCount
