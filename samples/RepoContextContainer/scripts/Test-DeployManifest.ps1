<#
.SYNOPSIS
	Demonstrates that the deploy manifest's declared/effective divergence check
	both FIRES and CAN BE SEEN NOT TO HAVE FIRED.

.DESCRIPTION
	This harness is cited by _deployManifest.ps1 and Assert-DeployManifest.ps1
	as the thing that demonstrates the refusing direction without a daemon. Both
	citations predate the file. That is worth stating plainly rather than
	quietly fixing, because a docstring asserting a guarantee that has no
	implementation is the same shape as #2983 itself: a claim of coverage with
	nothing behind it, indistinguishable in the reading from a claim with
	something behind it.

	WHAT #2983 WAS. Assert-DeployManifest.ps1 never called `docker compose
	config`. Its declared half was empty on every real invocation, so
	Get-DeployManifestDivergence short-circuited, found nothing, and the script
	printed DEPLOY MANIFEST OK. Thirteen deployments. The refusal path HAD been
	perturbation-tested - but only through -DeclaredReading, the synthetic
	entry point, so the tests proved a code path that operations never reached.
	A guard proven against synthetic input and deployed against real input it
	cannot read.

	Hence the two halves of this file, and the reason the second exists at all:

	  PURE            the library's behaviour, driven directly. Fast, daemon-
	                  free, and deterministic. This is where the tri-state, the
	                  rendering, the round-trip and the divergence gating are
	                  asserted.
	  ACQUISITION     the REAL path - Assert-DeployManifest.ps1 resolving actual
	                  compose files off disk. Synthetic coverage of the pure
	                  half is exactly what failed to catch #2983, so a test that
	                  never executes the acquisition cannot close it.

	THE ACQUISITION HALF NEEDS NO RUNNING STACK. It needs `docker compose
	config`, which resolves files and does not start anything. Where even that
	is unavailable the section is SKIPPED and the skip is COUNTED and PRINTED
	as a skip - never as a pass. A harness that renders "could not check" as a
	green line is the defect it is testing for.

	POSITIVE CONTROLS THROUGHOUT. Every assertion that something is NOT reported
	is paired with one showing the same harness reports it when it is there.
	An absence observed by an instrument never shown to detect the presence is
	not evidence.

.NOTES
	MUTATION TESTING. To confirm this harness can fail, neuter one function in
	_deployManifest.ps1 - return $true unconditionally from the status check in
	Get-DeployManifestDivergence, say - and re-run. The failures must be
	CONFINED to the assertions that name that behaviour, and the TOTAL must be
	IDENTICAL across both arms. The identical Total is the denominator check: it
	is what proves the failing arm ran the same number of tests rather than
	aborting early into a smaller, greener population. Two counts without that
	equality are just two numbers.

.EXAMPLE
	./scripts/Test-DeployManifest.ps1

.EXAMPLE
	./scripts/Test-DeployManifest.ps1 -SkipAcquisition
#>
[CmdletBinding()]
param(
	[switch] $SkipAcquisition
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_deployManifest.ps1')

$script:_PassCount = 0
$script:_FailCount = 0
$script:_SkipCount = 0

<#
	The literal com.docker.compose.project.config_files label read from the live
	rig on 2026-09-14 by

	  docker inspect repocontextcontainer-repocontext-1 --format
	    '{{ index .Config.Labels "com.docker.compose.project.config_files" }}'

	Kept verbatim, absolute deploy-checkout paths and all, because the point of
	#2993 is that the check works on what docker ACTUALLY writes rather than on a
	tidied approximation of it. Defined ONCE and shared by the pure section and
	the end-to-end section, so the two cannot drift into agreeing with each other
	while both disagree with the rig.

	Note the paths name the DEPLOY checkout (bucket4-merge), which is not the
	checkout these tests run from. That divergence is real, permanent, and must
	not be read as a mismatch - which is itself one of the assertions below.
#>
$script:RealRigConfigFilesLabel = 'C:\dev\copilot-worktrees\lattice\bucket4-merge\samples\RepoContextContainer\docker-compose.yml,C:\dev\copilot-worktrees\lattice\bucket4-merge\samples\RepoContextContainer\docker-compose.tuning.yml'

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

<#
.SYNOPSIS
	Records a check that could not be performed, as a SKIP and never as a PASS.

.DESCRIPTION
	Counted separately and printed in the totals so the denominator is visible.
	The alternative - omitting the check silently - makes a run with no daemon
	print the same green summary as a run with one, which is the exact
	substitution this epic keeps finding.
#>
function _Skip {
	param(
		[Parameter(Mandatory)] [string] $Name,
		[Parameter(Mandatory)] [string] $Why
	)
	$script:_SkipCount++
	Write-Host ("  SKIP  {0}  ({1})" -f $Name, $Why) -ForegroundColor Yellow
}

function _Section {
	param([Parameter(Mandatory)] [string] $Name)
	Write-Host ''
	Write-Host $Name -ForegroundColor Cyan
}

<#
	Normalises a value returned by a `return ,$array` function into a plain array.

	Borrowed verbatim in intent from Test-TuningIntegrity.ps1 beside this file,
	which documents the trap as having "caused a real defect in this rig twice".
	This harness made it three: the assertions that an agreeing pair and a
	suppressed check report NOTHING both failed on first run, because
	`@(Get-DeployManifestDivergence ...)` collects the single emitted object into
	a one-element array whose element is the empty array - Count 1 for a clean
	result. The library is right and the `,` is deliberate; wrapping it inline is
	what is wrong.

	Worth recording that the failure direction is the dangerous one AGAIN: it
	breaks the PASSING path and leaves every refusal working, so the positive
	control passed while three negative assertions failed. Had this harness
	tested only that divergences are reported, it would have been green.

	The implementation is the sibling's verbatim, including the `,` on its OWN
	returns - without which `return @($Value)` unrolls a one-element result back
	to a scalar and the helper reintroduces the trap it exists to remove. That
	was this file's second encounter with it in one sitting.
#>
function _Flatten {
	param([Parameter(ValueFromPipeline = $false)] $Value)

	if ($null -eq $Value) { return ,@() }
	return ,@($Value)
}

# A reading in which every tracked key agrees with its declaration. Built from
# the attribution table rather than hand-listed, so a variable added there is
# covered here without anybody remembering to add it - the hand-authored-list
# failure this epic has already paid for once.
function _AgreeingPair {
	$declared = @{}
	$effective = @{}

	foreach ($record in @(Get-AttributionVariable) + @(Get-AttributionGrant)) {
		$declared[$record.Name] = 'same'
		$effective[$record.Name] = 'same'
	}

	return @{ Declared = $declared; Effective = $effective }
}

Write-Host ''
Write-Host 'TEST-DEPLOYMANIFEST' -ForegroundColor White
Write-Host '-------------------'

# ---------------------------------------------------------------------------
_Section 'OVERLAY POLICY (the #2983 second defect, assertable with no daemon)'
# ---------------------------------------------------------------------------

$composeFiles = @(Get-DeployComposeFile)

_Assert -Name 'compose file list is not empty' `
	-Condition ($composeFiles.Count -gt 0)

_Assert -Name 'base compose file is resolved first' `
	-Condition ($composeFiles[0] -eq 'docker-compose.yml') `
	-Detail "got '$($composeFiles[0])'"

# The whole point. A bare `docker compose config` reads only docker-compose.yml
# and docker-compose.override.yml, and every knob this manifest tracks is set by
# the tuning overlay - so a fix that resolved compose WITHOUT it would report
# <absent> for all of them while now claiming to have looked, which is strictly
# worse than not looking.
_Assert -Name 'tuning overlay is in the resolution list' `
	-Condition ($composeFiles -contains 'docker-compose.tuning.yml') `
	-Detail "got '$($composeFiles -join ', ')'"

# ---------------------------------------------------------------------------
_Section 'DECLARATION TRI-STATE (absent, unreadable and not-resolved differ)'
# ---------------------------------------------------------------------------

$pair = _AgreeingPair

$available = New-DeployManifest -Declared $pair.Declared -Effective $pair.Effective `
	-CgroupCpuQuota $null -Label 'available' -DeclarationStatus 'Available' -DeclarationReason 'test'

$unreadable = New-DeployManifest -Declared @{} -Effective $pair.Effective `
	-CgroupCpuQuota $null -Label 'unreadable' -DeclarationStatus 'Unreadable' -DeclarationReason 'compose exited 1'

$notAttempted = New-DeployManifest -Declared @{} -Effective $pair.Effective `
	-CgroupCpuQuota $null -Label 'not-attempted' -DeclarationStatus 'NotAttempted' -DeclarationReason ''

_Assert -Name 'Available status is carried' `
	-Condition ($available.DeclarationStatus -eq 'Available')

_Assert -Name 'Unreadable status is carried' `
	-Condition ($unreadable.DeclarationStatus -eq 'Unreadable')

_Assert -Name 'NotAttempted status is carried' `
	-Condition ($notAttempted.DeclarationStatus -eq 'NotAttempted')

_Assert -Name 'DeclarationAvailable is derived from the status, not from emptiness' `
	-Condition ($available.DeclarationAvailable -and -not $unreadable.DeclarationAvailable -and -not $notAttempted.DeclarationAvailable)

# An omitted status must never infer Available. A caller that forgot to say
# cannot be allowed to license the divergence check against nothing, which is
# the failure mode of inferring from Keys.Count.
$inferred = New-DeployManifest -Declared @{} -Effective $pair.Effective `
	-CgroupCpuQuota $null -Label 'inferred'

_Assert -Name 'an omitted status over an empty reading infers NotAttempted, never Available' `
	-Condition ($inferred.DeclarationStatus -eq 'NotAttempted') `
	-Detail "got '$($inferred.DeclarationStatus)'"

# ---------------------------------------------------------------------------
_Section 'RENDERING (the tokens must not be the same token)'
# ---------------------------------------------------------------------------

$emptyDeclared = @{}
$someEffective = @{ 'DOTNET_GCHeapCount' = '6' }

$renderedAvailable = Format-DeployManifest -GeneratedAt 'now' -Manifest (
	New-DeployManifest -Declared $emptyDeclared -Effective $someEffective -CgroupCpuQuota $null `
		-Label 'r1' -DeclarationStatus 'Available' -DeclarationReason 'resolved')

$renderedUnreadable = Format-DeployManifest -GeneratedAt 'now' -Manifest (
	New-DeployManifest -Declared $emptyDeclared -Effective $someEffective -CgroupCpuQuota $null `
		-Label 'r2' -DeclarationStatus 'Unreadable' -DeclarationReason 'no .env')

$renderedNotAttempted = Format-DeployManifest -GeneratedAt 'now' -Manifest (
	New-DeployManifest -Declared $emptyDeclared -Effective $someEffective -CgroupCpuQuota $null `
		-Label 'r3' -DeclarationStatus 'NotAttempted' -DeclarationReason '')

_Assert -Name 'a resolved declaration renders a genuinely undeclared key as <absent>' `
	-Condition ($renderedAvailable -match 'DOTNET_gcServer\|declared=<absent>')

_Assert -Name 'an unreadable declaration renders <unreadable>, not <absent>' `
	-Condition (($renderedUnreadable -match 'DOTNET_gcServer\|declared=<unreadable>') -and
		($renderedUnreadable -notmatch 'declared=<absent>')) `
	-Detail 'the two claims must not share a token'

_Assert -Name 'an unattempted declaration renders <not-resolved>' `
	-Condition ($renderedNotAttempted -match 'DOTNET_gcServer\|declared=<not-resolved>')

_Assert -Name 'the status is recorded IN the manifest file' `
	-Condition (($renderedUnreadable -match 'DECLARATION_STATUS=Unreadable') -and
		($renderedUnreadable -match 'DECLARATION_REASON=no \.env'))

# The effective half keeps <absent> under every status. Only the DECLARED half
# is affected by whether a declaration was read.
_Assert -Name 'the effective column is unaffected by declaration status' `
	-Condition ($renderedUnreadable -match 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS\|declared=<unreadable>\|effective=<absent>')

# ---------------------------------------------------------------------------
_Section 'ROUND TRIP (a baseline must not lose its status)'
# ---------------------------------------------------------------------------

$readBack = Read-DeployManifest -Text $renderedUnreadable

_Assert -Name 'status survives a write/read round trip' `
	-Condition ($readBack.DeclarationStatus -eq 'Unreadable') `
	-Detail "got '$($readBack.DeclarationStatus)'"

_Assert -Name 'reason survives a write/read round trip' `
	-Condition ($readBack.DeclarationReason -eq 'no .env') `
	-Detail "got '$($readBack.DeclarationReason)'"

$readBackAvailable = Read-DeployManifest -Text $renderedAvailable

_Assert -Name 'an Available manifest reads back as Available' `
	-Condition ($readBackAvailable.DeclarationStatus -eq 'Available')

_Assert -Name 'every placeholder reads back as a null value, not as its own text' `
	-Condition ($null -eq (@($readBack.Records | Where-Object { $_.Name -eq 'DOTNET_gcServer' })[0].Declared))

# A manifest written before #2983 has no status line. Reading it as Available
# would retroactively promote its <absent> columns into positive findings they
# never were.
$legacy = @'
# DEPLOY MANIFEST
# label        : legacy
# generated at : then

PROCESSOR_COUNT_RESOLVED=6
PROCESSOR_COUNT_SOURCE=CgroupDerivation

DOTNET_GCHeapCount|declared=<absent>|effective=6
'@

$legacyRead = Read-DeployManifest -Text $legacy

_Assert -Name 'a pre-#2983 manifest reads back as NotAttempted, not Available' `
	-Condition ($legacyRead.DeclarationStatus -eq 'NotAttempted') `
	-Detail "got '$($legacyRead.DeclarationStatus)'"

# ---------------------------------------------------------------------------
_Section 'DIVERGENCE GATING (and the positive control that proves it can fire)'
# ---------------------------------------------------------------------------

# POSITIVE CONTROL FIRST. Everything below asserts that divergences are NOT
# reported under some condition; none of it means anything unless this same
# harness, on this same code, reports one when it is there.
$divergentDeclared = $pair.Declared.Clone()
$divergentEffective = $pair.Effective.Clone()
$divergentDeclared['DOTNET_GCHeapCount'] = '6'
$divergentEffective['DOTNET_GCHeapCount'] = '12'

$divergent = New-DeployManifest -Declared $divergentDeclared -Effective $divergentEffective `
	-CgroupCpuQuota $null -Label 'divergent' -DeclarationStatus 'Available' -DeclarationReason 'resolved'

$found = _Flatten (Get-DeployManifestDivergence -Manifest $divergent)

_Assert -Name 'POSITIVE CONTROL: a genuine divergence IS reported' `
	-Condition ($found.Count -eq 1) `
	-Detail "expected 1, got $($found.Count)"

_Assert -Name 'POSITIVE CONTROL: the finding names the variable and both values' `
	-Condition ($found.Count -eq 1 -and $found[0] -match 'DOTNET_GCHeapCount' -and $found[0] -match "'6'" -and $found[0] -match "'12'") `
	-Detail ($found -join ' | ')

_Assert -Name 'an agreeing pair reports nothing' `
	-Condition ((_Flatten (Get-DeployManifestDivergence -Manifest $available)).Count -eq 0)

_Assert -Name 'an Unreadable declaration suppresses the check' `
	-Condition ((_Flatten (Get-DeployManifestDivergence -Manifest $unreadable)).Count -eq 0)

_Assert -Name 'a NotAttempted declaration suppresses the check' `
	-Condition ((_Flatten (Get-DeployManifestDivergence -Manifest $notAttempted)).Count -eq 0)

# ---------------------------------------------------------------------------
_Section 'SUPPRESSION IS ANNOUNCED (zero findings vs no findings possible)'
# ---------------------------------------------------------------------------

_Assert -Name 'a check that ran announces nothing' `
	-Condition ([string]::IsNullOrWhiteSpace((Get-DeclarationSuppression -Manifest $available)))

$unreadableNotice = Get-DeclarationSuppression -Manifest $unreadable

_Assert -Name 'an unreadable declaration announces that the check DID NOT RUN' `
	-Condition ($unreadableNotice -match 'DID NOT RUN') `
	-Detail $unreadableNotice

_Assert -Name 'the announcement carries the reason' `
	-Condition ($unreadableNotice -match 'compose exited 1') `
	-Detail $unreadableNotice

_Assert -Name 'an unattempted declaration announces that the check DID NOT RUN' `
	-Condition ((Get-DeclarationSuppression -Manifest $notAttempted) -match 'DID NOT RUN')

# The two suppressed states must be distinguishable in the announcement too,
# for the same reason the tokens are: they license different conclusions.
_Assert -Name 'the two suppressed states announce themselves differently' `
	-Condition ((Get-DeclarationSuppression -Manifest $unreadable) -ne (Get-DeclarationSuppression -Manifest $notAttempted))

# ---------------------------------------------------------------------------
_Section 'SERVICE SCOPING (a regression guard, found by running the real path)'
# ---------------------------------------------------------------------------

# This repository declares DOTNET_gcServer as '1' on repocontext and '0' on
# embedder. The first live run of the #2983 fix merged both containers'
# environments flat, the embedder's 0 overwrote repocontext's 1, and the script
# reported a DECLARED/EFFECTIVE DIVERGENCE on a correct deployment. The manifest
# keys records by bare variable NAME, so nothing in the key says which container
# a value must come from - only the attribution table's Service does.
$services = @(@(Get-AttributionVariable) + @(Get-AttributionGrant) | ForEach-Object { $_.Service } | Sort-Object -Unique)

_Assert -Name 'more than one service is tracked, so scoping is load-bearing' `
	-Condition ($services.Count -gt 1) `
	-Detail "services: $($services -join ', ')"

_Assert -Name 'every tracked record names a service' `
	-Condition (@(@(Get-AttributionVariable) + @(Get-AttributionGrant) | Where-Object { [string]::IsNullOrWhiteSpace($_.Service) }).Count -eq 0)

_Assert -Name 'the embedder owns at least one tracked record' `
	-Condition (@(@(Get-AttributionVariable) + @(Get-AttributionGrant) | Where-Object { $_.Service -eq 'embedder' }).Count -gt 0)

# ---------------------------------------------------------------------------
_Section 'REAL ACQUISITION (the path #2983 proved synthetic tests cannot cover)'
# ---------------------------------------------------------------------------

$composeDirectory = Split-Path -Parent $here
$assert = Join-Path $here 'Assert-DeployManifest.ps1'
$dockerUsable = $false

if ($SkipAcquisition) {
	_Skip -Name 'real compose resolution' -Why '-SkipAcquisition was passed'
}
else {
	try {
		& docker compose version 2>&1 | Out-Null
		$dockerUsable = ($LASTEXITCODE -eq 0)
	}
	catch {
		$dockerUsable = $false
	}

	if (-not $dockerUsable) {
		# Counted as a SKIP, never as a PASS. A harness that renders "could not
		# check" as a green line is the defect under test.
		_Skip -Name 'real compose resolution (Unreadable arm)' -Why 'docker compose is not available on this host'
		_Skip -Name 'real compose resolution (Available arm)' -Why 'docker compose is not available on this host'

		# #2993's end-to-end arms live inside the same `if ($dockerUsable)` block,
		# so without these they would simply not run and the denominator would
		# drop by four with nothing anywhere saying so. An unrun check that leaves
		# no trace is the exact substitution this suite exists to refuse, and it
		# does not stop being that when the unrun check is one of mine.
		_Skip -Name 'compose set on the real path (agreeing arm)' -Why 'docker compose is not available on this host'
		_Skip -Name 'compose set on the real path (divergent arm)' -Why 'docker compose is not available on this host'
		_Skip -Name 'compose set on the real path (reordered arm)' -Why 'docker compose is not available on this host'
		_Skip -Name 'compose set on the real path (absent-label arm)' -Why 'docker compose is not available on this host'
	}
}

if ($dockerUsable) {
	$scratch = Join-Path ([IO.Path]::GetTempPath()) ("deploymanifest-" + [Guid]::NewGuid().ToString('n'))
	New-Item -ItemType Directory -Path $scratch -Force | Out-Null

	try {
		# The effective half is supplied, so NO RUNNING STACK is needed - only
		# `docker compose config`, which resolves files and starts nothing. What
		# is exercised is acquisition of the DECLARED half, the path that had no
		# coverage at all and where #2983 lived.
		#
		# These values AGREE with the .env written below, deliberately. A filler
		# reading would diverge from every key, the clean arm would refuse, and
		# the divergence control at the end could not fail - it would be asserting
		# exit 2 against a run that exits 2 regardless of the skew. That is the
		# unfalsifiable-guard shape this epic exists to find, and the first draft
		# of this section had it.
		$effective = @{
			'LATTICE_WAL_MAX_CONCURRENT_REPLAYS' = '0'
			'DOTNET_GCHeapCount'                 = '6'
			'DOTNET_gcServer'                    = '1'
			'EMBED_INTRA_THREADS'                = '4'
			'repocontext.cpus'                   = '6'
			'repocontext.mem_limit'              = '12288m'
			'embedder.cpus'                      = '4'
			'embedder.mem_limit'                 = '5120m'
		}

		# ARM 1 - THE FAILURE PATH, ON REAL INPUT.
		# A checkout with no .env cannot resolve the :?-guarded variables. The
		# requirement is not that this succeeds; it is that the failure is
		# recorded as Unreadable WITH a reason, and never as a row of <absent>
		# that reads like a positive finding. This is the arm that would have
		# caught #2983: before the fix this path reported the same empty
		# declaration whether compose was readable or not.
		$noEnvDirectory = Join-Path $scratch 'no-env'
		New-Item -ItemType Directory -Path $noEnvDirectory -Force | Out-Null

		foreach ($file in Get-DeployComposeFile) {
			Copy-Item -LiteralPath (Join-Path $composeDirectory $file) -Destination $noEnvDirectory
		}

		$unreadablePath = Join-Path $scratch 'unreadable.manifest'

		& $assert -ComposeDirectory $noEnvDirectory -ManifestPath $unreadablePath `
			-Label 'unreadable-probe' -EffectiveReading $effective -CgroupCpuQuota 6 -Quiet 2>&1 | Out-Null

		if (Test-Path -LiteralPath $unreadablePath) {
			$unreadableRun = Read-DeployManifest -Text ([IO.File]::ReadAllText($unreadablePath))

			_Assert -Name 'an unresolvable compose set is recorded Unreadable, not absent' `
				-Condition ($unreadableRun.DeclarationStatus -eq 'Unreadable') `
				-Detail "got '$($unreadableRun.DeclarationStatus)'"

			_Assert -Name 'the Unreadable manifest carries a reason naming the cause' `
				-Condition (-not [string]::IsNullOrWhiteSpace($unreadableRun.DeclarationReason)) `
				-Detail $unreadableRun.DeclarationReason

			_Assert -Name 'the Unreadable manifest renders <unreadable>, never <absent>' `
				-Condition (([IO.File]::ReadAllText($unreadablePath)) -notmatch 'declared=<absent>') `
				-Detail 'a row of <absent> would assert the keys are undeclared, which is not known'
		}
		else {
			_Assert -Name 'an unresolvable compose set is recorded Unreadable, not absent' -Condition $false -Detail 'no manifest written'
			_Assert -Name 'the Unreadable manifest carries a reason naming the cause' -Condition $false -Detail 'no manifest written'
			_Assert -Name 'the Unreadable manifest renders <unreadable>, never <absent>' -Condition $false -Detail 'no manifest written'
		}

		# ARM 2 - THE SUCCESS PATH, ON REAL INPUT.
		# Same real compose files, plus a .env, resolved for real. Without this
		# arm the Unreadable arm above proves nothing: an acquisition that ALWAYS
		# fails would satisfy it. This is the positive control for the section.
		$envDirectory = Join-Path $scratch 'with-env'
		New-Item -ItemType Directory -Path $envDirectory -Force | Out-Null

		foreach ($file in Get-DeployComposeFile) {
			Copy-Item -LiteralPath (Join-Path $composeDirectory $file) -Destination $envDirectory
		}

		$corpus = Join-Path $scratch 'corpus'
		New-Item -ItemType Directory -Path $corpus -Force | Out-Null

		# Explicit values rather than .env.example's, so the expected declarations
		# are fixed by this file and cannot drift with the example.
		$envLines = @(
			"REPO_PATH=$corpus",
			"REPOCONTEXT_MEMORY_ARCHIVE_PATH=$corpus",
			"REPOCONTEXT_BACKUP_PATH=$corpus",
			'REPOCONTEXT_PORT=5000',
			'REPOCONTEXT_BACKUP_SINK_PORT=10000',
			'REPOCONTEXT_CPUS=6',
			'REPOCONTEXT_CPUSET=0-5',
			'REPOCONTEXT_MEM_LIMIT=12884901888',
			'REPOCONTEXT_GC_HEAP_COUNT=6',
			'REPOCONTEXT_MAX_CONCURRENT_REPLAYS=0',
			'EMBEDDER_CPUS=4',
			'EMBEDDER_CPUSET=6-9',
			'EMBEDDER_MEM_LIMIT=5368709120',
			'EMBEDDER_INTRA_THREADS=4',
			'GIT_COMMIT=0000000000000000000000000000000000000000'
		)

		[IO.File]::WriteAllText((Join-Path $envDirectory '.env'), ($envLines -join "`n") + "`n")

		$acquiredPath = Join-Path $scratch 'acquired.manifest'

		& $assert -ComposeDirectory $envDirectory -ManifestPath $acquiredPath `
			-Label 'acquisition-probe' -EffectiveReading $effective -CgroupCpuQuota 6 -Quiet 2>&1 | Out-Null

		$cleanExit = $LASTEXITCODE

		$wrote = Test-Path -LiteralPath $acquiredPath

		_Assert -Name 'the real path writes a manifest' -Condition $wrote

		if ($wrote) {
			$acquired = Read-DeployManifest -Text ([IO.File]::ReadAllText($acquiredPath))

			# THE ASSERTION #2983 IS ABOUT. Before the fix this was NotAttempted on
			# every real invocation, and every declared value was <absent>.
			_Assert -Name 'the real path RESOLVES the declaration (status is Available)' `
				-Condition ($acquired.DeclarationStatus -eq 'Available') `
				-Detail "got '$($acquired.DeclarationStatus)' - reason: $($acquired.DeclarationReason)"

			$populated = @($acquired.Records | Where-Object { $null -ne $_.Declared })

			_Assert -Name 'the declared half is POPULATED from real compose files' `
				-Condition ($populated.Count -gt 0) `
				-Detail "declared values found: $($populated.Count) of $(@($acquired.Records).Count)"

			# Named explicitly, because it is set by the overlay and NOT by the base
			# compose file. Its presence is the executable proof that the overlay was
			# in the resolution - a bare `docker compose config` yields nothing here.
			$replay = @($acquired.Records | Where-Object { $_.Name -eq 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS' })

			_Assert -Name 'an overlay-only knob is declared, proving the overlay resolved' `
				-Condition ($replay.Count -eq 1 -and $null -ne $replay[0].Declared) `
				-Detail 'a bare `docker compose config` yields nothing for this key'

			# Per service, not key-only. DOTNET_gcServer is declared 1 on
			# repocontext and 0 on embedder, so a key-only scan can read either.
			$gcServer = @($acquired.Records | Where-Object { $_.Name -eq 'DOTNET_gcServer' })

			_Assert -Name "a key declared differently per service takes its OWN service's value" `
				-Condition ($gcServer.Count -eq 1 -and $gcServer[0].Declared -eq '1') `
				-Detail "repocontext declares 1, embedder declares 0; got '$(if ($gcServer.Count -eq 1) { $gcServer[0].Declared } else { '<missing>' })'"

			# Grants are normalised into a shared form on both halves. Compose says
			# 12884901888 bytes and docker reports 12884901888 bytes; without a
			# normal form the manifest would compare "12884901888" against "12288m"
			# and report a divergence that is purely a difference of units.
			$mem = @($acquired.Records | Where-Object { $_.Name -eq 'repocontext.mem_limit' })

			_Assert -Name 'a declared memory grant is normalised to the effective form' `
				-Condition ($mem.Count -eq 1 -and $mem[0].Declared -eq '12288m') `
				-Detail "got '$(if ($mem.Count -eq 1) { $mem[0].Declared } else { '<missing>' })', expected '12288m'"

			_Assert -Name 'the reason names the files that were resolved' `
				-Condition ($acquired.DeclarationReason -match 'docker-compose\.tuning\.yml') `
				-Detail $acquired.DeclarationReason
		}
		else {
			foreach ($name in @(
				'the real path RESOLVES the declaration (status is Available)',
				'the declared half is POPULATED from real compose files',
				'an overlay-only knob is declared, proving the overlay resolved',
				"a key declared differently per service takes its OWN service's value",
				'a declared memory grant is normalised to the effective form',
				'the reason names the files that were resolved')) {
				_Assert -Name $name -Condition $false -Detail 'no manifest written'
			}
		}

		# POSITIVE CONTROL ON THE REAL PATH. The acquisition above is only
		# meaningful if this same real path REFUSES when the halves disagree. A
		# declared half that populates but never disagrees would be
		# indistinguishable from one that is ignored.
		$skewed = $effective.Clone()
		$skewed['LATTICE_WAL_MAX_CONCURRENT_REPLAYS'] = 'deliberately-not-the-declared-value'
		$skewedPath = Join-Path $scratch 'skewed.manifest'

		& $assert -ComposeDirectory $envDirectory -ManifestPath $skewedPath `
			-Label 'divergence-probe' -EffectiveReading $skewed -CgroupCpuQuota 6 -Quiet 2>&1 | Out-Null

		$skewedExit = $LASTEXITCODE

		_Assert -Name 'POSITIVE CONTROL: the real path REFUSES a genuine divergence (exit 2)' `
			-Condition ($skewedExit -eq 2) `
			-Detail "got exit $skewedExit"

		# THE PAIR IS WHAT MAKES EITHER ARM MEAN ANYTHING. Exit 2 above proves
		# the check fires only if this proves it stays silent when the halves
		# agree. Without it, an acquisition that diverged on every key would
		# satisfy the refusal arm and look like a working guard.
		_Assert -Name 'the same real path ACCEPTS agreeing halves (exit 0)' `
			-Condition ($cleanExit -eq 0) `
			-Detail "got exit $cleanExit - the skew arm is unfalsifiable unless this is 0"

		# -------------------------------------------------------------------
		# #2993 ON THE REAL PATH. The pure section proves the VERDICT; these
		# prove the WIRING, which is a different claim. A correct verdict that
		# nothing acts on is the same silent no-op this epic keeps finding.
		#
		# Every arm below is the SAME invocation as the clean one above, differing
		# ONLY in the config_files label. Anything that moves is therefore caused
		# by the label and by nothing else.
		# -------------------------------------------------------------------

		# HEALTHY FIRST, and with the rig's real bytes. A guard that refuses a
		# correct deployment gets deleted, so this is the arm that must hold.
		$agreePath = Join-Path $scratch 'compose-agree.manifest'

		& $assert -ComposeDirectory $envDirectory -ManifestPath $agreePath `
			-Label 'compose-agree' -EffectiveReading $effective -CgroupCpuQuota 6 `
			-ComposeConfigFilesLabel $script:RealRigConfigFilesLabel -Quiet 2>&1 | Out-Null

		$agreeExit = $LASTEXITCODE

		_Assert -Name 'REAL PATH: the live rig label is ACCEPTED (exit 0)' `
			-Condition ($agreeExit -eq 0) `
			-Detail "got exit $agreeExit - the refusal arms below are unfalsifiable unless this is 0"

		_Assert -Name 'REAL PATH: an agreeing label still WRITES the manifest' `
			-Condition (Test-Path -LiteralPath $agreePath) `
			-Detail 'the check must not cost a reading when nothing is wrong'

		if (Test-Path -LiteralPath $agreePath) {
			$agreed = Read-DeployManifest -Text ([IO.File]::ReadAllText($agreePath))

			_Assert -Name 'REAL PATH: the agreeing manifest records the set as CONFIRMED' `
				-Condition ($agreed.DeclarationReason -match 'confirmed against the container') `
				-Detail $agreed.DeclarationReason

			_Assert -Name 'REAL PATH: a confirmed set is still DeclarationStatus Available' `
				-Condition ($agreed.DeclarationStatus -eq 'Available') `
				-Detail "got '$($agreed.DeclarationStatus)'"
		}
		else {
			_Assert -Name 'REAL PATH: the agreeing manifest records the set as CONFIRMED' -Condition $false -Detail 'no manifest written'
			_Assert -Name 'REAL PATH: a confirmed set is still DeclarationStatus Available' -Condition $false -Detail 'no manifest written'
		}

		# REFUSAL. A deployment brought up without the tuning overlay.
		$divergePath = Join-Path $scratch 'compose-diverge.manifest'

		& $assert -ComposeDirectory $envDirectory -ManifestPath $divergePath `
			-Label 'compose-diverge' -EffectiveReading $effective -CgroupCpuQuota 6 `
			-ComposeConfigFilesLabel '/srv/app/docker-compose.yml' -Quiet 2>&1 | Out-Null

		$divergeExit = $LASTEXITCODE

		_Assert -Name 'REAL PATH: a divergent overlay set is REFUSED (exit 5)' `
			-Condition ($divergeExit -eq 5) `
			-Detail "got exit $divergeExit"

		# THE LOAD-BEARING ONE. The declared half resolved perfectly and would
		# have rendered indistinguishably from a correct manifest. Had it been
		# written it would have become a baseline - a reading attributed to a
		# deployment it never came from. This asserts the file does not exist.
		_Assert -Name 'REAL PATH: the refusal writes NO manifest, so no poisoned baseline exists' `
			-Condition (-not (Test-Path -LiteralPath $divergePath)) `
			-Detail 'a manifest written here would be scored as a measurement'

		# REORDERING, end to end. The set difference is empty, so the remedy the
		# issue specified would have written a manifest here.
		$reorderPath = Join-Path $scratch 'compose-reorder.manifest'

		& $assert -ComposeDirectory $envDirectory -ManifestPath $reorderPath `
			-Label 'compose-reorder' -EffectiveReading $effective -CgroupCpuQuota 6 `
			-ComposeConfigFilesLabel 'docker-compose.tuning.yml,docker-compose.yml' -Quiet 2>&1 | Out-Null

		$reorderExit = $LASTEXITCODE

		_Assert -Name 'REAL PATH: a REORDERED overlay set is refused too (exit 5)' `
			-Condition ($reorderExit -eq 5) `
			-Detail "got exit $reorderExit - a set-difference check would have written this manifest"

		_Assert -Name 'REAL PATH: the reordering refusal also writes NO manifest' `
			-Condition (-not (Test-Path -LiteralPath $reorderPath)) `
			-Detail 'overlay order changes which value wins, so this reading is not this deployment'

		# UNKNOWN, end to end. A container with no label must still be readable -
		# recorded as unverified, never refused and never silently confirmed.
		$unknownPath = Join-Path $scratch 'compose-unknown.manifest'

		& $assert -ComposeDirectory $envDirectory -ManifestPath $unknownPath `
			-Label 'compose-unknown' -EffectiveReading $effective -CgroupCpuQuota 6 `
			-ComposeConfigFilesLabel '' -Quiet 2>&1 | Out-Null

		$unknownExit = $LASTEXITCODE

		_Assert -Name 'REAL PATH: an ABSENT label does not refuse (exit 0)' `
			-Condition ($unknownExit -eq 0) `
			-Detail "got exit $unknownExit - a non-compose container is not a divergence"

		_Assert -Name 'REAL PATH: an absent label still writes the manifest' `
			-Condition (Test-Path -LiteralPath $unknownPath) `
			-Detail 'refusing here would break every deployment this check cannot see'

		if (Test-Path -LiteralPath $unknownPath) {
			$unknownManifest = Read-DeployManifest -Text ([IO.File]::ReadAllText($unknownPath))

			# Carried in DECLARATION_REASON, a field consumers ALREADY read, so a
			# consumer that never learns about the compose-set check still cannot
			# mistake this for a verified set. Amendment 25 applied at authoring.
			_Assert -Name 'REAL PATH: the unverified set is marked UNVERIFIED in the manifest' `
				-Condition ($unknownManifest.DeclarationReason -match 'UNVERIFIED') `
				-Detail $unknownManifest.DeclarationReason

			_Assert -Name 'REAL PATH: an unverified set is NOT recorded as confirmed' `
				-Condition ($unknownManifest.DeclarationReason -notmatch 'confirmed against the container') `
				-Detail 'silently claiming confirmation is the defect #2993 is about'
		}
		else {
			_Assert -Name 'REAL PATH: the unverified set is marked UNVERIFIED in the manifest' -Condition $false -Detail 'no manifest written'
			_Assert -Name 'REAL PATH: an unverified set is NOT recorded as confirmed' -Condition $false -Detail 'no manifest written'
		}

		# The four arms differ ONLY in the label, so their exit codes must not all
		# be equal. A wiring that ignored the label entirely would give 0/0/0/0 and
		# every individual assertion above would still need to fail for that to be
		# caught; this catches it in one.
		$composeExits = @($agreeExit, $divergeExit, $reorderExit, $unknownExit)

		_Assert -Name 'REAL PATH: the label ALONE changes the outcome (4 arms, 2 distinct codes)' `
			-Condition (@($composeExits).Count -eq 4 -and @($composeExits | Sort-Object -Unique).Count -eq 2) `
			-Detail "exits [$($composeExits -join ', ')] - identical codes would mean the label is not being read"
	}
	finally {
		Remove-Item -LiteralPath $scratch -Recurse -Force -ErrorAction SilentlyContinue
	}
}


# ---------------------------------------------------------------------------
_Section 'DECLARATION DENOMINATOR (the status is a verdict; the count is not)'
# ---------------------------------------------------------------------------

# WHY THIS SECTION EXISTS. The #2983 mutation test neutered the acquisition to
# return an empty hashtable, and the assertion on the STATUS passed: an empty
# reading is still a successful reading, so it reports Available over nine
# unresolved rows. Only the population assertion caught it. A manifest carrying
# the verdict without the denominator reproduces that blind spot in the artefact
# a run is scored from, where no test is watching at all.

$pair = _AgreeingPair
$recordTotal = @(@(Get-AttributionVariable) + @(Get-AttributionGrant)).Count

# All keys resolved.
$fullManifest = New-DeployManifest -Declared $pair.Declared -Effective $pair.Effective `
	-Label 'denominator-full' -CgroupCpuQuota 6 `
	-DeclarationStatus 'Available' -DeclarationReason 'test'
$fullText = Format-DeployManifest -Manifest $fullManifest

_Assert -Name 'an Available manifest RECORDS the resolved count' `
	-Condition ($fullText -match "DECLARATION_RESOLVED=$recordTotal of $recordTotal") `
	-Detail 'the count is missing, so a reader sees a verdict with no denominator'

# The M1 shape, made visible. Resolution ran and returned nothing.
$emptyManifest = New-DeployManifest -Declared @{} -Effective $pair.Effective `
	-Label 'denominator-empty' -CgroupCpuQuota 6 `
	-DeclarationStatus 'Available' -DeclarationReason 'test'
$emptyText = Format-DeployManifest -Manifest $emptyManifest

# THE DISCRIMINATING ARM. If the count were a constant, or derived from the
# record list rather than from what actually resolved, it would read the same
# here as above and detect nothing. The status is identical in both manifests -
# only this number distinguishes them.
_Assert -Name 'an EMPTY Available resolution reports 0, not the record count' `
	-Condition ($emptyText -match "DECLARATION_RESOLVED=0 of $recordTotal") `
	-Detail 'the count does not track what resolved, so it cannot catch an empty reading'

_Assert -Name 'both manifests carry the SAME status, so only the count separates them' `
	-Condition (($fullText -match 'DECLARATION_STATUS=Available') -and ($emptyText -match 'DECLARATION_STATUS=Available')) `
	-Detail 'if the statuses differ this pair proves nothing about the count'

# NotAttempted must NOT render `0 of N`. Zero-resolved-because-we-looked and
# zero-resolved-because-we-did-not are different claims, and #2966 is the
# precedent for refusing to spend one token on both.
$notAttempted = New-DeployManifest -Declared @{} -Effective $pair.Effective `
	-Label 'denominator-notattempted' -CgroupCpuQuota 6 `
	-DeclarationStatus 'NotAttempted' -DeclarationReason 'test'
$notAttemptedText = Format-DeployManifest -Manifest $notAttempted

_Assert -Name 'a NotAttempted manifest does NOT render a count' `
	-Condition ($notAttemptedText -notmatch 'DECLARATION_RESOLVED=\d') `
	-Detail 'a never-attempted resolution is reporting a measured zero'

_Assert -Name 'a NotAttempted manifest renders the not-resolved placeholder instead' `
	-Condition ($notAttemptedText -match 'DECLARATION_RESOLVED=<not-resolved>') `
	-Detail 'the placeholder discipline is not applied to the count line'

# Round-trip. The count has to survive being written and read back, or it is
# console decoration rather than something a later reader can cite.
$fullRead = Read-DeployManifest -Text $fullText
_Assert -Name 'the resolved count ROUND-TRIPS through a read' `
	-Condition ($fullRead.DeclarationResolvedCount -eq $recordTotal -and $fullRead.DeclarationRecordCount -eq $recordTotal) `
	-Detail "got $($fullRead.DeclarationResolvedCount) of $($fullRead.DeclarationRecordCount)"

$emptyRead = Read-DeployManifest -Text $emptyText
_Assert -Name 'a zero resolved count round-trips as 0, not as null' `
	-Condition ($null -ne $emptyRead.DeclarationResolvedCount -and $emptyRead.DeclarationResolvedCount -eq 0) `
	-Detail 'a measured zero is being lost, which is the finding this exists to preserve'

# THE TRI-STATE, ONE LAYER UP. A legacy manifest carries no count line at all,
# and must read back as "not measured" rather than as a measured zero - the same
# distinction the declared column already makes, applied to the denominator.
$legacyRead = Read-DeployManifest -Text $notAttemptedText
_Assert -Name 'an unmeasured count reads back as $null, NOT as 0' `
	-Condition ($null -eq $legacyRead.DeclarationResolvedCount) `
	-Detail 'an unmeasured baseline would be scored as a measured-and-empty one'


# ---------------------------------------------------------------------------
_Section 'MANIFEST VINTAGE (#2992 - a cross-vintage read reported as drift)'

function _Delta {
	param([string] $Name, [string] $Kind, [string] $Was = '', [string] $Now = '')
	return [pscustomobject]@{
		Name = $Name; Kind = $Kind; Was = $Was; Now = $Now; Attribution = 'test'
	}
}

_Assert -Name 'two equal vintages are Same' `
	-Condition ((Get-VintageRelation -BaselineVintage 1 -CurrentVintage 1) -eq 'Same')

_Assert -Name 'a lower baseline vintage is BaselineOlder' `
	-Condition ((Get-VintageRelation -BaselineVintage 1 -CurrentVintage 2) -eq 'BaselineOlder')

_Assert -Name 'a lower current vintage is CurrentOlder' `
	-Condition ((Get-VintageRelation -BaselineVintage 2 -CurrentVintage 1) -eq 'CurrentOlder')

# The case every pre-#2992 baseline is in. Unknown has to sort OLDER than any
# known vintage; sorting it as newest, or as equal, would make the legacy
# baselines this exists for the one population it cannot reason about.
_Assert -Name 'an UNKNOWN baseline vintage sorts as OLDER, not as equal' `
	-Condition ((Get-VintageRelation -BaselineVintage $null -CurrentVintage 1) -eq 'BaselineOlder')

_Assert -Name 'an UNKNOWN current vintage sorts as OLDER too' `
	-Condition ((Get-VintageRelation -BaselineVintage 1 -CurrentVintage $null) -eq 'CurrentOlder')

# Two unknowns cannot be ordered, and guessing an order would let a legacy pair
# excuse a delta that the pre-#2992 behaviour reported. Indeterminate explains
# nothing, so legacy-versus-legacy behaves EXACTLY as it did before this change.
_Assert -Name 'two unknown vintages are Indeterminate, not Same' `
	-Condition ((Get-VintageRelation -BaselineVintage $null -CurrentVintage $null) -eq 'Indeterminate')

# THE HEALTHY CASE, AND IT IS GRADED FIRST. Every baseline captured before
# #2992 is cross-vintage against every reading taken after it. If that alone
# refused, this guard would refuse its own installation on every rig, and a
# guard that cries wolf gets switched off rather than fixed.
$healthy = Get-ComparabilityVerdict -Deltas @() -Relation 'BaselineOlder'
_Assert -Name 'HEALTHY CASE: cross-vintage with NO explicable delta stays COMPARABLE' `
	-Condition ($healthy.Comparable -and $healthy.ExplainedCount -eq 0) `
	-Detail 'this would refuse every legacy baseline on every rig'

$healthyDrift = Get-ComparabilityVerdict `
	-Deltas @((_Delta -Name 'A' -Kind 'Changed' -Was '1' -Now '2')) -Relation 'BaselineOlder'
_Assert -Name 'HEALTHY CASE: real drift across vintages is still COMPARABLE and still reported' `
	-Condition ($healthyDrift.Comparable -and $healthyDrift.ExplainedCount -eq 0) `
	-Detail 'a vintage difference must not suppress a value both instruments could read'

# The #2992 shape itself: the newer instrument populates cells the older one
# left empty, and each reads as a pinned variable.
$pinned = Get-ComparabilityVerdict `
	-Deltas @(
		(_Delta -Name 'EMBED_INTRA_THREADS' -Kind 'Pinned' -Now '4'),
		(_Delta -Name 'embedder.cpus' -Kind 'Pinned' -Now '2')
	) `
	-Relation 'BaselineOlder'
_Assert -Name 'a newer instrument populating empty cells is NOT comparable' `
	-Condition (-not $pinned.Comparable -and $pinned.ExplainedCount -eq 2)

_Assert -Name 'the incomparability NAMES the keys rather than only counting them' `
	-Condition ($pinned.ExplainedNames -contains 'EMBED_INTRA_THREADS' -and $pinned.Summary -match 'embedder\.cpus')

# The mirror, which is not hypothetical: a deploy checkout that has not been
# updated runs the OLDER script and loses cells the baseline recorded.
$unpinned = Get-ComparabilityVerdict `
	-Deltas @((_Delta -Name 'EMBED_INTRA_THREADS' -Kind 'Unpinned' -Was '4')) `
	-Relation 'CurrentOlder'
_Assert -Name 'a STALE reading losing cells the baseline had is NOT comparable either' `
	-Condition (-not $unpinned.Comparable -and $unpinned.ExplainedCount -eq 1)

# Direction matters. A newer reader cannot explain a cell going EMPTY, so the
# excusal is not symmetric and must not be applied by kind alone.
_Assert -Name 'BaselineOlder does NOT excuse an Unpinned delta (wrong direction)' `
	-Condition ((Get-ComparabilityVerdict -Deltas @((_Delta -Name 'A' -Kind 'Unpinned' -Was '1')) -Relation 'BaselineOlder').Comparable)

_Assert -Name 'CurrentOlder does NOT excuse a Pinned delta (wrong direction)' `
	-Condition ((Get-ComparabilityVerdict -Deltas @((_Delta -Name 'A' -Kind 'Pinned' -Now '1')) -Relation 'CurrentOlder').Comparable)

# 'Changed' is the one kind no vintage difference can ever explain. Two
# instruments disagreeing about a value they can BOTH read is drift whatever
# wrote them, and excusing it would rebuild the silent-pass defect this fixes.
foreach ($rel in 'BaselineOlder', 'CurrentOlder', 'Indeterminate', 'Same') {
	_Assert -Name "a Changed delta is NEVER explained by vintage ($rel)" `
		-Condition ((Get-ComparabilityVerdict -Deltas @((_Delta -Name 'A' -Kind 'Changed' -Was '1' -Now '2')) -Relation $rel).Comparable)
}

_Assert -Name 'Indeterminate explains nothing, so legacy-vs-legacy is unchanged' `
	-Condition ((Get-ComparabilityVerdict -Deltas @((_Delta -Name 'A' -Kind 'Pinned' -Now '1')) -Relation 'Indeterminate').Comparable)

# --- the field itself, through the real render/read path ---

$vintagePair = _AgreeingPair
$vintageManifest = New-DeployManifest `
	-Label 'vintage' -Declared $vintagePair.Declared -Effective $vintagePair.Effective `
	-DeclarationStatus 'Available' -DeclarationReason 'test' -CgroupCpuQuota '4'
$vintageText = Format-DeployManifest -Manifest $vintageManifest

_Assert -Name 'a freshly built manifest CARRIES a vintage' `
	-Condition ($null -ne $vintageManifest.ManifestVintage)

_Assert -Name 'the rendered manifest records MANIFEST_VINTAGE' `
	-Condition ($vintageText -match '(?m)^MANIFEST_VINTAGE=\d+$')

$vintageRead = Read-DeployManifest -Text $vintageText
_Assert -Name 'the vintage ROUND-TRIPS through a read' `
	-Condition ($vintageRead.ManifestVintage -eq $vintageManifest.ManifestVintage) `
	-Detail "got $($vintageRead.ManifestVintage)"

# The tri-state again, one field further on. A legacy manifest carries no line,
# and must read back as UNKNOWN and never as vintage 0 - a numeric default would
# make every legacy baseline claim to be the oldest KNOWN instrument, which is a
# comparability claim it is not entitled to make.
$legacyVintage = Read-DeployManifest -Text ($vintageText -replace '(?m)^MANIFEST_VINTAGE=\d+\r?\n', '')
_Assert -Name 'an ABSENT vintage line reads back as $null, NOT as 0' `
	-Condition ($null -eq $legacyVintage.ManifestVintage) `
	-Detail "got '$($legacyVintage.ManifestVintage)'"

_Assert -Name 'a MALFORMED vintage value reads back as unknown, not as a guess' `
	-Condition ($null -eq (Read-DeployManifest -Text ($vintageText -replace '(?m)^MANIFEST_VINTAGE=\d+$', 'MANIFEST_VINTAGE=v2-beta')).ManifestVintage)

# AMENDMENT 25 applied to the new field: re-rendering a legacy manifest must not
# STAMP it with the current vintage. Promoting an old file to a capability it
# never had is the same hazard the DECLARATION_STATUS absence protects against.
_Assert -Name 'RE-RENDERING a legacy manifest does not PROMOTE it to the current vintage' `
	-Condition ((Format-DeployManifest -Manifest $legacyVintage) -notmatch '(?m)^MANIFEST_VINTAGE=') `
	-Detail 'a legacy baseline would silently claim comparability it does not have'

# --- end to end, through the real Compare path ---

# The exact #2992 reading: a baseline whose instrument left three cells empty,
# against a current reading that populates them. Before this change the pair
# produced three Pinned deltas and a NotAttributable verdict naming
# configuration drift on a rig where nothing had moved. The three keys are the
# ones that were ACTUALLY thin in run-13-postfix.manifest, not a stand-in, so
# this reproduces the reading rather than approximating its shape.
$thinKeys = @('EMBED_INTRA_THREADS', 'embedder.cpus', 'embedder.mem_limit')
$thinPair = _AgreeingPair
$thinDeclared = @{}
$thinEffective = @{}
foreach ($k in $thinPair.Effective.Keys) { $thinDeclared[$k] = 'same'; $thinEffective[$k] = 'same' }
foreach ($k in $thinKeys) { $thinEffective[$k] = '' }

$thinManifest = New-DeployManifest `
	-Label 'thin' -Declared $thinDeclared -Effective $thinEffective `
	-DeclarationStatus 'Available' -DeclarationReason 'test' -CgroupCpuQuota '4'
$thinBaseline = Read-DeployManifest -Text (
	(Format-DeployManifest -Manifest $thinManifest) -replace '(?m)^MANIFEST_VINTAGE=\d+\r?\n', '')

# NOT wrapped in @(). Compare-DeployManifest returns `,$deltas`, so an @()
# around it yields a ONE-element array whose single element is the real array -
# and Get-ComparabilityVerdict still reports Comparable=$false off it, because
# $_.Kind member-enumerates to three values and -eq 'Pinned' matches. A
# verdict-only assertion passes on that; only the COUNT assertion below catches
# it. Assigned bare, exactly as Assert-DeployManifest.ps1 does, so this test
# exercises the shape production actually passes.
$e2eDeltas = Compare-DeployManifest -Baseline $thinBaseline -Current $vintageManifest
$e2eRelation = Get-VintageRelation -BaselineVintage $thinBaseline.ManifestVintage -CurrentVintage $vintageManifest.ManifestVintage
$e2eComparability = Get-ComparabilityVerdict -Deltas $e2eDeltas -Relation $e2eRelation

_Assert -Name 'END TO END: the real compare path sees the legacy baseline as OLDER' `
	-Condition ($e2eRelation -eq 'BaselineOlder') -Detail "got $e2eRelation"

_Assert -Name 'END TO END: a thin legacy baseline is reported INCOMPARABLE, not as drift' `
	-Condition (-not $e2eComparability.Comparable -and $e2eComparability.ExplainedCount -gt 0) `
	-Detail "explained $($e2eComparability.ExplainedCount) of $($e2eDeltas.Count) delta(s)"

# The denominator, not the verdict: prove the deltas this excused were REAL
# ones the old path would have counted, rather than an empty population that
# any comparability claim would satisfy. Asserting the COUNT as well as the
# verdict, because a verdict assertion cannot detect an empty population - the
# M1 lesson, which has already fired once inside these very tests.
_Assert -Name 'END TO END: the excused population is exactly the three thin keys' `
	-Condition ($e2eComparability.ExplainedCount -eq 3 -and @($e2eDeltas).Count -eq 3) `
	-Detail "explained $($e2eComparability.ExplainedCount) of $(@($e2eDeltas).Count)"

_Assert -Name 'END TO END: the excused deltas are ones the OLD path called unattributable' `
	-Condition ((Get-AttributionVerdict -Deltas $e2eDeltas).Attributable -eq $false) `
	-Detail 'an empty delta set would make the incomparability claim vacuous'


_Section 'COMPOSE FILE SET (#2993 - the overlay list is asserted, not assumed)'

# The live rig's real label, defined once at the top of this file. See the note
# there for how it was captured and why it is kept verbatim.
$realLabel = $script:RealRigConfigFilesLabel

# HEALTHY CASE FIRST. A guard that refuses a correct deployment is the failure
# mode that gets guards deleted, so the arm that must work is the one where
# nothing is wrong - and it is asserted against the real bytes, not a fixture.
$healthy = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue $realLabel

_Assert -Name 'HEALTHY: the live rig label AGREES with the hardcoded list' `
	-Condition ($healthy.Status -eq 'Agreed') `
	-Detail "status $($healthy.Status): $($healthy.Reason)"

_Assert -Name 'HEALTHY: agreement reports no missing and no unexpected file' `
	-Condition (@($healthy.Missing).Count -eq 0 -and @($healthy.Unexpected).Count -eq 0) `
	-Detail "missing $(@($healthy.Missing).Count), unexpected $(@($healthy.Unexpected).Count)"

_Assert -Name 'HEALTHY: the deploy checkout path does NOT count as a divergence' `
	-Condition ($healthy.Observed -contains 'docker-compose.yml' -and $healthy.Observed -contains 'docker-compose.tuning.yml') `
	-Detail "observed [$($healthy.Observed -join ', ')]"

_Assert -Name 'HEALTHY: the overlay is present in the verified set, not just the base' `
	-Condition ($healthy.Observed -contains 'docker-compose.tuning.yml') `
	-Detail 'every attribution-relevant knob is set by the tuning overlay'

# Portability of the parse. The label is written by whichever platform created
# the stack, and is read by whichever platform runs this script.
_Assert -Name 'PARSE: forward-slash paths agree too' `
	-Condition ((Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue '/srv/app/docker-compose.yml,/srv/app/docker-compose.tuning.yml').Status -eq 'Agreed') `
	-Detail 'a Linux-created stack must not read as a mismatch'

_Assert -Name 'PARSE: bare relative names agree too' `
	-Condition ((Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue 'docker-compose.yml,docker-compose.tuning.yml').Status -eq 'Agreed') `
	-Detail 'compose does not always record absolute paths'

_Assert -Name 'PARSE: surrounding whitespace is tolerated' `
	-Condition ((Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue ' docker-compose.yml , docker-compose.tuning.yml ').Status -eq 'Agreed') `
	-Detail 'whitespace is not a configuration difference'

# UNKNOWN. The arm the issue did not ask for and the one most likely to be got
# wrong, because the tempting implementation is to fall back to the hardcoded
# list - which bypasses the check exactly when the deployment is least standard.
$unknownNull = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue $null

_Assert -Name 'UNKNOWN: an ABSENT label reads as Unknown' `
	-Condition ($unknownNull.Status -eq 'Unknown') `
	-Detail "status $($unknownNull.Status)"

_Assert -Name 'UNKNOWN: an absent label is NOT silently reported as agreement' `
	-Condition ($unknownNull.Status -ne 'Agreed') `
	-Detail 'falling back to the hardcoded list is the defect, not the remedy'

_Assert -Name 'UNKNOWN: an absent label is NOT reported as a divergence either' `
	-Condition ($unknownNull.Status -ne 'Diverged') `
	-Detail 'a non-compose container has no label to disagree with; inventing a mismatch is a false finding'

_Assert -Name 'UNKNOWN: an EMPTY label reads as Unknown' `
	-Condition ((Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue '').Status -eq 'Unknown') `
	-Detail 'empty is absent'

_Assert -Name 'UNKNOWN: a WHITESPACE-only label reads as Unknown' `
	-Condition ((Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue '   ').Status -eq 'Unknown') `
	-Detail 'whitespace is absent'

$unparseable = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue ',,,'

_Assert -Name 'UNKNOWN: a label holding nothing parseable reads as Unknown, not as zero files' `
	-Condition ($unparseable.Status -eq 'Unknown') `
	-Detail "status $($unparseable.Status)"

_Assert -Name 'UNKNOWN: the unparseable reason is DISTINCT from the absent one' `
	-Condition ($unparseable.Reason -ne $unknownNull.Reason) `
	-Detail 'a present-but-broken label is a different fact from no label at all'

_Assert -Name 'UNKNOWN: the reason SAYS the set could not be verified' `
	-Condition ($unknownNull.Reason -match 'could not be verified') `
	-Detail "reason: $($unknownNull.Reason)"

# DIVERGED. Membership in both directions.
$missingOverlay = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue '/srv/docker-compose.yml'

_Assert -Name 'DIVERGED: a deployment brought up WITHOUT the tuning overlay is refused' `
	-Condition ($missingOverlay.Status -eq 'Diverged') `
	-Detail "status $($missingOverlay.Status)"

_Assert -Name 'DIVERGED: the missing overlay is NAMED, not merely counted' `
	-Condition ($missingOverlay.Missing -contains 'docker-compose.tuning.yml') `
	-Detail "missing [$($missingOverlay.Missing -join ', ')]"

$extraOverlay = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue 'docker-compose.yml,docker-compose.tuning.yml,docker-compose.onyx.yml'

_Assert -Name 'DIVERGED: an EXTRA overlay the reader never opened is refused' `
	-Condition ($extraOverlay.Status -eq 'Diverged') `
	-Detail 'docker-compose.onyx.yml sits in the same directory on the real rig'

_Assert -Name 'DIVERGED: the extra overlay is NAMED' `
	-Condition ($extraOverlay.Unexpected -contains 'docker-compose.onyx.yml') `
	-Detail "unexpected [$($extraOverlay.Unexpected -join ', ')]"

$withOverride = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue 'docker-compose.yml,docker-compose.override.yml,docker-compose.tuning.yml'

_Assert -Name 'DIVERGED: an override.yml in the deployed set is refused' `
	-Condition ($withOverride.Status -eq 'Diverged' -and $withOverride.Unexpected -contains 'docker-compose.override.yml') `
	-Detail 'the 11,679-byte override.yml on disk is deliberately NOT part of this deployment'

$bothWays = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue 'docker-compose.yml,docker-compose.onyx.yml'

_Assert -Name 'DIVERGED: both directions of the difference are reported together' `
	-Condition (@($bothWays.Missing).Count -gt 0 -and @($bothWays.Unexpected).Count -gt 0) `
	-Detail "missing [$($bothWays.Missing -join ', ')] unexpected [$($bothWays.Unexpected -join ', ')]"

# ORDER. The issue asked for a SYMMETRIC DIFFERENCE, which is a set operation and
# is empty for this input. A later overlay overrides an earlier one, so the same
# two files in the opposite order resolve to different values. The set matches;
# the order does not; only an ordered comparison sees it.
$reordered = Get-ComposeFileSetVerdict -Expected (Get-DeployComposeFile) -LabelValue 'docker-compose.tuning.yml,docker-compose.yml'

_Assert -Name 'ORDER: a REORDERED overlay list is refused, though the set difference is EMPTY' `
	-Condition ($reordered.Status -eq 'Diverged') `
	-Detail "status $($reordered.Status) - a set comparison would have passed this"

_Assert -Name 'ORDER: the empty symmetric difference is asserted, so the case is genuinely order-only' `
	-Condition (@($reordered.Missing).Count -eq 0 -and @($reordered.Unexpected).Count -eq 0) `
	-Detail "missing $(@($reordered.Missing).Count), unexpected $(@($reordered.Unexpected).Count) - both MUST be 0 or this proves nothing about ordering"

_Assert -Name 'ORDER: OrderDiffers is set so the caller can tell reordering from membership' `
	-Condition ($reordered.OrderDiffers -eq $true) `
	-Detail 'the operator is told which of the two happened'

_Assert -Name 'ORDER: the reason SAYS overlay order, not a bare mismatch' `
	-Condition ($reordered.Reason -match 'OVERLAY ORDER') `
	-Detail "reason: $($reordered.Reason)"

_Assert -Name 'ORDER: an AGREEING set does not claim a reordering' `
	-Condition ($healthy.OrderDiffers -eq $false) `
	-Detail 'the flag must be false on the healthy case or it is meaningless'

# Structural. The verdict must always answer, and only ever with a known token.
$allStatuses = @($healthy, $unknownNull, $unparseable, $missingOverlay, $extraOverlay, $reordered) | ForEach-Object { $_.Status }

_Assert -Name 'STRUCTURE: every verdict carries one of exactly three statuses' `
	-Condition (@($allStatuses | Where-Object { $_ -notin @('Agreed', 'Diverged', 'Unknown') }).Count -eq 0) `
	-Detail "statuses seen: $($allStatuses -join ', ')"

_Assert -Name 'STRUCTURE: the probe population is 6 and covers all three statuses' `
	-Condition (@($allStatuses).Count -eq 6 -and (@($allStatuses | Sort-Object -Unique).Count -eq 3)) `
	-Detail "count $(@($allStatuses).Count), distinct $(@($allStatuses | Sort-Object -Unique).Count) - a shrunk population would make the check above vacuous"

_Assert -Name 'STRUCTURE: every verdict carries a non-empty reason' `
	-Condition (@(@($healthy, $unknownNull, $unparseable, $missingOverlay, $extraOverlay, $reordered) | Where-Object { [string]::IsNullOrWhiteSpace($_.Reason) }).Count -eq 0) `
	-Detail 'a refusal without a stated cause sends the operator looking blind'


Write-Host ('  Total {0}   Passed {1}   Failed {2}   Skipped {3}' -f `
	($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount, $script:_SkipCount)

if ($script:_SkipCount -gt 0) {
	Write-Host ''
	Write-Host ("  {0} check(s) were SKIPPED and are NOT counted as passes. The population this run" -f $script:_SkipCount) -ForegroundColor Yellow
	Write-Host '  actually covered is the Total above; read it before citing this run as coverage.' -ForegroundColor Yellow
}

Write-Host ''

exit $script:_FailCount
