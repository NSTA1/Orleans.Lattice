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


Write-Host ('  Total {0}   Passed {1}   Failed {2}   Skipped {3}' -f `
	($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount, $script:_SkipCount)

if ($script:_SkipCount -gt 0) {
	Write-Host ''
	Write-Host ("  {0} check(s) were SKIPPED and are NOT counted as passes. The population this run" -f $script:_SkipCount) -ForegroundColor Yellow
	Write-Host '  actually covered is the Total above; read it before citing this run as coverage.' -ForegroundColor Yellow
}

Write-Host ''

exit $script:_FailCount
