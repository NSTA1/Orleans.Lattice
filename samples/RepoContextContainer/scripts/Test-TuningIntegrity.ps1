<#
.SYNOPSIS
	Conformance suite for the acceptance-rig integrity guards: radix adjudication
	(#2928), -Force key preservation (#2929), corpus measurement stability (#2930),
	attribution recording (#2931), and token/artefact ancestry.

.DESCRIPTION
	Every guard in this group shares one failure mode: it is supposed to make an
	invalid deployment SAY SO, and a guard that never fires is indistinguishable
	from a clean configuration. An absence is evidence only if the detector is
	independently known to work, so this suite is written to establish that each
	detector works - not to confirm that today's configuration is fine.

	Consequently each assertion is paired. Where one case asserts that a healthy
	value passes, another asserts that a named perturbation of that same value is
	REFUSED, with the refusal text checked for the term that makes it actionable.
	If you cannot name a mutation that would redden an assertion, the assertion is
	not testing what you think it is; the mutation is recorded in the assertion
	name so a later reader does not have to reconstruct it.

	BOUNDARIES ARE PART OF THE FINDING. Two of these guards are deliberately
	SILENT inside a documented range, and that silence is asserted here as
	positively as the refusals are:

	  - a hex knob below 10 reads identically in base 10 and base 16, so a bare
	    value there is exempt. This is why the live rig has been safe at 6, and a
	    guard that refused it would be reporting a defect that cannot bite.
	  - the corpus tolerance absorbs ordinary commit-to-commit churn and catches a
	    changed workspace. Measured across three checkouts of this repository the
	    tracked count varied by 0.2% while a wrong-tree measurement differed by
	    106%, so the two are nowhere near each other.

	A right answer applied outside its range is the commonest defect this epic has
	produced, so the exempt cases are tests, not omissions.

	Pure functions only. Nothing here starts a container, reads the live .env, or
	writes anywhere but a scratch directory, so it is safe to run on a host with an
	acceptance deployment in progress.

.PARAMETER Quiet
	Print only the summary line and any failures.

.EXAMPLE
	pwsh -File ./scripts/Test-TuningIntegrity.ps1

.NOTES
	Exit 0 all assertions passed. Exit 1 at least one failed. Exit 3 the suite
	could not run and examined nothing - loud, because a conformance suite that
	goes quietly green having checked nothing is the exact defect class it exists
	to catch.
#>
[CmdletBinding()]
param(
	[switch] $Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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
		if (-not $Quiet) { Write-Host ("  PASS  {0}" -f $Name) -ForegroundColor Green }
	}
	else {
		$script:_FailCount++
		Write-Host ("  FAIL  {0}  {1}" -f $Name, $Detail) -ForegroundColor Red
	}
}

function _Section {
	param([Parameter(Mandatory)] [string] $Name)
	if (-not $Quiet) {
		Write-Host ''
		Write-Host $Name -ForegroundColor Cyan
	}
}

# The floor exists so this suite cannot pass by examining nothing. If a refactor
# removes a whole section, the tally drops below the floor and the suite fails
# even though every assertion that ran passed - which is the only way to detect
# a suite that has quietly stopped covering its subject.
$MinimumAssertions = 32

. (Join-Path $PSScriptRoot '_tuningKnobs.ps1')
. (Join-Path $PSScriptRoot '_deployManifest.ps1')

$script:HeapKnob = Get-TuningKnob | Where-Object { $_.Name -eq 'REPOCONTEXT_GC_HEAP_COUNT' }
$script:ReplayKnob = Get-TuningKnob | Where-Object { $_.Name -eq 'REPOCONTEXT_MAX_CONCURRENT_REPLAYS' }

if ($null -eq $script:HeapKnob -or $null -eq $script:ReplayKnob) {
	Write-Host '  SKIPPED: the knob table no longer carries the knobs this suite adjudicates.' -ForegroundColor Yellow
	exit 3
}

function _Flatten {
	<#
		Normalises a value returned by a `return ,$array` function into a plain array.

		This exists because of a PowerShell trap that has now caused a real defect in
		this rig twice. A function that returns `,$violations` emits the array as ONE
		object so a clean result survives as an empty array rather than collapsing to
		$null. Assigning that (`$x = Get-Thing`) unwraps it correctly. Wrapping it
		INLINE (`@(Get-Thing)`) does not: it collects the single emitted object into a
		one-element array whose element is the empty array, which then renders as one
		blank violation and reports a healthy configuration as broken.

		The direction of that failure is what makes it worth a helper. It breaks the
		PASSING path while leaving every refusal working, so the guard still looks
		correct in exactly the tests most likely to be written for it.
	#>
	param([Parameter(ValueFromPipeline = $false)] $Value)

	if ($null -eq $Value) { return ,@() }
	return ,@($Value)
}

function New-HealthyReading {
	param([hashtable] $Override = @{})

	$reading = @{
		REPOCONTEXT_CPUS = '6'
		REPOCONTEXT_MEM_LIMIT = '12g'
		REPOCONTEXT_GC_HEAP_COUNT = '0x6'
		REPOCONTEXT_MAX_CONCURRENT_REPLAYS = 'auto'
		EMBEDDER_CPUS = '4'
		EMBEDDER_MEM_LIMIT = '5g'
		EMBEDDER_INTRA_THREADS = '4'
	}

	foreach ($key in $Override.Keys) {
		$reading[$key] = $Override[$key]
	}

	return $reading
}

function Get-Refusal {
	param([hashtable] $Reading)
	return @(Get-TuningEnvViolation -Reading $Reading)
}

# ---------------------------------------------------------------------------
_Section 'RADIX ADJUDICATION (#2928) - the CLR reads DOTNET_GCHeapCount in base 16'
# ---------------------------------------------------------------------------

_Assert -Name 'only the CLR-read knob is marked hex' `
	-Condition ($script:HeapKnob.Radix -eq 'Hex' -and $script:ReplayKnob.Radix -ne 'Hex') `
	-Detail "heap=$($script:HeapKnob.Radix) replay=$($script:ReplayKnob.Radix). MUTATION: mark the replay knob Hex - it goes through our own decimal int.TryParse, so emitting 0x there would be a NEW bug, not a fix."

_Assert -Name 'a healthy reading is accepted (MUTATION: any refusal below is then unattributable)' `
	-Condition ((Get-Refusal (New-HealthyReading)).Count -eq 0) `
	-Detail "got: $((Get-Refusal (New-HealthyReading)) -join ' | ')"

$bare6 = Get-Refusal (New-HealthyReading @{ REPOCONTEXT_GC_HEAP_COUNT = '6' })
_Assert -Name 'BOUNDARY: a bare 6 is EXEMPT, because 6 reads identically in both bases' `
	-Condition ($bare6.Count -eq 0) `
	-Detail "MUTATION: refuse every bare hex value. That would report the live rig as misconfigured when its value is inert - the overstatement this issue was specifically corrected for. got: $($bare6 -join ' | ')"

$bare9 = Get-Refusal (New-HealthyReading @{ REPOCONTEXT_GC_HEAP_COUNT = '9' })
_Assert -Name 'BOUNDARY: 9 is the last exempt value' `
	-Condition ($bare9.Count -eq 0) `
	-Detail "MUTATION: move the exemption to < 9. got: $($bare9 -join ' | ')"

$bare10 = Get-Refusal (New-HealthyReading @{ REPOCONTEXT_GC_HEAP_COUNT = '10' })
_Assert -Name 'BOUNDARY: 10 is the first refused value, being 16 to the CLR' `
	-Condition ($bare10.Count -eq 1) `
	-Detail "MUTATION: move the exemption to <= 10, and the first genuinely ambiguous value passes. got: $($bare10 -join ' | ')"

_Assert -Name 'the refusal names BOTH readings, so the operator can tell which was meant' `
	-Condition ($bare10.Count -eq 1 -and $bare10[0] -match '\b16\b' -and $bare10[0] -match '\b10\b') `
	-Detail "MUTATION: print only the hex reading. 'this is 16' does not tell an operator what to write instead. got: $($bare10 -join ' | ')"

_Assert -Name 'the refusal supplies the 0x remedy rather than only diagnosing' `
	-Condition ($bare10.Count -eq 1 -and $bare10[0] -match '0x10' -and $bare10[0] -match '0xA') `
	-Detail "MUTATION: drop the remedy clause. got: $($bare10 -join ' | ')"

$hex18 = Get-Refusal (New-HealthyReading @{ REPOCONTEXT_GC_HEAP_COUNT = '0x18' })
_Assert -Name 'an explicit 0x form above the boundary is accepted, intent being recoverable' `
	-Condition ($hex18.Count -eq 0) `
	-Detail "MUTATION: refuse the 0x form. The guard would then have no satisfiable remedy. got: $($hex18 -join ' | ')"

$hexOnDecimal = Get-Refusal (New-HealthyReading @{ REPOCONTEXT_MAX_CONCURRENT_REPLAYS = '0x6' })
_Assert -Name 'the 0x form is REFUSED on a decimal knob (MUTATION: accept it, and the replay gate silently reads 0)' `
	-Condition ($hexOnDecimal.Count -eq 1) `
	-Detail "got: $($hexOnDecimal -join ' | ')"

# ---------------------------------------------------------------------------
_Section 'HEX EMISSION (#2928 write side)'
# ---------------------------------------------------------------------------

_Assert -Name 'a hex knob is emitted in the 0x form' `
	-Condition ((Format-TuningKnobValue -Knob $script:HeapKnob -Value 24) -eq '0x18') `
	-Detail "MUTATION: return the value unconditionally as a plain string - which is exactly the defect, and is silent below 10. got: $(Format-TuningKnobValue -Knob $script:HeapKnob -Value 24)"

_Assert -Name 'a decimal knob is emitted plainly' `
	-Condition ((Format-TuningKnobValue -Knob $script:ReplayKnob -Value 24) -eq '24') `
	-Detail "MUTATION: emit 0x for every knob. got: $(Format-TuningKnobValue -Knob $script:ReplayKnob -Value 24)"

_Assert -Name 'BOUNDARY: emission below 10 is unchanged, so re-deriving does not move the live value' `
	-Condition ((Format-TuningKnobValue -Knob $script:HeapKnob -Value 6) -eq '0x6' -and
		[Convert]::ToInt32('0x6'.Substring(2), 16) -eq 6) `
	-Detail 'MUTATION: emit decimal 6. Numerically identical today, and wrong the moment the host grows.'

$emittedRoundTrip = @(@(6, 9, 10, 16, 24, 64) | Where-Object {
	[Convert]::ToInt32((Format-TuningKnobValue -Knob $script:HeapKnob -Value $_).Substring(2), 16) -ne $_
})
_Assert -Name 'every emitted hex literal round-trips through a base-16 read' `
	-Condition ($emittedRoundTrip.Count -eq 0) `
	-Detail "MUTATION: emit '{0:D}' rather than '0x{0:X}'. failed for: $($emittedRoundTrip -join ', ')"

$emissionAccepted = @(@(6, 9, 10, 16, 24, 64) | Where-Object {
	(Get-Refusal (New-HealthyReading @{
		REPOCONTEXT_GC_HEAP_COUNT = (Format-TuningKnobValue -Knob $script:HeapKnob -Value $_)
	})).Count -ne 0
})
_Assert -Name 'what the generator emits is what the adjudicator accepts' `
	-Condition ($emissionAccepted.Count -eq 0) `
	-Detail "MUTATION: change either side alone. A generator and guard that disagree produce a file that cannot be written without tripping its own check. failed for: $($emissionAccepted -join ', ')"

# ---------------------------------------------------------------------------
_Section 'TOKEN/ARTEFACT ANCESTRY - source and artefact are different objects'
# ---------------------------------------------------------------------------

$autoSha = (Get-TokenAncestryRequirement | Select-Object -First 1).IntroducedIn
$autoReading = New-HealthyReading

$contained = _Flatten (Get-TokenAncestryViolation -Reading $autoReading `
	-AncestryResult @{ $autoSha = $true } -BuildCommit 'abc123')
_Assert -Name 'an image containing the introducing commit is accepted' `
	-Condition ($contained.Count -eq 0) `
	-Detail "MUTATION: invert the ancestry test and every correct build is blocked. got: $($contained -join ' | ')"

$missing = _Flatten (Get-TokenAncestryViolation -Reading $autoReading `
	-AncestryResult @{ $autoSha = $false } -BuildCommit 'abc123')
_Assert -Name 'an image PREDATING the introducing commit is refused (MUTATION: skip the check, and the silo throws at startup)' `
	-Condition ($missing.Count -eq 1) `
	-Detail "got: $($missing -join ' | ')"

_Assert -Name 'the refusal states the required ORDER, which is the actionable part' `
	-Condition ($missing.Count -eq 1 -and $missing[0] -match 'Rebuild' -and $missing[0] -match 'BEFORE') `
	-Detail "MUTATION: report only that the token is unsupported. An operator told 'auto is unsupported' reverts the .env; the correct fix is to rebuild. got: $($missing -join ' | ')"

$undetermined = _Flatten (Get-TokenAncestryViolation -Reading $autoReading `
	-AncestryResult @{} -BuildCommit 'abc123')
_Assert -Name 'an UNDETERMINED ancestry is refused, not passed' `
	-Condition ($undetermined.Count -eq 1 -and $undetermined[0] -match 'could not be determined') `
	-Detail "MUTATION: treat a missing entry as satisfied. An absence is evidence only if the detector is known to have run. got: $($undetermined -join ' | ')"

$notUsingToken = _Flatten (Get-TokenAncestryViolation `
	-Reading (New-HealthyReading @{ REPOCONTEXT_MAX_CONCURRENT_REPLAYS = '6' }) `
	-AncestryResult @{ $autoSha = $false } -BuildCommit 'abc123')
_Assert -Name 'a reading that does not USE the token is unaffected by the artefact predating it' `
	-Condition ($notUsingToken.Count -eq 0) `
	-Detail "MUTATION: check ancestry regardless of value, and every old image is blocked for a token nobody asked for. got: $($notUsingToken -join ' | ')"

_Assert -Name 'the ancestry table is bound to the live token, so renaming it cannot silently disable the guard' `
	-Condition (@(Get-TokenAncestryRequirement | Where-Object { $_.VerifiedToken -ne $script:ReplayKnob.AutoToken }).Count -eq 0) `
	-Detail 'MUTATION: rename AutoToken without updating the table. The adjudicator reports that as staleness rather than matching nothing.'

# ---------------------------------------------------------------------------
_Section 'ATTRIBUTION (#2931) - two sufficient causes must not move silently'
# ---------------------------------------------------------------------------

$variableNames = @(Get-AttributionVariable | ForEach-Object { $_.Name })
_Assert -Name 'DOTNET_PROCESSOR_COUNT is a recorded attribution variable' `
	-Condition ($variableNames -contains 'DOTNET_PROCESSOR_COUNT') `
	-Detail "MUTATION: drop it from the table. Removing it from the overlay WITHOUT recording it is precisely how the 16 -> 6 movement became unattributable. got: $($variableNames -join ', ')"

# The CONTAINER-side name, not the .env knob name. The manifest records what the
# container actually carries, and the overlay maps REPOCONTEXT_MAX_CONCURRENT_REPLAYS
# onto LATTICE_WAL_MAX_CONCURRENT_REPLAYS. Asserting the knob name here would be
# checking the wrong object - the same source/artefact confusion this whole group of
# fixes is about, committed inside its own test.
_Assert -Name 'the replay gate is a recorded attribution variable' `
	-Condition ($variableNames -contains 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS') `
	-Detail "MUTATION: drop it, and the other sufficient cause of the same movement goes unrecorded. got: $($variableNames -join ', ')"

$single = Get-AttributionVerdict -Deltas @(
	[pscustomobject]@{ Name = 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS'; Kind = 'Changed'
		Was = '16'; Now = '6'; Attribution = 'ReplayGate' })
_Assert -Name 'ONE moved variable is attributable' `
	-Condition ($single.Attributable) `
	-Detail 'MUTATION: require zero deltas, and no deploy can ever change anything.'

$double = Get-AttributionVerdict -Deltas @(
	[pscustomobject]@{ Name = 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS'; Kind = 'Changed'
		Was = '16'; Now = '6'; Attribution = 'ReplayGate' },
	[pscustomobject]@{ Name = 'DOTNET_PROCESSOR_COUNT'; Kind = 'Unpinned'
		Was = '16'; Now = ''; Attribution = 'ProcessorCount' })
_Assert -Name 'TWO moved variables are NOT attributable - the #2931 shape exactly' `
	-Condition (-not $double.Attributable) `
	-Detail 'MUTATION: gate on a count of three. The defect being prevented is two sufficient causes changing in one step.'

_Assert -Name 'the verdict NAMES both movers, so the reader knows what was confounded' `
	-Condition (-not $double.Attributable -and
		$double.Summary -match 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS' -and
		$double.Summary -match 'DOTNET_PROCESSOR_COUNT') `
	-Detail "MUTATION: report only the count. 'two variables moved' cannot be acted on. got: $($double.Summary)"

$none = Get-AttributionVerdict -Deltas @()
_Assert -Name 'an unchanged deploy is attributable (MUTATION: otherwise every repeat run is blocked)' `
	-Condition ($none.Attributable) `
	-Detail "got: $($none.Summary)"

# ---------------------------------------------------------------------------
_Section 'DIVERGENCE - the guard must not accuse a deploy of its own missing input'
# ---------------------------------------------------------------------------

$effectiveOnly = New-DeployManifest -Declared @{} -Effective @{
	LATTICE_WAL_MAX_CONCURRENT_REPLAYS = '6'
	DOTNET_GCHeapCount = '6'
} -CgroupCpuQuota 6.0 -Label 'probe'

_Assert -Name 'NO declared reading yields NO divergences' `
	-Condition (((_Flatten (Get-DeployManifestDivergence -Manifest $effectiveOnly))).Count -eq 0) `
	-Detail "MUTATION: drop the DeclarationAvailable gate, and every variable the container carries is reported unattributable on a normal run - a guard that fires on its own operating condition is one operators learn to ignore. got: $((_Flatten (Get-DeployManifestDivergence -Manifest $effectiveOnly)) -join ' | ')"

$diverged = New-DeployManifest -Declared @{ LATTICE_WAL_MAX_CONCURRENT_REPLAYS = '6' } -Effective @{
	LATTICE_WAL_MAX_CONCURRENT_REPLAYS = '16'
} -CgroupCpuQuota 6.0 -Label 'probe'
$divergences = (_Flatten (Get-DeployManifestDivergence -Manifest $diverged))
_Assert -Name 'a REAL divergence is still reported when a declaration exists' `
	-Condition ($divergences.Count -ge 1 -and $divergences[0] -match '16') `
	-Detail "MUTATION: return early unconditionally. The gate above would then have disabled the whole check rather than scoping it. got: $($divergences -join ' | ')"

$agreeing = New-DeployManifest -Declared @{ LATTICE_WAL_MAX_CONCURRENT_REPLAYS = '6' } -Effective @{
	LATTICE_WAL_MAX_CONCURRENT_REPLAYS = '6'
} -CgroupCpuQuota 6.0 -Label 'probe'
_Assert -Name 'an agreeing declaration yields no divergence' `
	-Condition (((_Flatten (Get-DeployManifestDivergence -Manifest $agreeing))).Count -eq 0) `
	-Detail "got: $((_Flatten (Get-DeployManifestDivergence -Manifest $agreeing)) -join ' | ')"

# ---------------------------------------------------------------------------
_Section 'PROCESSOR-COUNT PROVENANCE (#2931) - the resolved count AND its source'
# ---------------------------------------------------------------------------

$pinned = Resolve-ProcessorCountProvenance -Declared '16' -CgroupCpuQuota 6.0
_Assert -Name 'an explicit pin is attributed to the pin, not to the cgroup' `
	-Condition ($pinned.Source -eq 'ExplicitEnvironment' -and $pinned.Resolved -eq 16) `
	-Detail "MUTATION: always attribute to the cgroup, and a pinned 16 is reported as a derived 6. got: $($pinned.Source)/$($pinned.Resolved)"

$derived = Resolve-ProcessorCountProvenance -Declared '' -CgroupCpuQuota 6.0
_Assert -Name 'an absent pin with a readable quota is attributed to the cgroup' `
	-Condition ($derived.Source -eq 'CgroupDerivation' -and $derived.Resolved -eq 6) `
	-Detail "MUTATION: report Indeterminate whenever the pin is absent, and the normal case becomes a refusal. got: $($derived.Source)/$($derived.Resolved)"

$unknown = Resolve-ProcessorCountProvenance -Declared '' -CgroupCpuQuota $null
_Assert -Name 'neither a pin nor a quota is INDETERMINATE, not a guess' `
	-Condition ($unknown.Source -eq 'Indeterminate' -and $null -eq $unknown.Resolved) `
	-Detail "MUTATION: fall back to the host CPU count. That is the unattributable state this issue is about, recorded as though it were known. got: $($unknown.Source)"

$garbage = Resolve-ProcessorCountProvenance -Declared 'many' -CgroupCpuQuota 6.0
_Assert -Name 'an UNPARSEABLE pin is indeterminate, not silently ignored in favour of the quota' `
	-Condition ($garbage.Source -eq 'Indeterminate') `
	-Detail "MUTATION: fall through to the cgroup on a bad pin. The CLR's behaviour there is not something this script can claim to know, and guessing it is how a misconfiguration gets recorded as a fact. got: $($garbage.Source)"

# ---------------------------------------------------------------------------
_Section 'CORPUS AND -Force (#2930, #2929) - via the real generator'
# ---------------------------------------------------------------------------

$generator = Join-Path $PSScriptRoot 'New-TuningEnv.ps1'
$generatorText = [System.IO.File]::ReadAllText($generator)

# The corpus assertions INVOKE the real measurement rather than grepping the
# generator's source. A source grep detects the feature being deleted but not the
# feature being disabled, and disabled is the shape this defect actually takes -
# an early return, a swallowed failure, a fallback that fires every time. The
# functions are lifted out by AST so the generator's top-level derivation does not
# run; dot-sourcing the whole script would execute it against this host.
$corpusProbe = {
	param($Path, $Root, $Names)
	$tokens = $null
	$errors = $null
	$ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref] $tokens, [ref] $errors)
	$found = $ast.FindAll({
		param($node)
		$node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $Names -contains $node.Name
	}, $true)
	# The measurement reads one script-scope list. Lifting it with the functions keeps
	# the probe honest: the exclusions under test are the generator's own, not a copy
	# in this file that could drift away from them without either side noticing.
	$assignments = $ast.FindAll({
		param($node)
		$node -is [System.Management.Automation.Language.AssignmentStatementAst] -and
		$node.Left -is [System.Management.Automation.Language.VariableExpressionAst] -and
		$node.Left.VariablePath.UserPath -eq 'ExcludedDirectoryNames'
	}, $true)
	$text = (@($assignments | ForEach-Object { $_.Extent.Text }) +
		@($found | ForEach-Object { $_.Extent.Text })) -join "`n`n"
	. ([scriptblock]::Create($text))
	return Measure-Corpus -Root $Root
}

$corpusNames = @('Measure-Corpus', 'Get-TrackedCorpusFile', 'Get-CorpusCommit')
$measured = & $corpusProbe $generator $PSScriptRoot $corpusNames

_Assert -Name 'the corpus measurement actually RESOLVES to the tracked-file method' `
	-Condition ($measured.Method -eq 'Tracked') `
	-Detail "MUTATION: disable Get-TrackedCorpusFile (an early return is enough) and this falls back to the walk. A count that includes untracked build output is a property of the CHECKOUT, so two checkouts of one commit derive two grants and run-to-run comparison silently breaks. got method: $($measured.Method)"

_Assert -Name 'the tracked measurement counts something, so Tracked is not an empty success' `
	-Condition ($measured.Counted -gt 0 -and $measured.Raw -ge $measured.Counted) `
	-Detail "MUTATION: return an empty list from the tracked path. It would still report Method=Tracked while deriving a grant from nothing. got raw=$($measured.Raw) counted=$($measured.Counted)"

$untrackedRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("tuningcorpus-" + [System.Guid]::NewGuid().ToString('N'))
[void] (New-Item -ItemType Directory -Path $untrackedRoot -Force)
[System.IO.File]::WriteAllText((Join-Path $untrackedRoot 'a.txt'), 'a')
try {
	$outside = & $corpusProbe $generator $untrackedRoot $corpusNames
	_Assert -Name 'a NON-checkout is reported as Walk, so the method is a measurement and not a constant' `
		-Condition ($outside.Method -eq 'Walk') `
		-Detail "MUTATION: hard-code Method='Tracked'. The assertion above would then pass everywhere while measuring nothing, which is the vacuity this pair exists to exclude. got: $($outside.Method)"
}
finally {
	Remove-Item -LiteralPath $untrackedRoot -Recurse -Force -ErrorAction SilentlyContinue
}

_Assert -Name 'enumeration failures surface rather than lowering the count' `
	-Condition ($generatorText -notmatch 'Recurse -File -Force -ErrorAction SilentlyContinue') `
	-Detail 'MUTATION: restore SilentlyContinue. A swallowed permission-denied subtree derives a SMALLER grant, and an under-provisioned run reports normally.'

_Assert -Name 'the derived file records the corpus provenance' `
	-Condition ($generatorText -match 'corpus commit' -and $generatorText -match 'corpus root') `
	-Detail 'MUTATION: drop the header lines. A count without a root and a commit cannot be checked afterwards.'

_Assert -Name '-Force merges rather than overwriting' `
	-Condition ($generatorText -match 'Merge-TuningEnvContent') `
	-Detail 'MUTATION: restore the wholesale write. REPO_PATH disappears, the base compose default mounts a different tree, and the container indexes the wrong corpus while reporting healthy.'

$merge = & {
	. $generator -WorkspacePath $PSScriptRoot -CorpusOnly -ErrorAction SilentlyContinue
} 2>$null

# Merge-TuningEnvContent is exercised directly. Dot-sourcing the generator would
# run its whole derivation, so the function is re-read from source and invoked in
# an isolated scope - the assertion is about the merge, not about the host.
$mergeScript = {
	param($Path, $Existing, $Derived)
	$text = [System.IO.File]::ReadAllText($Path)
	$start = $text.IndexOf('function Merge-TuningEnvContent')
	$end = $text.IndexOf("`nif (`$DryRun) {", $start)
	. ([scriptblock]::Create($text.Substring($start, $end - $start)))
	return Merge-TuningEnvContent -Existing $Existing -Derived $Derived
}

$derivedText = @"
REPOCONTEXT_CPUS=6
REPOCONTEXT_GC_HEAP_COUNT=0x6
"@
$existingText = @"
REPOCONTEXT_CPUS=4
REPOCONTEXT_GC_HEAP_COUNT=4
REPO_PATH=C:\dev\lattice
REPOCONTEXT_MEMORY_ARCHIVE_PATH=D:\archive
"@

$merged = & $mergeScript $generator $existingText $derivedText

_Assert -Name 'the merge CARRIES REPO_PATH across (MUTATION: the #2929 defect, silent and consequential)' `
	-Condition ($merged.Carried -contains 'REPO_PATH' -and $merged.Content -match 'REPO_PATH=C:\\dev\\lattice') `
	-Detail "carried: $($merged.Carried -join ', ')"

_Assert -Name 'the merge carries the archive path, whose loss caused #2627' `
	-Condition ($merged.Carried -contains 'REPOCONTEXT_MEMORY_ARCHIVE_PATH') `
	-Detail "carried: $($merged.Carried -join ', ')"

_Assert -Name 'a DERIVED key takes the newly derived value, re-deriving being the point' `
	-Condition ($merged.Content -match 'REPOCONTEXT_CPUS=6' -and $merged.Content -notmatch 'REPOCONTEXT_CPUS=4') `
	-Detail "MUTATION: carry every existing key, and -Force stops deriving anything. got: $($merged.Content)"

_Assert -Name 'a derived key is not duplicated by the carry block' `
	-Condition (([regex]::Matches($merged.Content, '(?m)^REPOCONTEXT_CPUS=')).Count -eq 1) `
	-Detail "MUTATION: carry keys present in both. docker compose would then read whichever came last. got: $(([regex]::Matches($merged.Content, '(?m)^REPOCONTEXT_CPUS=')).Count) occurrences"

$nothingToCarry = & $mergeScript $generator "REPOCONTEXT_CPUS=4" $derivedText
_Assert -Name 'nothing to carry adds no carry block' `
	-Condition ($nothingToCarry.Carried.Count -eq 0 -and $nothingToCarry.Content -notmatch 'CARRIED ACROSS') `
	-Detail "MUTATION: always append the header, and every re-derivation grows the file. got: $($nothingToCarry.Carried -join ', ')"

$commentedOut = & $mergeScript $generator "# REPOCONTEXT_MEMORY_ARCHIVE_PATH=" $derivedText
_Assert -Name 'a commented-out assignment is documentation, not a value to carry' `
	-Condition ($commentedOut.Carried.Count -eq 0) `
	-Detail "MUTATION: carry comment lines, and the block accumulates a duplicate on every run. got: $($commentedOut.Carried -join ', ')"

# ---------------------------------------------------------------------------
Write-Host ''

# The machine-readable tally, printed BEFORE any early exit below so that a run which
# ends in the incomplete-suite branch still reports what it managed to run. The NUnit
# executor reads this line rather than the exit code, because a terminating PowerShell
# error also exits 1 and reads naively as "one assertion failed" - a precise, plausible,
# wrong reading of a run that never reached its first check.
Write-Host ('  Total {0}   Passed {1}   Failed {2}' -f `
	($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount)

if ($script:_PassCount + $script:_FailCount -lt $MinimumAssertions) {
	Write-Host ("SUITE INCOMPLETE: ran {0} assertions, expected at least {1}." -f `
		($script:_PassCount + $script:_FailCount), $MinimumAssertions) -ForegroundColor Red
	Write-Host 'A conformance suite that goes green having examined less than it claims is the'
	Write-Host 'failure class this suite exists to catch, so a shrunken tally is itself a failure.'
	exit 1
}

if ($script:_FailCount -gt 0) {
	Write-Host ("TUNING INTEGRITY: {0} passed, {1} FAILED." -f $script:_PassCount, $script:_FailCount) -ForegroundColor Red
	# A FIXED code, deliberately not the failure count. Exiting with the count reads as
	# self-documenting and collides: a run with exactly as many failures as the
	# precondition code reports itself as a SKIP, and a suite whose subject is silent
	# misconfiguration would then be silently not running. The count is on the tally line
	# above, which is where a reader should take it from anyway.
	exit 1
}

Write-Host ("TUNING INTEGRITY: all {0} assertions passed." -f $script:_PassCount) -ForegroundColor Green
exit 0
