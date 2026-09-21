#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Refuses a tuning .env whose knobs are unset or carry a retired sentinel.

.DESCRIPTION
	Issue #2863. docker-compose.tuning.yml guards each of its seven resource
	knobs with a variable reference that errors when the variable is unset or
	empty. That is a PRESENCE check, and presence is not meaning: `0` is
	non-empty, so it satisfies the guard while meaning "decide at runtime" to
	every consumer - which is the fallback the overlay exists to eliminate. The
	deployment then runs unpinned while compose resolves cleanly, the container
	starts, and `docker ps` reports healthy.

	Compose interpolation cannot express a value predicate, so this script is
	where the predicate lives. Run it before `docker compose up`, and it is run
	for you by New-TuningEnv.ps1 immediately after that script writes a .env, so
	a derived file is checked by the same rules as a hand-edited one.

	This is the ACQUISITION half. Every decision is made by the pure functions in
	_tuningKnobs.ps1 beside it, which take a reading and return violations
	without touching a file, an environment variable, or a process. The split is
	what makes the FAILING direction demonstrable: a test can adjudicate a
	fabricated reading without a container, a compose file, or a deploy.

.PARAMETER EnvFile
	The .env to read. Defaults to the .env beside the compose files.

.PARAMETER Reading
	A pre-built hashtable of knob values, used instead of reading a file. Exists
	so a test can drive the whole script hermetically, and so an operator can
	check a candidate configuration without writing it anywhere first.

.PARAMETER Quiet
	Suppress the success line. Violations are always written.

.PARAMETER BuildCommit
	The commit the image is about to be built from. When supplied, the .env's
	tokens are additionally checked against what that commit's binary would
	understand (issue #2931).

	This is a claim about an ARTEFACT, not about source, and the two come apart:
	when this was written the running image was built from a commit predating
	the one that taught the resolver `auto`, so migrating the .env to `auto`
	before rebuilding would have thrown at silo configuration rather than
	falling back. Omitting this parameter skips the check; it never passes it
	silently.

.PARAMETER RepositoryPath
	Where to resolve -BuildCommit. Defaults to the repository containing this
	script.

.OUTPUTS
	Exit code, matching the vocabulary Assert-ContainerProvenance.ps1 beside it
	already uses:

	  0  every knob carries a meaningful value
	  2  REFUSED: at least one knob is unset or carries a retired sentinel
	  4  the .env could not be read

	REFUSED is distinct from "could not read" on purpose. An operator whose file
	is missing and an operator whose file is present but meaningless have
	different problems, and collapsing them would repeat, at the level of this
	script's own result, the exact conflation it exists to remove.

.EXAMPLE
	./scripts/Assert-TuningEnv.ps1
	Checks the .env beside the compose files.

.EXAMPLE
	./scripts/Assert-TuningEnv.ps1 -EnvFile ./.env.candidate
	Checks a candidate before putting it in place.
#>
[CmdletBinding(DefaultParameterSetName = 'File')]
param(
	[Parameter(ParameterSetName = 'File')]
	[string] $EnvFile,

	[Parameter(ParameterSetName = 'Reading', Mandatory)]
	[hashtable] $Reading,

	[string] $BuildCommit,

	[string] $RepositoryPath,

	[switch] $Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_tuningKnobs.ps1')

<#
.SYNOPSIS
	Reads a .env into a hashtable of raw values.

.DESCRIPTION
	Deliberately minimal, and deliberately NOT a general dotenv parser. It
	understands the subset compose itself documents and the subset
	New-TuningEnv.ps1 writes: KEY=VALUE, one per line, `#` comments, blank lines,
	and optional surrounding quotes. Anything richer would be this script
	inventing semantics the deployment does not have.

	A malformed line is skipped rather than fatal. The knobs it was supposed to
	supply then read as UNSET, which is a refusal with an accurate message,
	whereas failing the whole parse would report "unreadable" for a file that is
	merely untidy.
#>
function Read-TuningEnvFile {
	[CmdletBinding()]
	[OutputType([hashtable])]
	param(
		[Parameter(Mandatory)] [string] $Path
	)

	$values = @{}

	foreach ($line in [IO.File]::ReadAllLines($Path)) {
		$trimmed = $line.Trim()

		if ($trimmed.Length -eq 0 -or $trimmed.StartsWith('#')) {
			continue
		}

		$separator = $trimmed.IndexOf('=')

		if ($separator -lt 1) {
			continue
		}

		$key = $trimmed.Substring(0, $separator).Trim()
		$value = $trimmed.Substring($separator + 1).Trim()

		if ($value.Length -ge 2 -and
			(($value.StartsWith('"') -and $value.EndsWith('"')) -or
			 ($value.StartsWith("'") -and $value.EndsWith("'")))) {
			$value = $value.Substring(1, $value.Length - 2)
		}

		$values[$key] = $value
	}

	return $values
}

if ($PSCmdlet.ParameterSetName -eq 'Reading') {
	$reading = $Reading
	$source = 'the supplied reading'
}
else {
	$path = if ([string]::IsNullOrWhiteSpace($EnvFile)) {
		Join-Path (Split-Path -Parent $PSScriptRoot) '.env'
	}
	else {
		$EnvFile
	}

	if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
		Write-Host "TUNING ENV UNREADABLE: no file at $path."
		Write-Host 'Run ./scripts/New-TuningEnv.ps1 to derive one, or copy .env.example and fill it in.'
		exit 4
	}

	try {
		$reading = Read-TuningEnvFile -Path $path
	}
	catch {
		Write-Host "TUNING ENV UNREADABLE: $path could not be read. $($_.Exception.Message)"
		exit 4
	}

	$source = $path
}

# Assigned WITHOUT an @() wrapper on purpose. Get-TuningEnvViolation returns its
# array through the comma operator so a clean result survives as an empty array
# rather than collapsing to $null; wrapping that in @() re-wraps the empty array
# as a one-element array, and the script then reports one blank violation for a
# perfectly good .env. That is the passing direction failing silently, which is
# the exact shape of defect this file exists to catch, so it is worth the note.
$violations = Get-TuningEnvViolation -Reading $reading

# The token-ancestry check is ADDITIVE and opt-in. It answers a different
# question from everything above - not "is this value meaningful?" but "will the
# binary we are about to run understand it?" - and it needs a repository, which
# the value checks deliberately do not.
if (-not [string]::IsNullOrWhiteSpace($BuildCommit)) {
	$repository = if ([string]::IsNullOrWhiteSpace($RepositoryPath)) { $PSScriptRoot } else { $RepositoryPath }
	$ancestry = @{}

	foreach ($requirement in (Get-TokenAncestryRequirement | Sort-Object IntroducedIn -Unique)) {
		# A sha this repository has never heard of is left ABSENT from the map
		# rather than recorded as $false. "Not an ancestor" and "I could not
		# tell" are different findings with different remedies, and the pure
		# adjudicator words them differently - collapsing them here would throw
		# that distinction away before it ever reached the operator.
		& git -C $repository cat-file -e "$($requirement.IntroducedIn)^{commit}" 2>$null

		if ($LASTEXITCODE -ne 0) {
			continue
		}

		& git -C $repository merge-base --is-ancestor $requirement.IntroducedIn $BuildCommit 2>$null
		$ancestry[$requirement.IntroducedIn] = ($LASTEXITCODE -eq 0)
	}

	# Assigned to a variable BEFORE any @() wrapper, for exactly the reason the
	# comment above gives. @(Get-TokenAncestryViolation ...) would wrap this
	# function's single empty-array output into a ONE-element array holding an
	# empty array, and the script would report one blank violation on a healthy
	# config. Assigning first unwraps it; @($ancestryViolations) on the variable
	# is then a no-op. This is the passing direction failing silently, so the
	# two-step is deliberate and not redundant.
	$ancestryViolations = Get-TokenAncestryViolation `
		-Reading $reading `
		-AncestryResult $ancestry `
		-BuildCommit $BuildCommit

	$violations = @($violations) + @($ancestryViolations)
}

if ($violations.Count -eq 0) {
	if (-not $Quiet) {
		$knobCount = @(Get-TuningKnob).Count
		Write-Host "TUNING ENV OK: all $knobCount knobs in $source carry a meaningful value."
	}

	exit 0
}

Write-Host "TUNING ENV REFUSED: $($violations.Count) of $(@(Get-TuningKnob).Count) knobs in $source cannot be honoured."
Write-Host ''

foreach ($violation in $violations) {
	Write-Host "  - $violation"
	Write-Host ''
}

Write-Host 'Nothing has been deployed. A knob that is unset or set to a retired sentinel'
Write-Host 'would let the stack start and run against a configuration nobody chose, which'
Write-Host 'is not a wrong run but a void one that reports as clean (issue #2863).'

exit 2
