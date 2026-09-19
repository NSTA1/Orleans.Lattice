<#
.SYNOPSIS
    Derives this host's resource knobs for docker-compose.tuning.yml and writes .env.

.DESCRIPTION
    Issue #2779. Every resource knob in the tuning overlay used to be a literal
    transcription of one developer machine (16 logical CPUs, 55.7 GiB RAM). The two
    mem_limit values summed to 17 GiB, so the stack could not start at all on a 16 GiB
    host, and DOTNET_PROCESSOR_COUNT: "16" held the WAL replay concurrency gate at 16
    permits against a 6.0-CPU quota - a 2.67x oversubscription measured on the live
    deployment, and the deployment half of the root cause in #2692.

    Compose cannot compute, so derivation happens here, at deploy time, and reaches the
    overlay through ${VAR:?...} references that have no defaults. A default would be the
    transcription problem wearing a variable's name: an operator who does not know to
    override it is in exactly the state this script exists to prevent.

    WHAT IS DERIVED FROM WHAT. The memory grant is CORPUS-derived and host-CLAMPED. It is
    deliberately NOT a fraction of host RAM: the requirement is a property of the indexed
    corpus, so a fixed fraction would grant far too much on a large machine and far too
    little on a small one for the identical repository. That 12g happened to be 21.5% of
    this box's 55.7 GiB is a coincidence, not a rule. The CPU grants ARE host-derived,
    because they are a share of a contended resource rather than a requirement.

        grant = clamp(corpusRequirement + headroom, FLOOR, hostUsableForContainers)

    When the host ceiling binds BELOW the corpus requirement, this script REFUSES and
    names both figures. It does not silently grant less. A silent under-grant reproduces
    the very defect being fixed while reporting success, and an undersized repocontext
    grant does not present as a container kill - it presents as a STORAGE fault
    (OutOfMemoryException inside a grain-state read) while `docker ps` still reports the
    container healthy.

.PARAMETER WorkspacePath
    The host directory mounted read-only at /workspace. Defaults to REPO_PATH from an
    existing .env, else this file's grandparent, matching the base compose file.

.PARAMETER OutFile
    Where to write. Defaults to .env beside the compose files.

.PARAMETER DryRun
    Print the derivation and the resulting file without writing anything.

.PARAMETER Force
    Overwrite the derived keys in an existing .env, PRESERVING every key this script does
    not derive.

    -Force used to rewrite the file wholesale, which silently dropped REPO_PATH along with
    every other hand-set key (issue #2929). That is not a cosmetic loss: with REPO_PATH
    gone, the base compose file falls back to its own default and mounts a DIFFERENT tree
    at /workspace, so the container indexes the wrong corpus while every layer reports
    success. It now merges: derived keys are replaced, unrecognised keys are carried
    across untouched, and both counts are reported.

.PARAMETER ExpectedCorpusFiles
    Refuse unless the measured corpus is within -CorpusTolerance of this count.

    The memory grant derives from the corpus size, so two runs measured against corpora
    of different sizes are not comparable - and that is the basis the acceptance rig
    scores on. Declaring the expected size makes a moved corpus a refusal instead of an
    unremarked change in the denominator (issue #2930).

.PARAMETER CorpusTolerance
    Fractional drift allowed by -ExpectedCorpusFiles. Defaults to 0.02 (2%), which
    absorbs ordinary commit-to-commit churn while catching a changed workspace. Measured
    across three checkouts of this repository the tracked count varied by 0.2%, and a
    wrong-tree measurement differed by 106%, so the two are not close together.

.EXAMPLE
    pwsh -File ./scripts/New-TuningEnv.ps1 -DryRun
#>
[CmdletBinding()]
param(
    [string] $WorkspacePath,
    [string] $OutFile,
    [switch] $DryRun,
    [switch] $CorpusOnly,
    [switch] $IgnoreHostLoad,
    [int] $ExpectedCorpusFiles,
    [ValidateRange(0.0, 1.0)]
    [double] $CorpusTolerance = 0.02,
    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# The knob table is the single place that knows each knob's RADIX. The heap count is
# emitted through Format-TuningKnobValue rather than string-interpolated, because
# DOTNET_GCHeapCount is read by the CLR in base 16 and this script used to write it in
# decimal (issue #2928). That was inert at 6 - below 10 the two bases agree - and
# becomes a real misconfiguration the moment a host is large enough for the derived
# count to reach 10, where a written 10 is honoured as 16.
. (Join-Path $PSScriptRoot '_tuningKnobs.ps1')

# ---------------------------------------------------------------------------
# PROVENANCE OF EVERY CONSTANT.
#
# Each entry says where the number came from and what would falsify it. A constant
# whose provenance is "policy" is a CHOICE, not a measurement, and is labelled that way
# on purpose: dressing a policy as a derivation is how an arbitrary number acquires
# unearned authority.
#
# MEASUREMENT CONDITION. The memory constants below were fitted with the replay gate at
# 6 permits, which is the condition this change creates. They are an UPPER BOUND with
# respect to #2692: memory-adaptive backpressure can only reduce admitted concurrency
# below the core-count ceiling, never raise it, so a requirement measured at gate=6
# cannot under-grant a system that additionally throttles. Being conservative is not
# free - it is the safe direction for FLOOR and the wrong direction for a clamp refusal,
# because it refuses to start on a host that would have coped.
#
# REFIT TRIGGER: if #2692 reduces the measured steady-state requirement by more than
# 15%, refit FIXED_OVERHEAD_BYTES and PER_FILE_BYTES and update this block.
# ---------------------------------------------------------------------------

# POLICY. The share of host RAM this stack may claim in total. Leaves the rest for the
# operating system, the editor, and everything else the developer is running. Not a
# measurement and not derived from anything; it is a judgement about a shared machine.
$HOST_MEMORY_SHARE = 0.45

# POLICY. The share of host logical CPUs the repocontext service may claim. The original
# deployment used 6 of 16 (37.5%) and measured about 92% of ONE core in steady state, so
# this is headroom against a burst rather than a working limit.
$REPOCONTEXT_CPU_SHARE = 0.375

# POLICY. The same for the embedder: 4 of 16 (25%) on the reference host.
$EMBEDDER_CPU_SHARE = 0.25

# MEASURED, workload-derived. The embedder's footprint is dominated by the ONNX model
# resident in the image, so it does not grow with the repository. Measured at 4.08 GiB
# with no limit, and observed pinned at 2.486 GiB of a 2560m cap (99.4%) while idle,
# holding the model at its ceiling with no room to work. 5 GiB clears the requirement.
# FALSIFIER: a different embedding model in the image changes this and nothing else here.
$EMBEDDER_MEMORY_BYTES = 5GB

# PROVISIONAL, pending the requirement sweep. The intercept: what an essentially empty
# corpus costs (runtime, Orleans, SQLite, the host itself). Stated as provisional rather
# than measured because the near-empty arm has not been run on this build.
#
# THIS IS THE WEAKEST NUMBER IN THE MODEL, AND THE REASON IS STRUCTURAL, NOT SLOPPY.
# It and PER_FILE_BYTES below were fitted together against ONE observation (an 11.0-11.2
# GiB plateau at 8,239 indexed files). One point does not identify two parameters: every
# (intercept, slope) pair on `intercept + slope * 8239 = 11.05 GiB` fits that datum
# exactly as well. The split into "3 GiB fixed + 1 MB per file" is therefore ASSUMED, not
# measured. Near n ~ 8,000 the model interpolates correctly, because that is the region
# the one point pins; at a materially different corpus size the split IS the answer and
# has no evidence under it. The 6 GiB floor bounds the low end, the host clamp bounds the
# high end, and the refusal makes the high end loud - that is what contains the risk, not
# the quality of the fit.
#
# FALSIFIER, and it is a DIRECT MEASUREMENT rather than another fit: run the container
# once against a near-empty corpus. At n ~ 0 the variable term vanishes and the settled
# plateau IS this intercept. That converts two unknowns and one equation into one
# measured intercept plus one measured slope, with the existing 8,239-file point then
# determining the slope alone. Two runs, one of them trivially cheap because an empty
# index does almost no work. Prefer this over sweeping a second large corpus, which
# would still be two parameters fitted from two points with no redundancy.
$FIXED_OVERHEAD_BYTES = 3GB

# PROVISIONAL, pending the requirement sweep. The slope, per indexed file.
#
# FITTED AGAINST: the GIT-TRACKED file count, 8,239 files in this repository, against a
# measured steady state of 11.0 to 11.2 GiB, with the intercept above subtracted. Check
# it: 3 GiB + 1 MB * 8,239 = 11.05 GiB, which is the observed plateau.
#
# THE UNIT THIS IS DENOMINATED IN IS LOAD-BEARING AND IS NOT A RAW TREE WALK. A
# Get-ChildItem -Recurse -Force over the same repository returns about 26,850 files -
# 3.15x the tracked count - because it counts .git object files, bin/, and obj/. Feeding
# that count into this slope requests roughly 35 GiB, which on the reference host clamps
# to the ceiling and fires the shortfall warning below, reporting "the host is too small"
# when the host is fine and the MEASUREMENT is 3.15x too large. That is worse than no
# diagnostic, because raising HOST_MEMORY_SHARE or buying RAM both appear to work and
# confirm the wrong model. Measure-Corpus enforces the exclusions; do not bypass it.
#
# A TWO-point fit cannot see non-linearity, and this is currently a ONE-point fit, which
# is weaker still.
# FALSIFIER: a corpus sweep whose measured requirement departs from this line.
$PER_FILE_BYTES = 1MB

# The directory names excluded from the corpus count, and the reason the count is a
# proxy for "what repocontext will ingest" rather than a raw tree walk.
#
# APPROXIMATION, STATED. This does not reproduce the ingest filter exactly; it is a
# cheap stand-in for it. On this repository it yields 8,530 against 8,514 git-tracked
# files, a 0.2% overshoot, which is well inside the precision of a one-point fit. The
# git-tracked count is the quantity PER_FILE_BYTES was fitted against, so agreeing with
# it to that tolerance is the property that matters.
#
# WHY NOT JUST SHELL OUT TO `git ls-files`. Because the indexed corpus is not required
# to be a git repository - repocontext indexes a mounted directory - so a git-only count
# would fail or silently return zero on a perfectly valid non-git corpus. The exclusion
# list works on any tree and degrades gracefully.
#
# WHY THIS IS NOT A HOST-DERIVED QUANTITY IN DISGUISE. The .git / bin / obj share of a
# tree is a property of THAT CHECKOUT's history and build state, not of the repository:
# a fresh shallow clone and a long-lived built worktree differ several-fold for a
# byte-identical source corpus. Counting them would make two hosts indexing the same
# repository derive materially different grants, which is precisely the defect this
# whole change exists to remove. The quiet direction is the dangerous one - a freshly
# cloned, never-built checkout counts LOW and therefore grants LOW.
$ExcludedDirectoryNames = @(
    '.git',
    'bin',
    'obj',
    'node_modules',
    '.vs',
    '.idea',
    'TestResults',
    'packages'
)

# MEASURED. The floor below which this stack must not be started at all. A 4 GiB cap was
# about 40% of the known requirement and produced 2,929 OutOfMemoryException lines in a
# grain-state read path, 259 cold activations across 64 leaves (4.05x repeat-cold), and
# was never OOM-killed, so it looked healthy throughout. The floor is set above that
# demonstrated failure rather than at it.
# FALSIFIER: a clean settle at or below this value on a non-trivial corpus.
$FLOOR_BYTES = 6GB

# POLICY. Headroom above the fitted requirement. .NET sizes its heap hard limit from the
# cgroup limit and collects harder as it approaches it, so a grant at exactly the
# requirement trades throughput for a cap that is technically sufficient.
$HEADROOM_FRACTION = 0.20

# ---------------------------------------------------------------------------

function Get-CorpusCommit {
    <#
        The commit the measured workspace is sitting on, or $null.

        Recorded next to the count because a count without a commit cannot be checked
        later. "8,621 files" is unverifiable a week afterwards; "8,621 files at
        <sha>, tracked" can be re-derived by anyone.
    #>
    param([string] $Root)

    if (-not (Get-Command git -CommandType Application -ErrorAction SilentlyContinue)) {
        return $null
    }

    $sha = & git -C $Root rev-parse HEAD 2>$null

    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($sha)) {
        return $null
    }

    $dirty = & git -C $Root status --porcelain 2>$null
    $suffix = if ($LASTEXITCODE -eq 0 -and @($dirty).Where({ -not [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) {
        '-dirty'
    }
    else {
        ''
    }

    return (([string] $sha).Trim() + $suffix)
}

function Measure-Corpus {
    <#
        Counts the files under a root that repocontext would plausibly ingest.

        TWO methods, and which one ran is part of the answer (issue #2930).

        `Tracked` asks git for the working tree minus everything .gitignore excludes.
        That is the corpus repocontext ingests, and - this is the point - it is a
        property of the COMMIT, so two checkouts of the same commit agree. The
        directory-name filter alone is a property of the CHECKOUT: it cannot see
        untracked build output under a name not on the list, editor state, tool caches,
        or deploy backups, so the same commit measured in two worktrees returns two
        numbers and derives two different grants. A grant that is not stable across
        runs silently breaks run-to-run comparison, which is the basis the acceptance
        rig scores on.

        `Walk` is the filesystem fallback for a root that is not a git checkout. It is
        the old behaviour, kept because it is better than refusing, and REPORTED
        because a reader must be able to tell that the weaker method ran.

        Returns the raw total, the counted figure, and the method. Reporting only the
        filtered number would hide the exclusion doing its job: the ratio between them
        is the signal that tells an operator the filter is alive, and it is the first
        thing to look at when a derived grant is surprising.
    #>
    param([string] $Root)

    $excludedPattern = ($ExcludedDirectoryNames | ForEach-Object {
        [regex]::Escape([IO.Path]::DirectorySeparatorChar + $_ + [IO.Path]::DirectorySeparatorChar)
    }) -join '|'

    $tracked = Get-TrackedCorpusFile -Root $Root

    if ($null -ne $tracked) {
        $counted = 0

        foreach ($relative in $tracked) {
            $candidate = [IO.Path]::DirectorySeparatorChar +
                ($relative -replace '/', [IO.Path]::DirectorySeparatorChar)

            if ($candidate -notmatch $excludedPattern) {
                $counted++
            }
        }

        return [pscustomobject]@{ Raw = $tracked.Count; Counted = $counted; Method = 'Tracked' }
    }

    $raw = 0
    $counted = 0

    # -ErrorAction Stop, NOT SilentlyContinue. A permission-denied subtree that is
    # swallowed here produces a LOW count, and a low count derives a SMALL grant that
    # the deployment then honours without complaint. The failure mode of the quiet
    # version is a run that is under-provisioned for a reason nothing recorded.
    foreach ($file in (Get-ChildItem -LiteralPath $Root -Recurse -File -Force -ErrorAction Stop)) {
        $raw++

        # Match on the path WITH separators on both sides so a directory called `bin`
        # is excluded while a file called `bin` or a directory called `binaries` is not.
        $relative = [IO.Path]::DirectorySeparatorChar + `
            $file.FullName.Substring($Root.TrimEnd([IO.Path]::DirectorySeparatorChar).Length).TrimStart([IO.Path]::DirectorySeparatorChar)

        if ($relative -notmatch $excludedPattern) {
            $counted++
        }
    }

    return [pscustomobject]@{ Raw = $raw; Counted = $counted; Method = 'Walk' }
}

function Get-TrackedCorpusFile {
    <#
        The tracked-and-not-ignored file list for a git checkout, or $null when the root
        is not one (or git is unavailable, or the call fails).

        $null means "I could not measure this way", and the caller falls back and says
        so. It deliberately does not mean "zero files": an empty result from a real
        repository is returned as an empty array, so a genuinely empty corpus stays
        distinguishable from an unmeasurable one.
    #>
    param([string] $Root)

    if (-not (Get-Command git -CommandType Application -ErrorAction SilentlyContinue)) {
        return $null
    }

    # --cached --others --exclude-standard is the working tree as git sees it: tracked
    # files plus untracked ones that .gitignore does not exclude. Deleted-but-staged
    # entries are filtered by existence below, so a dirty index does not inflate it.
    $output = & git -C $Root ls-files --cached --others --exclude-standard 2>$null

    if ($LASTEXITCODE -ne 0) {
        return $null
    }

    $files = @()

    foreach ($line in @($output)) {
        $relative = ([string] $line).Trim()

        if ([string]::IsNullOrWhiteSpace($relative)) {
            continue
        }

        if (Test-Path -LiteralPath (Join-Path $Root $relative) -PathType Leaf) {
            $files += $relative
        }
    }

    return ,$files
}

function Format-Bytes {
    param([double] $Bytes)
    return '{0:0.##} GiB' -f ($Bytes / 1GB)
}

function Get-HostMemoryBytes {
    # Order matters: $IsWindows does not exist on Windows PowerShell 5.1, and reading an
    # undefined variable under Set-StrictMode throws. The version test must short-circuit
    # first. This is not a style preference; reversing these two clauses breaks 5.1.
    if ($PSVersionTable.PSVersion.Major -le 5 -or $IsWindows) {
        return [double](Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    }

    $line = Select-String -Path '/proc/meminfo' -Pattern '^MemTotal:\s+(\d+) kB'
    if (-not $line) { throw 'Could not read MemTotal from /proc/meminfo.' }
    return [double]$line.Matches[0].Groups[1].Value * 1024
}

function Get-HostAvailableMemoryBytes {
    <#
        Free physical memory RIGHT NOW. This is deliberately NOT what the grant is
        derived from - see the WHICH BASE block below - it is only used to refuse a
        derivation the host cannot honour at this moment.

        Returns $null rather than throwing if the reading is unavailable, because a
        missing availability probe must not break a derivation that is otherwise sound.
        An absent reading skips the check; it never silently passes it.
    #>
    try {
        if ($PSVersionTable.PSVersion.Major -le 5 -or $IsWindows) {
            return [double](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
        }

        $line = Select-String -Path '/proc/meminfo' -Pattern '^MemAvailable:\s+(\d+) kB'
        if (-not $line) { return $null }
        return [double]$line.Matches[0].Groups[1].Value * 1024
    }
    catch {
        return $null
    }
}

function Get-HostCpuCount {
    return [int][Environment]::ProcessorCount
}

$composeDirectory = Split-Path -Parent $PSScriptRoot

if (-not $OutFile) {
    $OutFile = Join-Path $composeDirectory '.env'
}

if (-not $WorkspacePath) {
    # THE REPOSITORY ROOT - this file's grandparent. An earlier revision of this comment
    # claimed it was the repository root's PARENT, emphatically and wrongly, while the
    # code did what it does now. Recorded because the comment was the confident half.
    #
    # This is deliberately NOT the base compose file's REPO_PATH, which on the reference
    # deployment is C:\dev and mounts every sibling checkout and all 215 worktrees at
    # /workspace. The quantity this model is denominated in is the INDEXED corpus - what
    # repocontext actually ingests under its indexed root - which is one repository.
    #
    # If your deployment genuinely indexes several repositories, pass the root that
    # contains them: the count, and therefore the grant, scales with it. The default is
    # the common case, not the only one.
    $WorkspacePath = (Resolve-Path (Join-Path $composeDirectory '..' '..')).Path
}

if (-not (Test-Path $WorkspacePath)) {
    throw "Workspace path '$WorkspacePath' does not exist. Pass -WorkspacePath explicitly."
}

$hostMemory = Get-HostMemoryBytes
$hostCpus = Get-HostCpuCount

Write-Host ''
Write-Host 'HOST' -ForegroundColor Cyan
Write-Host ("  logical CPUs      : {0}" -f $hostCpus)
Write-Host ("  physical memory   : {0}" -f (Format-Bytes $hostMemory))

# ---- Corpus ---------------------------------------------------------------
# Indexed FILE COUNT is the primary proxy, deliberately in preference to the on-disk
# size of the /data volume. On the reference deployment that volume measures 72.68 GB,
# of which about 27.4 GiB is write-ahead log that the defect in #2692 prevents from ever
# being reclaimed. Keying the grant to on-disk bytes would therefore bake a bug into the
# model and over-grant by a factor that shrinks the moment that defect is fixed. File
# count is a property of the repository and is independent of the defect.
Write-Host ''
Write-Host 'CORPUS' -ForegroundColor Cyan
Write-Host ("  scanning {0} ..." -f $WorkspacePath)

$fileCount = 0
$rawFileCount = 0
$corpusMethod = 'Unknown'
try {
    $corpus = Measure-Corpus -Root $WorkspacePath
    $fileCount = $corpus.Counted
    $rawFileCount = $corpus.Raw
    $corpusMethod = $corpus.Method
}
catch {
    throw "Could not enumerate '$WorkspacePath': $($_.Exception.Message)"
}

if ($fileCount -le 0) {
    throw @"
Found no files under '$WorkspacePath', so the corpus requirement cannot be derived.
Deriving a memory grant from a corpus measurement of zero would produce the FLOOR and
present it as a fitted value, which is worse than refusing: it would look derived.
Pass -WorkspacePath explicitly if the default is wrong.
"@
}

$corpusCommit = Get-CorpusCommit -Root $WorkspacePath

Write-Host ("  indexable files   : {0:N0}   (of {1:N0} on disk; {2:N0} excluded as {3})" -f `
    $fileCount, $rawFileCount, ($rawFileCount - $fileCount), ($ExcludedDirectoryNames -join '/'))
Write-Host ("  method            : {0}{1}" -f $corpusMethod, $(
    if ($corpusMethod -eq 'Tracked') { '   (git ls-files, .gitignore honoured)' }
    else { '   [WEAK: not a git checkout, count includes untracked debris]' }))
Write-Host ("  corpus commit     : {0}" -f $(if ($corpusCommit) { $corpusCommit } else { 'unknown' }))

# -ExpectedCorpusFiles is the fail-closed half of issue #2930. Measuring the corpus
# reproducibly is necessary but not sufficient: nothing yet NOTICES when the number
# moves. An acceptance run that re-derives against a corpus of a different size is not
# comparable with its predecessors, and the movement is invisible unless someone
# happens to read two headers side by side. Declaring the expected count turns that
# into a refusal.
#
# The tolerance is fractional rather than exact because the corpus legitimately drifts
# by a few files between commits, and a guard that fires on every commit is one that
# gets passed -Force out of habit.
if ($PSBoundParameters.ContainsKey('ExpectedCorpusFiles')) {
    $drift = [Math]::Abs($fileCount - $ExpectedCorpusFiles)
    $allowed = [Math]::Max(1, [Math]::Ceiling($ExpectedCorpusFiles * $CorpusTolerance))

    if ($drift -gt $allowed) {
        throw @"
Corpus size has moved outside the declared tolerance, so this derivation is NOT
comparable with the runs that preceded it.

  expected  : $('{0:N0}' -f $ExpectedCorpusFiles) files
  measured  : $('{0:N0}' -f $fileCount) files ($corpusMethod)
  drift     : $('{0:N0}' -f $drift) files, tolerance $('{0:N0}' -f $allowed) ($('{0:P1}' -f $CorpusTolerance))
  root      : $WorkspacePath
  commit    : $(if ($corpusCommit) { $corpusCommit } else { 'unknown' })

Either the workspace is not the one the expectation was set against, or the corpus has
genuinely grown. Both are real findings and neither should be absorbed silently: the
memory grant derives from this number, so a run scored against a differently-sized
corpus is being compared on a basis that changed underneath it.

Re-run with -ExpectedCorpusFiles $fileCount once you have decided the new size is the
one you mean, and record why in the run log.
"@
    }
}

if ($CorpusOnly) {
    [pscustomobject]@{
        Root = $WorkspacePath
        Raw = $rawFileCount
        Counted = $fileCount
        Method = $corpusMethod
        Commit = $corpusCommit
        Excluded = $ExcludedDirectoryNames
    } | ConvertTo-Json -Compress | Write-Output
    return
}

# ---- Derivation -----------------------------------------------------------
$corpusRequirement = $FIXED_OVERHEAD_BYTES + ($PER_FILE_BYTES * $fileCount)
$withHeadroom = $corpusRequirement * (1 + $HEADROOM_FRACTION)
$requested = [Math]::Max($withHeadroom, $FLOOR_BYTES)

$hostUsable = $hostMemory * $HOST_MEMORY_SHARE
$repocontextCeiling = $hostUsable - $EMBEDDER_MEMORY_BYTES

Write-Host ''
Write-Host 'DERIVATION' -ForegroundColor Cyan
Write-Host ("  fixed overhead    : {0}   [provisional]" -f (Format-Bytes $FIXED_OVERHEAD_BYTES))
Write-Host ("  per-file term     : {0}   [provisional, one-point fit]" -f (Format-Bytes ($PER_FILE_BYTES * $fileCount)))
Write-Host ("  corpus requirement: {0}" -f (Format-Bytes $corpusRequirement))
Write-Host ("  + {0:P0} headroom   : {1}" -f $HEADROOM_FRACTION, (Format-Bytes $withHeadroom))
Write-Host ("  floor             : {0}   [measured: 4 GiB demonstrably faults]" -f (Format-Bytes $FLOOR_BYTES))
Write-Host ("  requested         : {0}" -f (Format-Bytes $requested))
Write-Host ("  host ceiling      : {0}  ({1:P0} of host, less {2} for the embedder)" -f `
    (Format-Bytes $repocontextCeiling), $HOST_MEMORY_SHARE, (Format-Bytes $EMBEDDER_MEMORY_BYTES))

if ($repocontextCeiling -lt $FLOOR_BYTES) {
    throw @"

REFUSING: this host cannot run this stack.

  floor (repocontext)   : $(Format-Bytes $FLOOR_BYTES)
  embedder needs        : $(Format-Bytes $EMBEDDER_MEMORY_BYTES)
  host physical memory  : $(Format-Bytes $hostMemory)
  usable at $('{0:P0}' -f $HOST_MEMORY_SHARE) share    : $(Format-Bytes $hostUsable)
  leaves for repocontext: $(Format-Bytes $repocontextCeiling)

The floor is not a preference. A 4 GiB cap on this service was about 40% of the known
requirement and produced thousands of OutOfMemoryException lines inside a grain-state
READ path while the container was never killed and `docker ps` reported it healthy.
Starting below the floor does not fail fast; it degrades quietly and looks fine.

Options, in order of preference:
  1. Run this stack on a host with more memory.
  2. Index a smaller workspace (-WorkspacePath), which lowers the requirement.
  3. Raise HOST_MEMORY_SHARE in this script if this box is dedicated to the stack and
     $('{0:P0}' -f $HOST_MEMORY_SHARE) is too conservative for it. That is a judgement about the machine,
     so it belongs to whoever owns the machine - which is why it is not a parameter.
"@
}

$granted = [Math]::Min($requested, $repocontextCeiling)

if ($granted -lt $requested) {
    Write-Host ''
    Write-Warning @"
The host ceiling binds BELOW the corpus requirement.

  corpus needs : $(Format-Bytes $requested)
  host offers  : $(Format-Bytes $repocontextCeiling)
  shortfall    : $(Format-Bytes ($requested - $repocontextCeiling))

The stack is above the floor so it will start, and it may well be fine - the fitted
requirement carries headroom and is a conservative upper bound. But it is running
BELOW its derived requirement, so if you see OutOfMemoryException in grain-state reads
or leaves repeatedly activating cold, this is the first thing to suspect and it will
NOT announce itself as a resource event.
"@
}

# --- WHICH BASE THE GRANT IS TAKEN OF: TOTAL, NOT AVAILABLE ------------------------
#
# This is a real fork and it was raised against this script by measurement, so the
# choice is recorded here rather than left to be inferred. A bucket build failed twice
# with MSBUILD error MSB4166 (child node exited prematurely) while 19.75 GiB of 55.67
# GiB was free, under about ten concurrent worker sessions; rebuilding at -m:2 was
# clean. So a large and highly variable share of host memory is routinely spoken for by
# processes this container knows nothing about.
#
# The grant is derived from TOTAL. Three reasons, in order of weight:
#
#   1. Deriving from AVAILABLE reintroduces exactly the defect this script exists to
#      remove. A grant taken from a transient reading makes the SAME repository on the
#      SAME host derive a different grant according to what else happened to be running
#      at that second. That is host-state-dependence wearing a different hat, and it is
#      the identical objection that rules out counting .git/bin/obj in the corpus.
#
#   2. mem_limit is a CEILING, not a reservation. Docker does not pre-allocate it. A
#      13.58 GiB grant against 19.75 GiB free does not consume 13.58 GiB, so sizing the
#      ceiling to instantaneous free memory conflates a bound with an allocation.
#
#   3. A .env is written once and used for many `docker compose up` invocations. Even a
#      correct availability reading is stale by the next one.
#
# But total alone cannot see a host that is genuinely too busy RIGHT NOW, so
# availability is used as a REFUSAL INPUT rather than a derivation input. That is the
# clamp-with-refusal shape this script already uses, applied to the one condition it
# could not otherwise detect.
#
# THE MESSAGE MATTERS AS MUCH AS THE CHECK. The failure above is the case where a
# misleading diagnostic is at its worst: an operator running ten builds sees a memory
# warning, and there genuinely IS a memory shortage - from a completely unrelated
# cause. A false explanation corroborated by a true symptom is more convincing than a
# false explanation alone, and both obvious responses (raise HOST_MEMORY_SHARE, buy
# RAM) appear to work. So this message names concurrent load as the cause, says the
# host size is NOT the constraint, and does not offer the share as a remedy.
$hostAvailable = Get-HostAvailableMemoryBytes
$totalCommitment = $granted + $EMBEDDER_MEMORY_BYTES

if ($null -ne $hostAvailable -and $totalCommitment -gt $hostAvailable -and -not $IgnoreHostLoad) {
    throw @"
REFUSING: the host does not have enough FREE memory to start this stack right now.

  derived grant   : $(Format-Bytes $granted) (repocontext) + $(Format-Bytes $EMBEDDER_MEMORY_BYTES) (embedder) = $(Format-Bytes $totalCommitment)
  free right now  : $(Format-Bytes $hostAvailable)
  host total      : $(Format-Bytes $hostMemory)

THIS IS NOT A STATEMENT THAT THE HOST IS TOO SMALL. The derivation above already fits
within host total; what it does not fit within is what is free at this moment. The
constraint is CONCURRENT LOAD, not host size, and the derived values are correct.

Do NOT raise HOST_MEMORY_SHARE in response to this message, and do not add RAM. Both
will appear to work, because both make the check pass, and neither addresses why the
memory was unavailable - so the stack will then start into a host that is genuinely
contended and the symptom will reappear as something harder to read.

Do one of:

1. Wait for the host to quiet down and re-run this script. This is almost always the
   right answer. Free memory of $(Format-Bytes $hostAvailable) against a $(Format-Bytes $hostMemory) host means
   something substantial is running - concurrent builds are the usual cause.
2. Stop the competing work, then re-run.
3. Re-run with -IgnoreHostLoad if you know the competing load is about to end and you
   want the .env written now. The grant is a ceiling, not a reservation, so this is
   safe when that is genuinely true - and a lie when it is not.
"@
}

# Round CPU shares UP, matching what .NET itself reports for a fractional limit, so a
# derived pool size never disagrees with the runtime in the unsafe direction.
$repocontextCpus = [Math]::Max(2, [int][Math]::Ceiling($hostCpus * $REPOCONTEXT_CPU_SHARE))
$embedderCpus = [Math]::Max(1, [int][Math]::Ceiling($hostCpus * $EMBEDDER_CPU_SHARE))

if (($repocontextCpus + $embedderCpus) -gt $hostCpus) {
    $repocontextCpus = [Math]::Max(2, $hostCpus - $embedderCpus)
}

# The GC heap count and the replay gate both derive from the SAME number as the CPU
# grant, which is the point: on the reference host these were three independent literals
# (6, 6, and an implied 16) that agreed by hand and could silently stop agreeing.
$gcHeapCount = $repocontextCpus
$maxConcurrentReplays = $repocontextCpus
$embedderIntraThreads = $embedderCpus

Write-Host ''
Write-Host 'RESULT' -ForegroundColor Green
Write-Host ("  repocontext: cpus={0} mem={1} gcHeaps={2} replays={3}" -f `
    $repocontextCpus, (Format-Bytes $granted), $gcHeapCount, $maxConcurrentReplays)
Write-Host ("  embedder   : cpus={0} mem={1} intraThreads={2}" -f `
    $embedderCpus, (Format-Bytes $EMBEDDER_MEMORY_BYTES), $embedderIntraThreads)

$grantedMiB = [int][Math]::Floor($granted / 1MB)
$embedderMiB = [int][Math]::Floor($EMBEDDER_MEMORY_BYTES / 1MB)

# Rendered through the knob table, not interpolated. See the dot-source note at the top
# of this file: only this knob is hex, because only this knob is read by the CLR.
# REPOCONTEXT_MAX_CONCURRENT_REPLAYS goes through our own decimal int.TryParse, so
# emitting 0x there would be a new bug rather than a fix for an old one.
$gcHeapCountKnob = Get-TuningKnob | Where-Object { $_.Name -eq 'REPOCONTEXT_GC_HEAP_COUNT' }

if ($null -eq $gcHeapCountKnob) {
    throw 'REPOCONTEXT_GC_HEAP_COUNT is not in the knob table, so its radix cannot be ' +
        'determined and it would be written in the wrong base (issue #2928).'
}

$gcHeapCountLiteral = Format-TuningKnobValue -Knob $gcHeapCountKnob -Value $gcHeapCount

$content = @"
# GENERATED by scripts/New-TuningEnv.ps1 on $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss').
# Issue #2779. Re-run that script after changing hosts or materially changing the size
# of the indexed workspace. Do NOT copy this file to another machine: every value below
# is derived from THIS host and THIS corpus, and copying it is the exact failure the
# script exists to prevent.
#
# Derived from:
#   host logical CPUs : $hostCpus
#   host memory       : $(Format-Bytes $hostMemory)
#   indexable files   : $fileCount  ($corpusMethod)
#   corpus root       : $WorkspacePath
#   corpus commit     : $(if ($corpusCommit) { $corpusCommit } else { 'unknown' })
#   corpus requirement: $(Format-Bytes $corpusRequirement) (+$('{0:P0}' -f $HEADROOM_FRACTION) headroom, floor $(Format-Bytes $FLOOR_BYTES))
#   measurement cond. : replay gate = CPU grant (see the script's provenance block)
#
# The four corpus lines above are the grant's provenance, and they are recorded because
# the grant DERIVES from the file count: a run measured against a different corpus is
# not comparable with its predecessors, and without these lines that difference leaves
# no trace anywhere (issue #2930). Method 'Tracked' means git ls-files with .gitignore
# honoured, which is a property of the commit and so reproducible in any checkout of it;
# 'Walk' is the weaker filesystem fallback and its count includes untracked debris.
# Re-derive with -ExpectedCorpusFiles $fileCount to make a later movement a refusal.

REPOCONTEXT_CPUS=$repocontextCpus
REPOCONTEXT_MEM_LIMIT=${grantedMiB}m
# Base 16: the CLR reads DOTNET_GCHeapCount as hexadecimal (issue #2928).
REPOCONTEXT_GC_HEAP_COUNT=$gcHeapCountLiteral
REPOCONTEXT_MAX_CONCURRENT_REPLAYS=$maxConcurrentReplays

EMBEDDER_CPUS=$embedderCpus
EMBEDDER_MEM_LIMIT=${embedderMiB}m
EMBEDDER_INTRA_THREADS=$embedderIntraThreads

# NOT DERIVED - you must set this yourself. It has no default because a relative path
# resolves against whatever directory compose was invoked from, which once placed the
# only working backup of durable agent memory inside an ephemeral git worktree (#2627).
# Point it at an ABSOLUTE host path outside every git checkout and worktree.
# REPOCONTEXT_MEMORY_ARCHIVE_PATH=
"@

function Merge-TuningEnvContent {
    <#
        Fold freshly derived content into an existing .env, preserving every key the
        existing file carries that the new content does not.

        THIS IS THE FIX FOR #2929. -Force previously wrote the derived content over the
        top, which discarded REPO_PATH silently. REPO_PATH is the one key whose loss is
        invisible AND consequential: the base compose file has its own default, so the
        stack still starts, still reports healthy, and indexes a different tree. An
        acceptance run taken in that state is void and looks clean.

        The merge is deliberately conservative in one direction only. A key present in
        BOTH files takes the derived value, because that is what re-deriving means. A key
        present only in the existing file is carried across, because this script has no
        basis for deciding it is obsolete - it does not know what it does not derive.

        Pure: takes and returns strings, touches no disk, so it is directly testable.
        Returns the merged text plus the names carried across and replaced, because the
        counts are the operator's evidence that the merge did something rather than
        nothing.
    #>
    param(
        [Parameter(Mandatory)] [AllowEmptyString()] [string] $Existing,
        [Parameter(Mandatory)] [string] $Derived
    )

    $derivedKeys = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase)

    foreach ($line in ($Derived -split "`r?`n")) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=') {
            [void] $derivedKeys.Add($Matches[1])
        }
    }

    $carried = @()
    $carriedLines = @()
    $seen = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase)

    foreach ($line in ($Existing -split "`r?`n")) {
        # Commented-out assignments are NOT carried. A `# FOO=` line is documentation,
        # and the derived content supplies its own; carrying them would accumulate a
        # duplicate comment block on every re-derivation.
        if ($line -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=') {
            continue
        }

        $name = $Matches[1]

        if ($derivedKeys.Contains($name) -or -not $seen.Add($name)) {
            continue
        }

        $carried += $name
        $carriedLines += $line.TrimEnd()
    }

    $replaced = @(foreach ($line in ($Existing -split "`r?`n")) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=' -and $derivedKeys.Contains($Matches[1])) {
            $Matches[1]
        }
    }) | Select-Object -Unique

    $merged = $Derived

    if ($carriedLines.Count -gt 0) {
        $merged = $Derived.TrimEnd() + "`n`n" + @"
# ---------------------------------------------------------------------------
# CARRIED ACROSS from the previous .env by -Force. This script does not derive
# these keys, so it has no basis for dropping them. REPO_PATH in particular is
# load-bearing and silent when absent: the base compose file falls back to its
# own default and mounts a different tree at /workspace, so the container
# indexes the wrong corpus while every layer reports success (issue #2929).
# ---------------------------------------------------------------------------
"@ + "`n" + ($carriedLines -join "`n") + "`n"
    }

    return [pscustomobject]@{
        Content  = $merged
        Carried  = @($carried)
        Replaced = @($replaced)
    }
}

if ($DryRun) {
    Write-Host ''
    Write-Host "--- would write $OutFile ---" -ForegroundColor Yellow
    Write-Host $content
    return
}

if ((Test-Path $OutFile) -and -not $Force) {
    Write-Host ''
    Write-Warning @"
$OutFile already exists and was NOT overwritten.

It may carry hand-set values this script does not derive - REPO_PATH and
REPOCONTEXT_MEMORY_ARCHIVE_PATH in particular, whose loss is how #2627 and #2929
happened. Re-run with -Force to replace the derived keys while carrying those across,
or merge the values above by hand.
"@

    # The EXISTING file is what a deploy would use, so it is the one worth adjudicating.
    # Returning silently here would mean the one path that touches nothing is also the
    # one path that checks nothing, and an operator who ran this script and saw no
    # refusal would reasonably conclude the .env on disk is fine.
    Write-Host ''
    & (Join-Path $PSScriptRoot 'Assert-TuningEnv.ps1') -EnvFile $OutFile
    exit $LASTEXITCODE
}

$carriedNames = @()

if (Test-Path $OutFile) {
    $merge = Merge-TuningEnvContent `
        -Existing ([System.IO.File]::ReadAllText($OutFile)) `
        -Derived $content

    $content = $merge.Content
    $carriedNames = $merge.Carried

    Write-Host ''
    Write-Host ("MERGE: replaced {0} derived key(s), carried across {1}." -f `
        @($merge.Replaced).Count, $(
            if ($carriedNames.Count -eq 0) { 'nothing' }
            else { "$($carriedNames.Count) - $($carriedNames -join ', ')" }
        )) -ForegroundColor Cyan
}

[System.IO.File]::WriteAllText($OutFile, $content)
Write-Host ''
Write-Host "Wrote $OutFile" -ForegroundColor Green

if ($carriedNames -notcontains 'REPOCONTEXT_MEMORY_ARCHIVE_PATH') {
    Write-Host 'Remember to set REPOCONTEXT_MEMORY_ARCHIVE_PATH before `docker compose up`.'
}

# Adjudicate what was just written, rather than trusting that writing it was
# enough. The overlay guards each knob with a compose presence check, which
# cannot inspect a value, so a file this script produced correctly and a file an
# operator later edited down to a retired sentinel are indistinguishable to
# compose (issue #2863). Checking here costs nothing and fails at the point the
# mistake is cheap, instead of tens of minutes into an acceptance run whose
# comparison arm is by then already void.
Write-Host ''
& (Join-Path $PSScriptRoot 'Assert-TuningEnv.ps1') -EnvFile $OutFile
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
