# Default parameters for the isolated cold-start and scale rig.
# Copy to parameters.local.ps1 and edit. The .local.ps1 form is gitignored.
#
# EVERY name below is deliberately distinct from the live RepoContext
# deployment. The rig's isolation guard (Assert-RigIsolation in
# _rig-helpers.ps1) re-derives the same requirements independently and
# REFUSES to start when any of them is violated, so editing this file cannot
# quietly point the rig at a live project, volume, image tag or port. Treat
# the Required* / Forbidden* entries as the contract, not as tuning knobs.

@{
	# ---- Isolation identity (all distinct from the live deployment) ----
	ProjectName             = 'lattice-coldstart'              # compose project; NEVER 'repocontextcontainer'
	MasterVolume            = 'lattice-coldstart-master'       # pristine restore of the backup tarball
	ScaleMasterVolume       = 'lattice-coldstart-scale-master' # pristine synthetic scale corpus (generate-corpus.ps1)
	WorkVolume              = 'lattice-coldstart-work'         # per-run clone of a master
	HfCacheVolume           = 'lattice-coldstart-hf'           # embedder model cache
	HostPort                = 18080                            # NEVER 8080
	McpImage                = 'repocontext-mcp:coldstart-rig'  # additional tag on an existing image
	EmbedderImage           = 'rc-embedder:coldstart-rig'      # additional tag on an existing image

	# ---- Tag sources ----
	# A cohort NEVER builds: it applies its own additional tag to an
	# already-built image, so no live tag is ever moved or rebuilt by a run. A
	# live tag is legal HERE (it is only ever read); the guard forbids it as a
	# destination. To measure code that is not what is deployed, build it with
	# `rig.ps1 build`, which records its own image as the source below.
	SourceMcpImage          = 'repocontext-mcp:local'
	SourceEmbedderImage     = 'repocontextcontainer-embedder:latest'

	# ---- Build tag (rig.ps1 build) ----
	# `rig.ps1 build` is the ONE rig operation that creates an image, so it is
	# the one place a live tag could be moved. Its destination must carry this
	# prefix, must not be any ForbiddenImages entry, and must not be the tag
	# the rig runs (Assert-RigBuildImage enforces all three). It exists so an
	# operator testing a branch never has to reach for the deploy script, which
	# promotes to production as a side effect.
	BuildImageTagPrefix     = 'coldstart-'

	# Behind a private / corporate-proxy / offline NuGet feed, point the build's
	# restore at your own NuGet.Config. Leave empty to use the SDK default
	# (public nuget.org). $env:NUGET_CONFIG_FILE overrides this, and
	# -NuGetConfigFile overrides both, so the same environment variable the
	# local-dev reference architecture uses works here unchanged. It is passed
	# as a BuildKit secret and never written into an image layer.
	NuGetConfigFile         = ''

	# ---- Durable state under test ----
	# The tarball lives in the MAIN checkout (.deploy/ is untracked and is
	# never committed). Override with -BackupTarball, or in
	# parameters.local.ps1, to point at your own backup.
	BackupTarball           = 'C:\dev\lattice\.deploy\volume-backup-2026-08-29T1000.tar'

	# ---- Workload ----
	RepoId                  = 'lattice'                        # repo id inside the restored corpus
	# Mounted READ-ONLY at /workspace. It must contain a directory named after
	# the repo id above, so the restored corpus's registered repository path
	# (/workspace/<repoId>) still resolves and the box is not measured while
	# reacting to a vanished workspace.
	WorkspaceRoot           = 'C:\dev'
	SemanticQuery           = 'where is the readiness health probe wired'
	WarmQueryCount          = 5                                 # warm samples after the first success
	# After the first successful answer, keep re-asking for up to this long to
	# find out whether the SEMANTIC path ever answers. It matters because
	# repocontext_search falls back to keyword recall when the semantic path
	# throws (for example when the exact-kNN prefix scan exceeds the Orleans
	# response timeout on a cold tree), and a rig that only recorded "a query
	# succeeded" would hide exactly the degradation this epic is about.
	SemanticRetryBudgetSec  = 300
	# Before a scenario hands over to the next one, keep sampling until the box
	# answers consistently fast. Without this, a run whose warm samples were
	# still competing with ongoing leaf activation performs its graceful stop
	# from a materially different state than a run that had settled, and that
	# difference lands in the NEXT scenario's headline rather than in this one.
	QuiesceThresholdMs      = 2000
	QuiesceSamples          = 2
	QuiesceTimeoutSec       = 300
	LiveTimeoutSec          = 300                               # cap on the wait for /health/live
	ReadyTimeoutSec         = 900                               # cap on the wait for /health/ready
	QueryTimeoutSec         = 900                               # cap on the wait for the first semantic query
	ProbeIntervalMs         = 250                               # health / query poll cadence
	SampleIntervalMs        = 1000                              # docker stats sampling cadence
	GracefulStopTimeoutSec  = 180                               # docker compose stop -t for the graceful scenario
	StartupSettleSec        = 2                                 # settle after `up` before the first probe

	# ---- Isolation contract (enforced by Assert-RigIsolation) ----
	RequiredProjectPrefix   = 'lattice-coldstart'
	RequiredVolumePrefix    = 'lattice-coldstart'
	RequiredImageTag        = 'coldstart-rig'
	ForbiddenProjects       = @('repocontextcontainer')
	ForbiddenVolumePrefixes = @('repocontextcontainer_')
	ForbiddenVolumes        = @('repocontextcontainer_repocontext-data', 'repocontextcontainer_hf-cache')
	ForbiddenImages         = @('repocontext-mcp:local', 'repocontextcontainer-repocontext:latest', 'repocontextcontainer-embedder:latest')
	ForbiddenPorts          = @(8080)
}
