# Default parameters for the isolated registry fan-in rig.
# Copy to parameters.local.ps1 and edit. The .local.ps1 form is gitignored.
#
# EVERY name below is deliberately distinct from the live RepoContext
# deployment. The rig's isolation guard (Assert-FanInIsolation in
# _fanin-helpers.ps1) re-derives the same requirements independently and
# REFUSES to start when any of them is violated, so editing this file cannot
# quietly point the rig at the protected container, its volume, its image tag
# or its port. Treat the Required* / Forbidden* entries as the contract, not as
# tuning knobs.

@{
	# ---- Isolation identity (all distinct from the live deployment) ----
	ProjectName             = 'lattice-registry-fanin'         # compose project; NEVER 'repocontextcontainer'
	WorkVolume              = 'lattice-registry-fanin-work'    # per-run durable state; starts EMPTY
	HfCacheVolume           = 'lattice-registry-fanin-hf'      # embedder model cache
	HostPort                = 18081                            # NEVER 8080, and NEVER the cold-start rig's 18080
	McpImage                = 'repocontext-mcp:fanin-rig'      # built from THIS branch by rig.ps1 build
	EmbedderImage           = 'rc-embedder:fanin-rig'          # additional tag on an existing image
	DriverImage             = 'repocontext-fanin-driver:rig'   # the sidecar, built by build-driver.ps1

	# ---- Tag sources ----
	# The embedder is only ever RE-TAGGED, never rebuilt: the rig applies its
	# own additional tag to an image that already exists. A live tag is legal
	# here (it is only ever read); the guard forbids it as a DESTINATION.
	SourceEmbedderImage     = 'repocontextcontainer-embedder:latest'

	# ---- Build ----
	# Unlike the cold-start rig, this rig MUST build its own silo image, and
	# that is not an optimisation. Deliverable 2's instrument has to be in the
	# RUNNING binary or the measurement is vacuous: an absent
	# orleans_lattice_registry_call_duration series would then be indis-
	# tinguishable from "the registry served nothing", which is precisely the
	# hypothesis under test. Running a pre-built live tag would make the
	# headline reading unfalsifiable.
	BuildImageTagPrefix     = 'fanin-'
	NuGetConfigFile         = ''   # empty => $env:APPDATA\NuGet\NuGet.Config if present, else SDK default

	# ---- Workload ----
	# Mounted READ-ONLY at /workspace. The fan-in rig does not index anything -
	# it creates its trees directly through ILatticeRegistry - but the host
	# still wants a workspace root to exist.
	WorkspaceRoot           = 'C:\dev'
	TreePrefix              = 'fanin_'      # the tree-id prefix the driver owns
	GatewayPort             = 30000         # silo gateway, inside the container's netns
	# The repocontext host's own defaults (RepoContextHostConfiguration.cs), not
	# Orleans' conventional 'dev'. A mismatch is refused at the handshake AFTER
	# the TCP connection succeeds, so it presents as a networking fault.
	ClusterId               = 'repo-context'
	ServiceId               = 'repo-context'

	# ---- Protocol ----
	# Phase 1 breadth arm. K counts the trees the DRIVER creates; the host
	# brings up its own fixed per-concern set on top, and seed-trees.ps1
	# records both numbers so the reported K is the real estate size rather
	# than the driver's switch value.
	BreadthK                = @(20, 40, 80)
	Replicates              = 3             # n >= 3 per cell; the spec's floor, not a target
	MeasureWindowSec        = 900           # 15 minutes from ready

	# Phase 1 DEPTH arm. Leaf count is a controlled axis in its own right, held
	# at a low fixed value while K varies and varied at fixed K = 20, and only
	# crossed once each has been read separately.
	#
	# Two candidate mechanisms fit the observed storm equally well and they
	# scale with DIFFERENT quantities: registry fan-in scales with tree count,
	# while storage starvation during cold-start snapshot replay scales with
	# leaf count and store size. A rig that varied only K could not tell them
	# apart - and on an empty estate would show no storm at any K and report a
	# clean scaling law that was purely an artefact of having no data. That
	# false green is the specific failure this axis exists to prevent.
	BreadthLeavesPerTree    = 8             # the low fixed depth the breadth arm holds
	DepthK                  = 20
	DepthLeavesPerTree      = @(8, 64, 256)
	# 1,600 B x the 128-key leaf capacity reproduces the live estate's ~200 KB
	# leaf snapshot, which is the unit cold-start replay actually reads.
	ValueBytes              = 1600
	KeysPerLeaf             = 128
	PopulateBatchSize       = 256
	PopulateParallelism     = 8
	# The ceiling exists because the depth axis writes real bytes: K trees x
	# leaves x 128 keys x 1,600 B. At K=20 and 256 leaves that is ~1.05 GiB,
	# and the live estate's 18,124 leaves would be ~3.5 GiB. Report the highest
	# point actually reached rather than quietly testing a small estate.
	MaxPopulateBytes        = 2GB
	# Contention arm (a): load on 16 of 20 trees across a cold start.
	ContentionK             = 20
	ContentionLoadTrees     = 16
	ContentionRate          = 50            # operations/second across the loaded subset
	# Contention arm (b): sustained load, NO restart, to test whether steady-
	# state saturation happens at all. This arm tests the FRAMING of the whole
	# criterion: the current characterisation is "saturates at cold start only,
	# healthy thereafter", and if a loaded estate saturates at steady state
	# that characterisation is false.
	SteadyStateSec          = 1800          # 30 minutes
	ReadyTimeoutSec         = 900
	ProbeIntervalMs         = 250
	StartupSettleSec        = 2
	GracefulStopTimeoutSec  = 180

	# ---- Isolation contract (enforced by Assert-FanInIsolation) ----
	RequiredProjectPrefix   = 'lattice-registry-fanin'
	RequiredVolumePrefix    = 'lattice-registry-fanin'
	RequiredImageTag        = 'fanin-rig'
	ForbiddenProjects       = @('repocontextcontainer', 'lattice-coldstart')
	ForbiddenVolumePrefixes = @('repocontextcontainer_')
	ForbiddenVolumes        = @('repocontextcontainer_repocontext-data', 'repocontextcontainer_hf-cache')
	ForbiddenImages         = @('repocontext-mcp:local', 'repocontextcontainer-repocontext:latest', 'repocontextcontainer-embedder:latest')
	ForbiddenPorts          = @(8080, 18080)
	# Containers the rig must never address. The guard checks every container
	# id / name it is handed against this list BEFORE the id reaches a docker
	# verb, because the sidecar is run with `--network container:<id>` and a
	# wrong id there would attach the driver to the protected deployment's
	# network namespace and drive synthetic load straight at it.
	ForbiddenContainerNames = @('repocontextcontainer-repocontext-1', 'repocontextcontainer-repocontext', 'repocontextcontainer-embedder-1')
}
