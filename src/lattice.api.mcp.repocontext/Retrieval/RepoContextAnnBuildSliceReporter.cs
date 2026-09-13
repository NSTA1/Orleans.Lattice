using System.Diagnostics.Metrics;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What one approximate-index build step did to the build it advances. The values
/// are exhaustive over a step that was taken - four outcomes for a step that
/// completed, plus one for a step that threw - which is what lets them be counted
/// as a partition rather than as unrelated tallies.
/// </summary>
internal enum RepoContextAnnBuildSliceOutcome
{
    /// <summary>
    /// The step banked progress: at least one more vector indexed, or at least one
    /// more partition resolved. This is the only arm that claims the build got
    /// closer to serving, and it is the one that makes the others interpretable.
    /// </summary>
    Advanced = 0,

    /// <summary>
    /// The step's ingest slice was stopped by its wall-clock budget having banked
    /// nothing, so the cursor did not move. The source did not deliver a single
    /// item before the budget was spent, which no larger budget repairs.
    /// </summary>
    Starved = 1,

    /// <summary>
    /// The step completed having changed nothing the build reports: no vector
    /// banked, no partition resolved, no phase moved, and no slice deadlined
    /// empty-handed. A converged coordinator re-opening its in-memory handle takes
    /// this arm, and so does a step that did nothing for any reason other than a
    /// spent deadline. It is distinct from <see cref="Churned"/>, which did move the
    /// phase: this arm says the step was inert, that one says it was busy.
    /// </summary>
    Idle = 2,

    /// <summary>
    /// The step did not complete: the build threw. The store-of-record read could
    /// not be served at all, which is a different condition from a read that was
    /// served and returned nothing, and it needs a different remedy - the
    /// projection or the store behind it, rather than the access gate or the
    /// embedding throughput that would explain an empty corpus.
    /// </summary>
    Faulted = 3,

    /// <summary>
    /// The step moved the build's phase and banked nothing: no further vector was
    /// indexed and no further partition was resolved. The step unambiguously did
    /// something, so it is not idle - but it got no closer to a servable index, so
    /// it is not progress either.
    /// <para>
    /// This is the arm that makes a livelock visible. A build oscillating between
    /// phases - the <c>Training -&gt; Persisting -&gt; Training</c> cycle recorded in
    /// issue #2791 - emits a step on this arm every time round, so a rising
    /// <c>churned</c> beside a flat <c>advanced</c> names a build that is running
    /// hard and getting nowhere. Folded into <see cref="Advanced"/>, as it was
    /// before issue #2818, that same livelock was indistinguishable from a build
    /// making real progress.
    /// </para>
    /// </summary>
    Churned = 4,
}

/// <summary>
/// Why an approximate-index build step threw. Resolved at the fault site rather
/// than inferred by the reporter, because the site is the only place that knows
/// what the step was doing when it failed.
/// <para>
/// <b>What this vocabulary is for, and what it deliberately is not.</b> It says
/// <i>why</i> a step threw, and therefore who owns the remedy. It does not say
/// <i>which tree</i> could not be read, and reading it as though it did is the
/// specific misattribution issue #2880 records: during run 12 of epic #2368 a
/// scorer was about to blame <c>repo-context-vector-index</c> on circumstantial
/// grounds while the exception named a different tree, and only the container log
/// separated them. The fault location is a cardinality question of its own and
/// rides the accompanying log line, exactly as the repository id does.
/// </para>
/// </summary>
internal enum RepoContextAnnBuildFaultCause
{
    /// <summary>
    /// Something outside the four classified causes. The only value that should
    /// page: it means a step faulted in a way nobody has classified, so the
    /// vocabulary itself is behind the code. Every unrecognised type fails open
    /// onto this arm rather than onto one with a benign explanation.
    /// </summary>
    Unexpected = 0,

    /// <summary>
    /// A <see cref="ScanPageStalledException"/>: the leaf chain behind the corpus
    /// could not be walked inside the per-call stall ceiling. The remedy is the
    /// tree's leaf geometry - an oversized leaf that cannot be materialised in one
    /// grain call - and NOT the slice budget, which no larger value repairs.
    /// Classified ahead of <see cref="DependencyUnavailable"/> because the
    /// exception derives from <see cref="TimeoutException"/> and would otherwise be
    /// swallowed into it, losing exactly the distinction that makes it actionable.
    /// </summary>
    ScanPageStalled = 1,

    /// <summary>
    /// A <see cref="LeafProjectionStaleException"/>: a durable projection
    /// checkpoint has fallen off the write-ahead log with no covering snapshot, so
    /// the read cannot be served at all and will not clear on retry. This is the
    /// condition issue #2737 measured on the acceptance rig, where the designed
    /// self-heal was itself refused by the access gate, and it needs an
    /// operator-driven rebuild rather than patience.
    /// </summary>
    ProjectionStale = 2,

    /// <summary>
    /// A dependency could not be reached: a grain call timed out, the transport
    /// failed, or the cluster rejected the message. Expected to clear once the
    /// cluster settles, which is what separates it from
    /// <see cref="PlaneRejected"/>. This is the arm the run-12 census landed on -
    /// 39 of 39 faults were a <see cref="TimeoutException"/> on a leaf read - so a
    /// deployment seeing it rise is looking at reachability, not at the build.
    /// </summary>
    DependencyUnavailable = 3,

    /// <summary>
    /// The plane refused the work: the embedding space did not match, or an
    /// argument the build supplied was not acceptable. Deterministic, so it will
    /// not clear on retry and the build is wrongly configured rather than unlucky.
    /// </summary>
    PlaneRejected = 4,
}

/// <summary>
/// The faulted step total decomposed by cause, cumulative since process start.
/// </summary>
/// <param name="Unexpected">Faults nobody has classified. The arm that pages.</param>
/// <param name="ScanPageStalled">Faults whose leaf chain could not be walked in time.</param>
/// <param name="ProjectionStale">Faults whose projection checkpoint is unrecoverable.</param>
/// <param name="DependencyUnavailable">Faults that could not reach a dependency.</param>
/// <param name="PlaneRejected">Faults the plane refused deterministically.</param>
internal readonly record struct RepoContextAnnBuildFaultTally(
    long Unexpected,
    long ScanPageStalled,
    long ProjectionStale,
    long DependencyUnavailable,
    long PlaneRejected)
{
    /// <summary>Every fault counted, across all five causes.</summary>
    public long Total
        => Unexpected + ScanPageStalled + ProjectionStale + DependencyUnavailable + PlaneRejected;
}

/// <summary>
/// The faulted step total decomposed by the phase the step was in, cumulative
/// since process start. The read-back that makes the <c>phase</c> tag falsifiable:
/// it is maintained independently of the meter, so a test can assert that the two
/// agree rather than trusting that the tag was passed at all.
/// </summary>
/// <param name="Coordinating">Faults on a tick that never reached the build step.</param>
/// <param name="Opening">Faults opening or restoring the durable index.</param>
/// <param name="Ingesting">Faults reading the ingest corpus. The DoD-1b read side.</param>
/// <param name="Training">Faults training the partitioning in memory.</param>
/// <param name="Persisting">Faults writing the trained index. The DoD-1b write side.</param>
/// <param name="Reconciling">Faults catching a serving index up with the store of record.</param>
internal readonly record struct RepoContextAnnBuildPhaseTally(
    long Coordinating,
    long Opening,
    long Ingesting,
    long Training,
    long Persisting,
    long Reconciling)
{
    /// <summary>Every fault counted, across all six phases.</summary>
    public long Total
        => Coordinating + Opening + Ingesting + Training + Persisting + Reconciling;
}

/// <summary>
/// A point-in-time reading of the build-slice counters, cumulative since process
/// start.
/// </summary>
/// <param name="Advanced">Steps that banked a vector or resolved a partition.</param>
/// <param name="Starved">Steps whose slice was deadlined having banked nothing.</param>
/// <param name="Idle">Steps that changed nothing the build reports.</param>
/// <param name="Faulted">Steps that threw rather than completing.</param>
/// <param name="Churned">Steps that moved the phase while banking nothing.</param>
/// <param name="FaultedByCause">
/// The faulted total decomposed by cause. Sums to <paramref name="Faulted"/>.
/// </param>
/// <param name="FaultedByPhase">
/// The faulted total decomposed by the phase the step was in. Sums to
/// <paramref name="Faulted"/> as well, and independently of
/// <paramref name="FaultedByCause"/>: the two are orthogonal decompositions of the
/// same population, which is why a fault can be a timeout AND be on the ingest
/// read.
/// </param>
internal readonly record struct RepoContextAnnBuildSliceSnapshot(
    long Advanced,
    long Starved,
    long Idle,
    long Faulted,
    long Churned,
    RepoContextAnnBuildFaultTally FaultedByCause,
    RepoContextAnnBuildPhaseTally FaultedByPhase)
{
    /// <summary>
    /// Every step counted, across all five arms. Non-zero exactly when the build
    /// coordinator has taken at least one step in this process, which is the fact
    /// no other series in the approximate-index family can report.
    /// <para>
    /// Read <i>step</i> here as <i>tick that did work or threw</i>, not as
    /// <i>build step that started</i>. Since the fault seam was widened to the
    /// whole tick, <see cref="Faulted"/> counts a tick that threw anywhere on it,
    /// including at the three call sites that run before the build step is reached
    /// at all. That is deliberate: a tick that dies before stepping is exactly as
    /// much a stalled build as one that dies inside the step, and metering only
    /// the latter left the former silent in every series.
    /// </para>
    /// </summary>
    public long Total => Advanced + Starved + Idle + Faulted + Churned;
}

/// <summary>
/// Meters what the approximate-index build coordinator does on each step, so a
/// build that is running and banking nothing can be told apart from a build that
/// is not running at all.
/// <para>
/// <b>Why this exists.</b> Every other instrument in the approximate-index family
/// fires only at a <i>terminal</i> moment.
/// <see cref="RepoContextAnnBuildCorpusReporter"/> records a build that reached
/// <c>Ready</c>; <see cref="RepoContextAnnPartitioningReporter"/> records a plane
/// that has finished building; <see cref="RepoContextAnnIndexSweepReporter"/>
/// records the sweep that <i>arms</i> a coordinator and then says nothing more
/// about it. Between arming and <c>Ready</c> the plane emitted no series at all,
/// so a coordinator grinding through slices that bank nothing and a coordinator
/// that never took a step produced byte-identical telemetry: every arm of every
/// counter sitting at its primed zero.
/// </para>
/// <para>
/// <b>That ambiguity is not hypothetical, and it blocked a real diagnosis.</b> On
/// the acceptance rig the approximate plane reported
/// <c>ann.sweep{outcome=armed} = 1</c> beside <c>ann.build.corpus = 0</c> on every
/// one of its five coverage arms and <c>ann.build.denial_terminal = 0</c>, while
/// retrieval sat permanently in <c>bootstrapping</c>. Those readings are exactly
/// as consistent with a build consuming nothing as with a build that never ran,
/// and no series anywhere could separate them - so the investigation could not
/// even establish which defect it was looking at. This counter separates them:
/// its total advances on every completed step, so a zero total beside an armed
/// sweep is a coordinator that is not stepping, and a rising
/// <c>progress=starved</c> or <c>progress=churned</c> arm is a coordinator that is
/// stepping and getting nowhere.
/// </para>
/// <para>
/// <b>Only <c>advanced</c> claims progress, and that is the point of the
/// partition.</b> A step that moves the build's phase without banking a vector or
/// resolving a partition is counted on <c>churned</c>, not on <c>advanced</c>.
/// Before issue #2818 the two were one arm, so the
/// <c>Training -&gt; Persisting -&gt; Training</c> oscillation recorded in issue
/// #2791 emitted a steadily rising <c>advanced</c> reading while banking nothing,
/// and the series advertised as the progress discriminator could not discriminate.
/// A healthy build churns at most once per phase transition, so its churn arm is
/// bounded by the phase count; read <c>churned</c> rising WITHOUT BOUND beside
/// <c>advanced</c> flat as a livelocked build.
/// </para>
/// <para>
/// <b>Why a series and not the log line that already existed.</b> The coordinator
/// already computes this discriminator -
/// <see cref="VectorIndexBuildProgress.IsStarvedBySource"/>, which its own
/// documentation calls "the discriminator this build is otherwise missing" - and
/// writes it to a warning log. This bucket has direct evidence that a warning log
/// documenting a blindness was ignored for three hours while a counter on the
/// dashboard read success, which is the same argument
/// <see cref="RepoContextAnnBuildCorpusReporter"/> settled on: prose that
/// contradicts a metric loses every time when only one of the two is being
/// watched, so the remedy has to be a series. The log line is kept - it names the
/// repository and the space, at a cardinality a log affords and a metric backend
/// does not - and this counter carries the same fact where it will be seen.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> Five tag keys: the closed <c>progress</c>
/// partition, the closed <c>phase</c> partition, the <c>cause</c> partition on the
/// faulted arm only, and the <c>repository</c> and <c>space</c> that name the plane
/// the step belongs to. No key, no path, no corpus content.
/// <para>
/// <b>The repository and space are a reversal, and it is deliberate.</b> This
/// reporter previously emitted neither, on the argument that they ride the
/// accompanying log line "at a cardinality a log affords and a metric backend does
/// not". That argument was made against a TENANT dimension - see
/// <see cref="RepoContextAnnIndexBuildGrain.MetricsTreeId"/>, which declines to
/// emit a tenant tag because every repository's vectors share one tree, so
/// <c>LatticeTenantLabel.ForTree</c> would resolve to one constant for every plane
/// on the host. That reasoning is intact and is the reason the tenant tag is still
/// the platform sentinel. It does not carry over to a direct dimension: a plane is
/// keyed by repository and embedding space, one durable index exists per pair, and
/// a coordinator grain is activated per pair, so the number of series here is the
/// number of approximate indexes this host is building. That is operator-chosen
/// and small - it is bounded by how many repositories were onboarded, not by the
/// data inside any of them - which is the distinction that separates it from the
/// per-leaf-grain cardinality issue #2518 records. The measurement that forced the
/// reversal is in issue #2855: on a heterogeneous corpus one division of sixteen
/// succeeded, and with no repository dimension the fifteen failures and the one
/// success were the same series.
/// </para>
/// </para>
/// <para>
/// <b>The <c>phase</c> tag is the one that makes a fault attributable.</b> Without
/// it a faulted read of the ingest corpus and a faulted write in the trained
/// index's persist are the same series value, and they imply opposite conclusions:
/// a read fault is an independent defect of the projection behind the corpus,
/// while a persist fault writes into the very tree a corpus-read defect would
/// already have named, so scoring the two separately counts one defect twice. See
/// <see cref="RepoContextAnnBuildStepPhase"/>. The phase is resolved where the step
/// runs - and REFINED at the fault site, so a step that trains and then persists in
/// one call is attributed to whichever half actually threw.
/// </para>
/// <para>
/// <b>All arms are pre-minted, per plane.</b> Every series is created with a
/// zero-valued add the first time a plane is seen, so on a correctly configured
/// host <c>progress=starved</c> is present for that plane and reads <c>0</c> rather
/// than being absent. A zero-primed family is a measurement rather than silence -
/// but note that it is then the <i>total</i>, not any single arm, that
/// distinguishes "the coordinator has not stepped" from "the coordinator has
/// stepped and nothing went wrong".
/// <para>
/// Priming moved from the constructor to <see cref="EnsurePrimed"/> when the
/// repository and space dimensions were added, because a constructor cannot know
/// which planes exist and a primed series for a plane that does not exist would be
/// a claim about a build nobody asked for. The coordinator calls
/// <see cref="EnsurePrimed"/> at the top of its tick, ABOVE every early return and
/// inside its fault seam, so a plane whose every tick dies before stepping still
/// reports zeros on all its arms rather than nothing at all - which is the
/// unprimed-arm defect issue #2952 records for a sibling instrument.
/// </para>
/// </para>
/// </summary>
internal sealed class RepoContextAnnBuildSliceReporter : IDisposable
{
    /// <summary>
    /// The counter of completed approximate-index build steps, partitioned by what
    /// the step did to the build.
    /// </summary>
    internal const string SliceInstrumentName = "repocontext.ann.build.slice";

    /// <summary>The tag key carrying the progress partition.</summary>
    internal const string ProgressTagKey = "progress";

    /// <summary>The tag value for a step that moved the build on.</summary>
    internal const string ProgressAdvancedTag = "advanced";

    /// <summary>The tag value for a step whose slice was deadlined having banked nothing.</summary>
    internal const string ProgressStarvedTag = "starved";

    /// <summary>The tag value for a step that changed nothing the build reports.</summary>
    internal const string ProgressIdleTag = "idle";

    /// <summary>The tag value for a step that threw rather than completing.</summary>
    internal const string ProgressFaultedTag = "faulted";

    /// <summary>The tag value for a step that moved the phase while banking nothing.</summary>
    internal const string ProgressChurnedTag = "churned";

    /// <summary>
    /// The tag key carrying the fault cause. Emitted on the faulted arm only, so
    /// the four completing arms keep exactly the cardinality they had before this
    /// dimension existed and a query selecting <c>progress=faulted</c> still
    /// aggregates across every cause.
    /// </summary>
    internal const string CauseTagKey = "cause";

    /// <summary>The tag value for a fault with no recognised cause.</summary>
    internal const string CauseUnexpectedTag = "unexpected";

    /// <summary>The tag value for a leaf chain that could not be walked in time.</summary>
    internal const string CauseScanPageStalledTag = "scan-page-stalled";

    /// <summary>The tag value for an unrecoverable durable projection checkpoint.</summary>
    internal const string CauseProjectionStaleTag = "projection-stale";

    /// <summary>The tag value for a dependency that could not be reached.</summary>
    internal const string CauseDependencyUnavailableTag = "dependency-unavailable";

    /// <summary>The tag value for work the plane refused deterministically.</summary>
    internal const string CausePlaneRejectedTag = "plane-rejected";

    /// <summary>
    /// The tag key naming the repository whose approximate index the step was
    /// building. Bounded by the number of repositories onboarded on this host, one
    /// durable index and one coordinator grain per (repository, space) pair.
    /// </summary>
    internal const string RepositoryTagKey = "repository";

    /// <summary>
    /// The tag key naming the embedding space the index is built in, rendered as
    /// <c>{modelId}/{dimension}</c>. Bounded by the number of spaces configured,
    /// which is one on every shipped configuration.
    /// </summary>
    internal const string SpaceTagKey = "space";

    /// <summary>The tag value for a space no plane has stamped yet.</summary>
    internal const string SpaceUnspecifiedTag = "unspecified";

    /// <summary>
    /// The tag key carrying the phase partition: where in the tick the step was
    /// when it was counted. Emitted on every arm, faulted and completing alike, so
    /// it partitions the whole population rather than only the faults - which is
    /// what makes each of its zeros a measurement.
    /// </summary>
    internal const string PhaseTagKey = "phase";

    /// <summary>The tag value for a tick that never reached the build step.</summary>
    internal const string PhaseCoordinatingTag = "coordinating";

    /// <summary>The tag value for opening or restoring the durable index.</summary>
    internal const string PhaseOpeningTag = "opening";

    /// <summary>The tag value for reading the ingest corpus. The DoD-1b read side.</summary>
    internal const string PhaseIngestingTag = "ingesting";

    /// <summary>The tag value for training the partitioning in memory.</summary>
    internal const string PhaseTrainingTag = "training";

    /// <summary>The tag value for writing the trained index. The DoD-1b write side.</summary>
    internal const string PhasePersistingTag = "persisting";

    /// <summary>The tag value for catching a serving index up with the store of record.</summary>
    internal const string PhaseReconcilingTag = "reconciling";

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than publishing
    // an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _slices;

    private readonly Lock _gate = new();
    private readonly Dictionary<(string RepoId, EmbeddingSpaceTag Space), PlaneTags> _planes = new();
    private long _advanced;
    private long _starved;
    private long _idle;
    private long _faulted;
    private long _churned;
    private long _faultedUnexpected;
    private long _faultedScanPageStalled;
    private long _faultedProjectionStale;
    private long _faultedDependencyUnavailable;
    private long _faultedPlaneRejected;
    private long _faultedCoordinating;
    private long _faultedOpening;
    private long _faultedIngesting;
    private long _faultedTraining;
    private long _faultedPersisting;
    private long _faultedReconciling;

    /// <summary>Creates the reporter, its instrument, and every one of its series.</summary>
    public RepoContextAnnBuildSliceReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _slices = _meter.CreateCounter<long>(
            SliceInstrumentName,
            unit: "{step}",
            description:
                "Approximate-index build steps that completed, partitioned by what the step did to the build: "
                + "'advanced' (it banked at least one more vector or resolved at least one more partition, so the "
                + "build got closer to serving), 'churned' (it moved the build's phase while banking no vector and "
                + "resolving no partition - a healthy build churns at most once per phase transition, so this arm "
                + "is bounded by the phase count on a build that completes, and an arm rising WITHOUT BOUND beside "
                + "a flat 'advanced' is a build oscillating between phases and banking nothing; it is counted apart "
                + "from 'advanced' precisely so that livelock cannot read as progress), "
                + "'starved' (its ingest slice was stopped by the wall-clock budget having banked nothing, so the "
                + "cursor did not move and no larger budget repairs it), or 'idle' (it neither advanced the build "
                + "nor moved its phase nor was deadlined empty-handed, which is what a converged coordinator "
                + "re-opening its in-memory "
                + "handle does), or 'faulted' (the step threw rather than completing, so the store-of-record read "
                + "could not be served at all - which needs the projection or the store behind it looked at, rather "
                + "than the access gate or the embedding throughput that would explain an empty corpus). Every "
                + "other instrument on this plane fires only at a terminal moment - a build "
                + "that reached Ready, a plane that finished building, a sweep that armed a coordinator - so "
                + "between arming and Ready the plane emitted nothing, and a build consuming nothing was "
                + "byte-identical in telemetry to a build that never ran. The TOTAL across all five arms is the "
                + "figure that separates them: zero beside a non-zero 'ann.sweep{outcome=armed}' means the "
                + "coordinator is not stepping, while a rising 'starved' arm means it is stepping and getting "
                + "nowhere, a rising 'churned' arm means it is stepping, moving, and banking nothing, and a rising "
                + "'faulted' arm means it is stepping and throwing. The 'faulted' arm ALONE carries a second tag, "
                + "'cause', drawn from a closed set resolved where the fault is raised: 'scan-page-stalled' (the "
                + "leaf chain behind the corpus could not be walked inside the per-call stall ceiling, which is a "
                + "leaf-geometry problem and which no larger slice budget repairs), 'projection-stale' (a durable "
                + "projection checkpoint has fallen off the write-ahead log with no covering snapshot, so the read "
                + "will not clear on retry and needs an operator-driven rebuild), 'dependency-unavailable' (a grain "
                + "call timed out, the transport failed, or the cluster rejected the message, which is expected to "
                + "clear once the cluster settles), 'plane-rejected' (the embedding space did not match or an "
                + "argument was refused, which is deterministic and will not clear on retry), or 'unexpected' "
                + "(unclassified - the only value that should page). The cause values are deliberately NOT "
                + "pre-minted: they partition 'faulted' rather than the whole population, so a zero on a cause is "
                + "uninterpretable until 'faulted' is itself non-zero, at which point the faults have minted their "
                + "own causes. The cause says WHY a step threw and therefore who owns the remedy; it deliberately "
                + "does NOT say WHICH TREE could not be read, which rides the accompanying log line - do not infer "
                + "a tree from it (issue #2880). Two further tags name the plane the step belongs to: "
                + "'repository' and 'space' ({modelId}/{dimension}). One durable index and one coordinator "
                + "grain exist per (repository, space) pair, so the series count here is the number of "
                + "approximate indexes this host is building - operator-chosen and small, bounded by how many "
                + "repositories were onboarded rather than by the data in any of them. Without them a "
                + "heterogeneous corpus on which one division of sixteen succeeded reported the fifteen "
                + "failures and the one success as the same series (issue #2855). Every arm also carries "
                + "'phase', a closed set naming where in the tick the step was: 'coordinating' (the tick "
                + "faulted before reaching the build step at all - resolving the run credential, probing the "
                + "corpus gate, or writing coordinator state), 'opening' (opening or restoring the durable "
                + "index), 'ingesting' (READING THE CORPUS out of the store of record, which is where a "
                + "corpus-read defect lands), 'training' (computing the partitioning over vectors already in "
                + "memory, which reads and writes nothing durable), 'persisting' (WRITING THE TRAINED INDEX "
                + "back, which is where an index-write defect lands), or 'reconciling' (catching an already "
                + "serving index up with the store of record). Read the ingesting/persisting split as the "
                + "attribution the fault cause cannot give: a read fault and a write fault are different "
                + "defects with different owners, and the write goes into the same tree a read defect would "
                + "already have named, so counting them as one series double-counts one defect or hides two. "
                + "The phase is resolved where the step runs and REFINED at the fault site, so a step that "
                + "trains and then persists in one call is attributed to whichever half threw. Unlike 'cause', "
                + "every phase IS pre-minted, because phase partitions the whole population rather than the "
                + "faults alone.");

        // Priming is per PLANE and lives in EnsurePrimed, not here: a constructor
        // cannot know which repositories and spaces exist, and a primed series for
        // a plane that does not exist claims a build nobody asked for. See
        // EnsurePrimed for the arms and for why the coordinator calls it above
        // every early return.
    }

    /// <summary>
    /// The rendered tag values for one plane, resolved once when the plane is first
    /// primed so no emission path has to re-render them.
    /// </summary>
    /// <param name="Repository">The repository tag value.</param>
    /// <param name="Space">The embedding-space tag value.</param>
    private readonly record struct PlaneTags(string Repository, string Space);

    /// <summary>
    /// Mints every series for one plane at zero, if they have not been minted
    /// already. Idempotent, and cheap after the first call.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the plane is built in.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    /// <remarks>
    /// <para>
    /// <b>Call this ABOVE every early return on the coordinator's tick, inside its
    /// fault seam.</b> An arm that is only minted on the path that also writes to it
    /// is not primed in the sense that matters: the zero a reader sees is produced
    /// by machinery that never ran, which is byte-identical to a measured absence
    /// and is exactly the defect issue #2952 records for a sibling instrument's
    /// scan-page phases. Priming here, first, means a plane whose every tick dies
    /// before it reaches the plane still reports six faulted phases at zero and one
    /// of them - <c>coordinating</c> - at whatever it actually is.
    /// </para>
    /// <para>
    /// <b>Which arms exist, and why not the full cross product.</b> Four phases can
    /// be reached by a step that COMPLETES - ingesting, training, persisting,
    /// reconciling - and each gets the whole five-arm progress partition, because a
    /// partition with an arm missing is not a partition and the missing arm is the
    /// one a reader would most want to read as zero. The remaining two -
    /// coordinating and opening - can only be reached by a fault: a tick that never
    /// reached the plane, or a step that died opening the index, cannot have
    /// advanced, starved, idled or churned, because no step ran to do any of those
    /// things. Priming them on the four completing arms would mint eight series per
    /// plane that are unreachable by construction, and an unreachable zero is not a
    /// measurement. Twenty-two series per plane in total.
    /// </para>
    /// <para>
    /// The zero-valued adds below are written out one arm at a time with literal tag
    /// constants rather than looped over an array, and deliberately so: the
    /// repository-wide priming gate
    /// (<c>test/lattice/Hygiene/InstrumentPrimingEnrolmentTests.cs</c>) reads the
    /// emission sites syntactically, so an arm primed through a loop variable is
    /// invisible to it and would be reported as never primed. Keep the literals.
    /// </para>
    /// </remarks>
    public void EnsurePrimed(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        _ = ResolvePlane(repoId, space);
    }

    private PlaneTags ResolvePlane(string repoId, EmbeddingSpaceTag space)
    {
        lock (_gate)
        {
            if (_planes.TryGetValue((repoId, space), out var cached))
            {
                return cached;
            }

            var plane = new PlaneTags(repoId, DescribeSpace(space));
            _planes[(repoId, space)] = plane;

            var r = new KeyValuePair<string, object?>(RepositoryTagKey, plane.Repository);
            var s = new KeyValuePair<string, object?>(SpaceTagKey, plane.Space);
            var t = LatticeTenantLabel.Platform;

            // Pre-mint every series with a zero-valued add. An arm that has never
            // been exercised is the arm most likely to be refused by a saturated
            // collector, and it is exactly the arm a reader is invited to read as a
            // measured zero.
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseIngestingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressAdvancedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseIngestingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressStarvedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseIngestingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressIdleTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseIngestingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressChurnedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseIngestingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag));

            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseTrainingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressAdvancedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseTrainingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressStarvedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseTrainingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressIdleTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseTrainingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressChurnedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseTrainingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag));

            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhasePersistingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressAdvancedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhasePersistingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressStarvedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhasePersistingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressIdleTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhasePersistingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressChurnedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhasePersistingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag));

            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseReconcilingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressAdvancedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseReconcilingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressStarvedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseReconcilingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressIdleTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseReconcilingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressChurnedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseReconcilingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag));

            // Fault-only phases: no step ran, so no completing arm is reachable.
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseCoordinatingTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag));
            _slices.Add(0, r, s, t,
                new KeyValuePair<string, object?>(PhaseTagKey, PhaseOpeningTag),
                new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag));

            // The five 'cause' values are deliberately NOT pre-minted, and that is
            // the one place this reporter departs from "prime everything". They
            // partition 'faulted' rather than the whole population, so a zero on a
            // cause is only interpretable once 'faulted' is itself non-zero - at
            // which point the faults have minted the causes themselves. Priming them
            // would mint series per plane that can never be read as measurements.
            // This is the same reasoning RepoContextAnnIndexSweepReporter records for
            // its own cause dimension, and the faulted arms primed above remain the
            // zero-anchor that makes the family falsifiable. Note the contrast with
            // 'phase', which IS primed: phase partitions the whole population, so its
            // zeros are measurements in their own right.
            return plane;
        }
    }

    /// <summary>
    /// Classifies one completed build step by comparing the progress it reported
    /// against the progress reported by the previous step.
    /// </summary>
    /// <param name="previous">
    /// Progress after the previous step of this activation, or the default value
    /// before any step has been taken. The default is the correct baseline: it
    /// names phase <see cref="VectorIndexBuildPhase.NotStarted"/> and zero of
    /// everything, which is precisely what a build that has not stepped holds.
    /// </param>
    /// <param name="current">Progress after the step being classified.</param>
    /// <returns>What the step did.</returns>
    /// <remarks>
    /// <para>
    /// <b>Deliberately a pure function of two progress readings.</b> The whole
    /// classification is a comparison of counts the index already knows, with no
    /// clock and no elapsed time anywhere in it, so it is exercised by constructing
    /// the two readings rather than by racing a real deadline. That matters: the
    /// condition being classified IS a timing defect, and a fixture that reproduced
    /// the timing to observe the classification would be pinning the detector with
    /// the same non-determinism the detector exists to report - which is how issue
    /// #2651's own fixture came to pass between 30% and 70% of the time while the
    /// defect it guarded was fully present.
    /// </para>
    /// <para>
    /// <b>Starvation is tested first, and the order is load-bearing.</b> The two
    /// conditions are mutually exclusive as the build is written today - a slice
    /// only counts as deadlined-without-progress when it consumed zero items, and a
    /// slice that consumed zero items banks no vector and moves no phase - so the
    /// order changes nothing about current behaviour. It is fixed anyway, because
    /// it is the direction a future overlap must fail in: a step that both banked
    /// something and starved a slice is a build in trouble, and classifying it as
    /// <see cref="RepoContextAnnBuildSliceOutcome.Advanced"/> would hide exactly the
    /// signal this counter was added to surface.
    /// </para>
    /// </remarks>
    internal static RepoContextAnnBuildSliceOutcome Classify(
        VectorIndexBuildProgress previous, VectorIndexBuildProgress current)
    {
        if (current.SlicesDeadlinedWithoutProgress > previous.SlicesDeadlinedWithoutProgress)
        {
            return RepoContextAnnBuildSliceOutcome.Starved;
        }

        // A PHASE MOVE IS COUNTED, BUT IT IS NOT COUNTED AS PROGRESS. Three-way, and
        // each leg answers a different question.
        //
        // Phase is still compared for INEQUALITY rather than for a forward move, and
        // that half is unchanged and deliberate: the phases are ordered, but a
        // rebuild legitimately returns the build to an EARLIER one, and a step that
        // reset the build has unambiguously done something. Calling that idle would
        // under-report a plane churning through repeated rebuilds as one sitting
        // still, which is the opposite of what this counter is for. Do not
        // "tighten" this into a forward-only comparison; it would reintroduce
        // exactly that blindness.
        //
        // What issue #2818 corrected is the CONCLUSION drawn from it. "Not idle" was
        // treated as equivalent to "advanced", because the outcome set offered no
        // third option - so a build oscillating Training -> Persisting -> Training
        // (issue #2791) banked not one vector and still emitted a steadily rising
        // 'advanced' arm, on the one series whose job is to say whether the build is
        // progressing. The two questions are separate and both need answering:
        //
        //   did the step BANK anything?  -> VectorsIndexed or PartitionsTotal rose:
        //                                   Advanced. Real, servable progress.
        //   did the step DO anything?    -> only the phase moved: Churned. Something
        //                                   happened; nothing was banked. A rising
        //                                   churned arm beside a flat advanced one
        //                                   IS the livelock signature.
        //   neither                      -> Idle.
        if (current.VectorsIndexed > previous.VectorsIndexed
            || current.PartitionsTotal > previous.PartitionsTotal)
        {
            return RepoContextAnnBuildSliceOutcome.Advanced;
        }

        return current.Phase != previous.Phase
            ? RepoContextAnnBuildSliceOutcome.Churned
            : RepoContextAnnBuildSliceOutcome.Idle;
    }

    /// <summary>Records one completed approximate-index build step.</summary>
    /// <param name="outcome">
    /// What the step did to the build. Must not be
    /// <see cref="RepoContextAnnBuildSliceOutcome.Faulted"/>, which is recorded
    /// through <see cref="RecordFaulted"/> so that it carries a cause.
    /// </param>
    /// <param name="repoId">The repository whose plane stepped. Must not be null.</param>
    /// <param name="space">The embedding space the plane is built in.</param>
    /// <param name="phase">Where in the tick the step was.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="outcome"/> is <see cref="RepoContextAnnBuildSliceOutcome.Faulted"/>.
    /// </exception>
    /// <remarks>
    /// The split into two entry points is what makes "no fault path may emit a
    /// default or empty cause" structural rather than a matter of discipline. A
    /// single <c>RecordSlice(outcome, cause = default)</c> would let a new fault
    /// path compile while emitting the default value, and a default is precisely
    /// how the next reader is handed a benign-looking number again. Here a fault
    /// cannot be counted without a cause because there is no overload that accepts
    /// one without. Borrowed unchanged from
    /// <see cref="RepoContextAnnIndexSweepReporter.RecordCompleted"/>, which
    /// settled the same question for the sweep on issue #2578. The plane and phase
    /// are required for the same reason and are deliberately NOT optional: an
    /// unnamed plane or a defaulted phase would be a benign-looking value on the
    /// dimension the epic added to stop exactly that.
    /// </remarks>
    public void RecordSlice(
        RepoContextAnnBuildSliceOutcome outcome,
        string repoId,
        EmbeddingSpaceTag space,
        RepoContextAnnBuildStepPhase phase)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        if (outcome == RepoContextAnnBuildSliceOutcome.Faulted)
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome),
                outcome,
                "A faulted build step must be recorded through RecordFaulted so that it carries a cause.");
        }

        var plane = ResolvePlane(repoId, space);
        _slices.Add(
            1,
            new KeyValuePair<string, object?>(RepositoryTagKey, plane.Repository),
            new KeyValuePair<string, object?>(SpaceTagKey, plane.Space),
            new KeyValuePair<string, object?>(PhaseTagKey, DescribePhase(phase)),
            new KeyValuePair<string, object?>(ProgressTagKey, DescribeOutcome(outcome)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            switch (outcome)
            {
                case RepoContextAnnBuildSliceOutcome.Advanced:
                    _advanced++;
                    break;
                case RepoContextAnnBuildSliceOutcome.Starved:
                    _starved++;
                    break;
                case RepoContextAnnBuildSliceOutcome.Churned:
                    _churned++;
                    break;
                default:
                    _idle++;
                    break;
            }
        }
    }

    /// <summary>
    /// Records one approximate-index build step that threw, under the cause its
    /// fault site resolved and the phase it was in.
    /// </summary>
    /// <param name="cause">
    /// Why the step threw. Required, and resolved where the fault was raised rather
    /// than inferred here, because the site is the only place that knows what the
    /// step was doing.
    /// </param>
    /// <param name="repoId">The repository whose plane faulted. Must not be null.</param>
    /// <param name="space">The embedding space the plane is built in.</param>
    /// <param name="phase">
    /// Where in the tick the fault happened. The dimension that separates a corpus
    /// read that could not be served from an index that could not be written.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public void RecordFaulted(
        RepoContextAnnBuildFaultCause cause,
        string repoId,
        EmbeddingSpaceTag space,
        RepoContextAnnBuildStepPhase phase)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var plane = ResolvePlane(repoId, space);
        _slices.Add(
            1,
            new KeyValuePair<string, object?>(RepositoryTagKey, plane.Repository),
            new KeyValuePair<string, object?>(SpaceTagKey, plane.Space),
            new KeyValuePair<string, object?>(PhaseTagKey, DescribePhase(phase)),
            new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag),
            new KeyValuePair<string, object?>(CauseTagKey, DescribeCause(cause)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            _faulted++;
            switch (phase)
            {
                case RepoContextAnnBuildStepPhase.Opening:
                    _faultedOpening++;
                    break;
                case RepoContextAnnBuildStepPhase.Ingesting:
                    _faultedIngesting++;
                    break;
                case RepoContextAnnBuildStepPhase.Training:
                    _faultedTraining++;
                    break;
                case RepoContextAnnBuildStepPhase.Persisting:
                    _faultedPersisting++;
                    break;
                case RepoContextAnnBuildStepPhase.Reconciling:
                    _faultedReconciling++;
                    break;
                default:
                    // Matches the tag DescribePhase resolves for the same value, so
                    // the tally can never disagree with the meter about which arm an
                    // out-of-range cast landed on.
                    _faultedCoordinating++;
                    break;
            }

            switch (cause)
            {
                case RepoContextAnnBuildFaultCause.ScanPageStalled:
                    _faultedScanPageStalled++;
                    break;
                case RepoContextAnnBuildFaultCause.ProjectionStale:
                    _faultedProjectionStale++;
                    break;
                case RepoContextAnnBuildFaultCause.DependencyUnavailable:
                    _faultedDependencyUnavailable++;
                    break;
                case RepoContextAnnBuildFaultCause.PlaneRejected:
                    _faultedPlaneRejected++;
                    break;
                default:
                    // Fails open onto the arm that pages, matching the tag
                    // DescribeCause resolves for the same value, so the tally can
                    // never disagree with the meter about which arm an out-of-range
                    // cast landed on.
                    _faultedUnexpected++;
                    break;
            }
        }
    }

    /// <summary>Reads the cumulative counters.</summary>
    /// <returns>The snapshot.</returns>
    public RepoContextAnnBuildSliceSnapshot Read()
    {
        lock (_gate)
        {
            return new RepoContextAnnBuildSliceSnapshot(
                _advanced,
                _starved,
                _idle,
                _faulted,
                _churned,
                new RepoContextAnnBuildFaultTally(
                    _faultedUnexpected,
                    _faultedScanPageStalled,
                    _faultedProjectionStale,
                    _faultedDependencyUnavailable,
                    _faultedPlaneRejected),
                new RepoContextAnnBuildPhaseTally(
                    _faultedCoordinating,
                    _faultedOpening,
                    _faultedIngesting,
                    _faultedTraining,
                    _faultedPersisting,
                    _faultedReconciling));
        }
    }

    /// <summary>
    /// The bounded tag value for a progress class. Resolved against a closed set so
    /// an unrecognised value can never reach the meter as unbounded-cardinality
    /// text, and so a new enum member fails closed onto <c>idle</c> rather than onto
    /// the arm that reads as healthy.
    /// </summary>
    /// <param name="outcome">The outcome to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeOutcome(RepoContextAnnBuildSliceOutcome outcome) => outcome switch
    {
        RepoContextAnnBuildSliceOutcome.Advanced => ProgressAdvancedTag,
        RepoContextAnnBuildSliceOutcome.Starved => ProgressStarvedTag,
        RepoContextAnnBuildSliceOutcome.Faulted => ProgressFaultedTag,
        RepoContextAnnBuildSliceOutcome.Churned => ProgressChurnedTag,
        _ => ProgressIdleTag,
    };

    /// <summary>
    /// The bounded tag value for a fault cause. Resolved against a closed set so an
    /// unrecognised value can never reach the meter as unbounded-cardinality text,
    /// and fails open onto <see cref="RepoContextAnnBuildFaultCause.Unexpected"/> -
    /// the arm that pages - rather than onto one with a benign explanation. A cause
    /// nobody mapped is closer to a fault nobody understands than to one that is
    /// already diagnosed.
    /// </summary>
    /// <param name="cause">The cause to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeCause(RepoContextAnnBuildFaultCause cause) => cause switch
    {
        RepoContextAnnBuildFaultCause.ScanPageStalled => CauseScanPageStalledTag,
        RepoContextAnnBuildFaultCause.ProjectionStale => CauseProjectionStaleTag,
        RepoContextAnnBuildFaultCause.DependencyUnavailable => CauseDependencyUnavailableTag,
        RepoContextAnnBuildFaultCause.PlaneRejected => CausePlaneRejectedTag,
        _ => CauseUnexpectedTag,
    };

    /// <summary>
    /// The bounded tag value for a build phase. Resolved against a closed set so an
    /// unrecognised value can never reach the meter as unbounded-cardinality text,
    /// and fails closed onto <see cref="RepoContextAnnBuildStepPhase.Coordinating"/>
    /// - the arm that claims the LEAST about where the step was. An unmapped phase
    /// is a phase nobody placed, and placing it on ingest or persist would be the
    /// specific misattribution this dimension exists to prevent.
    /// </summary>
    /// <param name="phase">The phase to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribePhase(RepoContextAnnBuildStepPhase phase) => phase switch
    {
        RepoContextAnnBuildStepPhase.Opening => PhaseOpeningTag,
        RepoContextAnnBuildStepPhase.Ingesting => PhaseIngestingTag,
        RepoContextAnnBuildStepPhase.Training => PhaseTrainingTag,
        RepoContextAnnBuildStepPhase.Persisting => PhasePersistingTag,
        RepoContextAnnBuildStepPhase.Reconciling => PhaseReconcilingTag,
        _ => PhaseCoordinatingTag,
    };

    /// <summary>
    /// The tag value naming one embedding space, as <c>{modelId}/{dimension}</c>.
    /// A space nobody has stamped renders as <see cref="SpaceUnspecifiedTag"/>
    /// rather than as an empty string, so an unstamped plane is visibly unstamped
    /// instead of looking like a tag the exporter dropped.
    /// </summary>
    /// <param name="space">The space to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeSpace(EmbeddingSpaceTag space) => space.IsSpecified
        ? string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"{space.ModelId}/{space.Dimension}")
        : SpaceUnspecifiedTag;

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
