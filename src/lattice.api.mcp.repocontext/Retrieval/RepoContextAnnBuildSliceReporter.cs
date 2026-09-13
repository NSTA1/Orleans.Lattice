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
internal readonly record struct RepoContextAnnBuildSliceSnapshot(
    long Advanced,
    long Starved,
    long Idle,
    long Faulted,
    long Churned,
    RepoContextAnnBuildFaultTally FaultedByCause)
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
/// <b>Cardinality and disclosure.</b> The only tag is the closed progress set. No
/// repository id, no embedding space, no key. The repository and space ride the
/// accompanying log line, for the same reason
/// <see cref="RepoContextAnnIndexBuildGrain.MetricsTreeId"/> declines to emit them
/// through a tenant dimension.
/// </para>
/// <para>
/// <b>All arms are pre-minted.</b> Every series is created with a zero-valued add
/// in the constructor, so on a correctly configured host <c>progress=starved</c>
/// is present and reads <c>0</c> rather than being absent. A zero-primed family is
/// a measurement rather than silence - but note that it is then the <i>total</i>,
/// not any single arm, that distinguishes "the coordinator has not stepped" from
/// "the coordinator has stepped and nothing went wrong".
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

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering throws at type-initialisation rather than publishing
    // an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _slices;

    private readonly Lock _gate = new();
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
                + "a tree from it (issue #2880).");

        // Pre-mint every series with a zero-valued add, so a correctly configured
        // host reports progress=starved at 0 rather than omitting it. An absent
        // series and a series reading zero look identical on a dashboard but are
        // very different claims, and only the second is falsifiable.
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressAdvancedTag), LatticeTenantLabel.Platform);
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressStarvedTag), LatticeTenantLabel.Platform);
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressIdleTag), LatticeTenantLabel.Platform);
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag), LatticeTenantLabel.Platform);
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressChurnedTag), LatticeTenantLabel.Platform);

        // The five 'cause' values are deliberately NOT pre-minted, and that is the
        // one place this reporter departs from "prime everything". They partition
        // 'faulted' rather than the whole population, so a zero on a cause is only
        // interpretable once 'faulted' is itself non-zero - at which point the
        // faults have minted the causes themselves. Priming them would mint five
        // series per host that can never be read as measurements, and would spend
        // the collector's series budget on arms whose zero says nothing. This is
        // the same reasoning RepoContextAnnIndexSweepReporter records for its own
        // cause dimension, and the 'faulted' arm primed above remains the
        // zero-anchor that makes the family falsifiable.
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
    /// settled the same question for the sweep on issue #2578.
    /// </remarks>
    public void RecordSlice(RepoContextAnnBuildSliceOutcome outcome)
    {
        if (outcome == RepoContextAnnBuildSliceOutcome.Faulted)
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome),
                outcome,
                "A faulted build step must be recorded through RecordFaulted so that it carries a cause.");
        }

        _slices.Add(
            1,
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
    /// fault site resolved.
    /// </summary>
    /// <param name="cause">
    /// Why the step threw. Required, and resolved where the fault was raised rather
    /// than inferred here, because the site is the only place that knows what the
    /// step was doing.
    /// </param>
    public void RecordFaulted(RepoContextAnnBuildFaultCause cause)
    {
        _slices.Add(
            1,
            new KeyValuePair<string, object?>(ProgressTagKey, ProgressFaultedTag),
            new KeyValuePair<string, object?>(CauseTagKey, DescribeCause(cause)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            _faulted++;
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
                    _faultedPlaneRejected));
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

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
