using System.Diagnostics.Metrics;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What one approximate-index build step did to the build it advances. The three
/// values are exhaustive over a step that completed, which is what lets them be
/// counted as a partition rather than as three unrelated tallies.
/// </summary>
internal enum RepoContextAnnBuildSliceOutcome
{
    /// <summary>
    /// The step moved the build on: it banked at least one more vector, or it
    /// carried the build into a later phase. This is the ordinary arm, and it is
    /// the one that makes the other two interpretable.
    /// </summary>
    Advanced = 0,

    /// <summary>
    /// The step's ingest slice was stopped by its wall-clock budget having banked
    /// nothing, so the cursor did not move. The source did not deliver a single
    /// item before the budget was spent, which no larger budget repairs.
    /// </summary>
    Starved = 1,

    /// <summary>
    /// The step completed without advancing the build and without its slice being
    /// deadlined empty-handed. A converged coordinator re-opening its in-memory
    /// handle takes this arm, and so does a slice that banked nothing for any
    /// reason other than a spent deadline.
    /// </summary>
    Idle = 2,
}

/// <summary>
/// A point-in-time reading of the build-slice counters, cumulative since process
/// start.
/// </summary>
/// <param name="Advanced">Steps that moved the build on.</param>
/// <param name="Starved">Steps whose slice was deadlined having banked nothing.</param>
/// <param name="Idle">Steps that neither advanced the build nor were deadlined empty-handed.</param>
internal readonly record struct RepoContextAnnBuildSliceSnapshot(
    long Advanced,
    long Starved,
    long Idle)
{
    /// <summary>
    /// Every step counted, across all three arms. Non-zero exactly when the build
    /// coordinator has taken at least one step in this process, which is the fact
    /// no other series in the approximate-index family can report.
    /// </summary>
    public long Total => Advanced + Starved + Idle;
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
/// <c>progress=starved</c> arm is a coordinator that is stepping and getting
/// nowhere.
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

    /// <summary>The tag value for a step that neither advanced nor was deadlined empty-handed.</summary>
    internal const string ProgressIdleTag = "idle";

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

    /// <summary>Creates the reporter, its instrument, and every one of its series.</summary>
    public RepoContextAnnBuildSliceReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _slices = _meter.CreateCounter<long>(
            SliceInstrumentName,
            unit: "{step}",
            description:
                "Approximate-index build steps that completed, partitioned by what the step did to the build: "
                + "'advanced' (it banked at least one more vector or carried the build into a later phase), "
                + "'starved' (its ingest slice was stopped by the wall-clock budget having banked nothing, so the "
                + "cursor did not move and no larger budget repairs it), or 'idle' (it neither advanced the build "
                + "nor was deadlined empty-handed, which is what a converged coordinator re-opening its in-memory "
                + "handle does). Every other instrument on this plane fires only at a terminal moment - a build "
                + "that reached Ready, a plane that finished building, a sweep that armed a coordinator - so "
                + "between arming and Ready the plane emitted nothing, and a build consuming nothing was "
                + "byte-identical in telemetry to a build that never ran. The TOTAL across all three arms is the "
                + "figure that separates them: zero beside a non-zero 'ann.sweep{outcome=armed}' means the "
                + "coordinator is not stepping, while a rising 'starved' arm means it is stepping and getting "
                + "nowhere.");

        // Pre-mint every series with a zero-valued add, so a correctly configured
        // host reports progress=starved at 0 rather than omitting it. An absent
        // series and a series reading zero look identical on a dashboard but are
        // very different claims, and only the second is falsifiable.
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressAdvancedTag), LatticeTenantLabel.Platform);
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressStarvedTag), LatticeTenantLabel.Platform);
        _slices.Add(0, new KeyValuePair<string, object?>(ProgressTagKey, ProgressIdleTag), LatticeTenantLabel.Platform);
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

        // Phase is compared for INEQUALITY rather than for a forward move. The
        // phases are ordered, but a rebuild legitimately returns the build to an
        // earlier one, and a step that reset the build has unambiguously done
        // something - calling that idle would under-report a plane churning through
        // repeated rebuilds as one sitting still, which is the opposite of what this
        // counter is for.
        return current.VectorsIndexed > previous.VectorsIndexed
            || current.PartitionsTotal > previous.PartitionsTotal
            || current.Phase != previous.Phase
            ? RepoContextAnnBuildSliceOutcome.Advanced
            : RepoContextAnnBuildSliceOutcome.Idle;
    }

    /// <summary>Records one completed approximate-index build step.</summary>
    /// <param name="outcome">What the step did to the build.</param>
    public void RecordSlice(RepoContextAnnBuildSliceOutcome outcome)
    {
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
                default:
                    _idle++;
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
            return new RepoContextAnnBuildSliceSnapshot(_advanced, _starved, _idle);
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
        _ => ProgressIdleTag,
    };

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
