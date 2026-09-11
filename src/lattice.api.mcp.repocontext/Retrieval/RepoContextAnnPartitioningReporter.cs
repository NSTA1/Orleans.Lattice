using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Whether one repository's approximate plane holds a partitioning, and when it
/// does not, whether the corpus is large enough that it should.
/// <para>
/// The three values are exhaustive over a build that reached <c>Ready</c>, which
/// is what lets them be counted as a partition rather than as three unrelated
/// tallies. <see cref="Partitioned"/> is the arm that makes the other two
/// interpretable: without it the total would read zero both on a healthy plane
/// and on one that never built.
/// </para>
/// </summary>
internal enum RepoContextAnnPartitioningState
{
    /// <summary>
    /// The plane holds a trained partitioning, so semantic retrieval is
    /// approximate. This is the steady state the plane exists to reach.
    /// </summary>
    Partitioned = 0,

    /// <summary>
    /// The plane holds no partitioning and its corpus is below
    /// <see cref="RepoContextAnnOptions.MinimumTrainingCount"/>. Correct and
    /// expected: a corpus this small is scanned exhaustively and exactly, which is
    /// faster than probing it. Nothing is wrong and no operator is needed.
    /// </summary>
    BelowMinimum = 1,

    /// <summary>
    /// The plane holds no partitioning although its corpus has reached
    /// <see cref="RepoContextAnnOptions.MinimumTrainingCount"/>. Every query is
    /// answered by brute-force scan of a corpus large enough to partition, so the
    /// plane is doing none of the work it exists to do.
    /// <para>
    /// A single observation of this state is not a fault: it is exactly what a
    /// plane looks like in the instant between the corpus crossing the threshold
    /// and the maintenance turn that partitions it. What is a fault is this arm
    /// advancing while <see cref="Partitioned"/> stays at zero, which is issue
    /// #2706's signature and is the reading this arm exists to make available
    /// without correlating three log lines.
    /// </para>
    /// </summary>
    AboveMinimum = 2,
}

/// <summary>
/// A point-in-time reading of the partitioning-state counters, cumulative since
/// process start.
/// </summary>
/// <param name="Partitioned">Observations of a plane holding a partitioning.</param>
/// <param name="BelowMinimum">Observations of an unpartitioned plane whose corpus is legitimately too small.</param>
/// <param name="AboveMinimum">Observations of an unpartitioned plane whose corpus is large enough to partition.</param>
/// <param name="Repartitioned">Threshold-crossing trainings that produced a partitioning.</param>
/// <param name="RepartitionDeclined">Threshold-crossing trainings that declined to partition anyway.</param>
internal readonly record struct RepoContextAnnPartitioningSnapshot(
    long Partitioned,
    long BelowMinimum,
    long AboveMinimum,
    long Repartitioned,
    long RepartitionDeclined);

/// <summary>
/// Meters whether each repository's approximate plane is partitioned, so an
/// unpartitioned plane holding a large corpus can never again be visible only by
/// correlating three log lines.
/// <para>
/// <b>Why this exists.</b> A plane that declined to partition on an empty corpus
/// and one that has partitioned a large one were, from outside, the same
/// observation: both reach <c>Ready</c>, both serve, both report healthy on every
/// readiness signal above them, because the serving latch deliberately ignores the
/// partition count. Issue #2706 measured a deployment that sat in the first state
/// for 8.6 hours holding 7,628 vectors against a minimum of 1,024, answering every
/// query by brute-force scan, with no series anywhere that could say so. The only
/// evidence was three log lines that had to be correlated by hand, one of which had
/// been emitted once, hours earlier, and was correct when it was written.
/// </para>
/// <para>
/// <b>Why the small case is a separate arm rather than an absence.</b> An
/// unpartitioned plane is the correct, expected state for a corpus below the
/// training minimum, and it is also the signature of the defect above. Counting
/// only the unpartitioned planes would therefore give one number that means
/// "healthy" on a small deployment and "broken" on a large one, and no reader
/// could tell which without going back to the corpus size. Splitting the
/// unpartitioned total by whether the corpus has reached the minimum is what makes
/// the number answer the question on its own - which is acceptance criterion 5 of
/// issue #2706.
/// </para>
/// <para>
/// <b>Why a zero here is evidence and not silence.</b> The partition is total over
/// every observation, <see cref="RepoContextAnnPartitioningState.Partitioned"/>
/// included, so the total advances whenever the plane is built at all. A zero on
/// <c>state=unpartitioned-large</c> beside a rising total is therefore a
/// <i>measured</i> absence of the defect rather than an absent measurement, and a
/// rising <c>state=unpartitioned-large</c> beside a zero <c>state=partitioned</c>
/// is the defect stating itself.
/// </para>
/// <para>
/// <b>All arms are pre-minted.</b> Every series is created with a zero-valued add
/// in the constructor, so on a correctly configured host each arm is present and
/// reads <c>0</c> rather than being absent. A counter exports no series until its
/// first <c>Add</c>, so an unprimed zero is indistinguishable from a missing
/// instrument - the confusion this bucket has already paid for more than once.
/// </para>
/// <para>
/// <b>Cardinality and disclosure.</b> The only tag is the closed state set. No
/// repository id, no key, no corpus size, so the series count is fixed at five
/// regardless of how many repositories a host serves.
/// </para>
/// </summary>
internal sealed class RepoContextAnnPartitioningReporter : IDisposable
{
    /// <summary>
    /// The counter of approximate-plane observations, partitioned by whether the
    /// plane holds a partitioning and, when it does not, whether its corpus is
    /// large enough that it should.
    /// </summary>
    internal const string PartitioningInstrumentName = "repocontext.ann.partitioning";

    /// <summary>
    /// The counter of threshold-crossing trainings - the repair issue #2706 adds -
    /// partitioned by whether the training produced a partitioning.
    /// </summary>
    internal const string RepartitionInstrumentName = "repocontext.ann.repartition";

    /// <summary>The tag key carrying the partitioning state.</summary>
    internal const string StateTagKey = "state";

    /// <summary>The tag value for a plane holding a trained partitioning.</summary>
    internal const string StatePartitionedTag = "partitioned";

    /// <summary>The tag value for an unpartitioned plane whose corpus is below the training minimum.</summary>
    internal const string StateUnpartitionedSmallTag = "unpartitioned-small";

    /// <summary>The tag value for an unpartitioned plane whose corpus has reached the training minimum.</summary>
    internal const string StateUnpartitionedLargeTag = "unpartitioned-large";

    /// <summary>The tag key carrying a threshold-crossing training's outcome.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The tag value for a threshold-crossing training that produced a partitioning.</summary>
    internal const string OutcomePartitionedTag = "partitioned";

    /// <summary>The tag value for a threshold-crossing training that declined to partition anyway.</summary>
    internal const string OutcomeDeclinedTag = "declined";

    // Declared above the instruments it constructs, and both instruments are built
    // from this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _partitioning;
    private readonly Counter<long> _repartitions;

    private readonly Lock _gate = new();
    private long _partitioned;
    private long _belowMinimum;
    private long _aboveMinimum;
    private long _repartitioned;
    private long _repartitionDeclined;

    /// <summary>Creates the reporter, its instruments, and every one of their series.</summary>
    public RepoContextAnnPartitioningReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _partitioning = _meter.CreateCounter<long>(
            PartitioningInstrumentName,
            unit: "{observation}",
            description:
                "Observations of a repository's approximate plane after a completed build, partitioned by "
                + "whether it holds a trained partitioning: 'partitioned' (semantic retrieval is approximate), "
                + "'unpartitioned-small' (no partitioning and the corpus is below the training minimum, which "
                + "is correct - a corpus this small is scanned exhaustively and exactly), or "
                + "'unpartitioned-large' (no partitioning although the corpus has reached the training minimum, "
                + "so every query is answered by brute-force scan of a corpus large enough to partition). Every "
                + "observation is counted, so the total is a denominator and a zero on 'unpartitioned-large' "
                + "beside a rising total is a measured absence rather than an absent measurement. A single "
                + "'unpartitioned-large' observation is the ordinary instant between the corpus crossing the "
                + "threshold and the maintenance turn that partitions it; that arm advancing while "
                + "'partitioned' stays at zero is the fault, and is what issue #2706 measured for 8.6 hours "
                + "with no series able to report it. All three arms are pre-minted at zero, so an arm reading "
                + "zero is a measurement and an arm that is absent is not.");
        _repartitions = _meter.CreateCounter<long>(
            RepartitionInstrumentName,
            unit: "{training}",
            description:
                "Trainings run because the corpus crossed the training minimum after an earlier training had "
                + "declined to partition it, partitioned by outcome: 'partitioned' (the repair worked and the "
                + "plane now serves approximately) or 'declined' (the corpus is at or above the minimum but "
                + "still resolves to fewer than two partitions, so it stays exhaustive). This is the repair "
                + "path itself rather than the state it repairs, so a non-zero 'partitioned' value is positive "
                + "evidence that a latched plane healed without operator action. A 'declined' value that keeps "
                + "rising means the threshold is being crossed and the partition count still will not resolve, "
                + "which no amount of further corpus growth inside one activation will change quickly; both "
                + "arms are pre-minted at zero.");

        // Pre-mint every series with a zero-valued add. An arm that has never been
        // exercised is the arm most likely to be refused by a saturated collector,
        // and it is exactly the arm the descriptions above invite the reader to read
        // as a measured zero.
        _partitioning.Add(0, new KeyValuePair<string, object?>(StateTagKey, StatePartitionedTag), LatticeTenantLabel.Platform);
        _partitioning.Add(0, new KeyValuePair<string, object?>(StateTagKey, StateUnpartitionedSmallTag), LatticeTenantLabel.Platform);
        _partitioning.Add(0, new KeyValuePair<string, object?>(StateTagKey, StateUnpartitionedLargeTag), LatticeTenantLabel.Platform);
        _repartitions.Add(0, new KeyValuePair<string, object?>(OutcomeTagKey, OutcomePartitionedTag), LatticeTenantLabel.Platform);
        _repartitions.Add(0, new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeDeclinedTag), LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Records one observation of a plane that has finished building, classified
    /// against the corpus size the plane would need to partition.
    /// </summary>
    /// <param name="state">What the plane's partitioning looks like right now.</param>
    public void RecordPartitioning(RepoContextAnnPartitioningState state)
    {
        _partitioning.Add(
            1,
            new KeyValuePair<string, object?>(StateTagKey, DescribeState(state)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            switch (state)
            {
                case RepoContextAnnPartitioningState.Partitioned:
                    _partitioned++;
                    break;
                case RepoContextAnnPartitioningState.BelowMinimum:
                    _belowMinimum++;
                    break;
                default:
                    _aboveMinimum++;
                    break;
            }
        }
    }

    /// <summary>
    /// Records one threshold-crossing training and whether it produced a
    /// partitioning.
    /// </summary>
    /// <param name="partitioned">Whether the training produced a partitioning.</param>
    public void RecordRepartition(bool partitioned)
    {
        _repartitions.Add(
            1,
            new KeyValuePair<string, object?>(
                OutcomeTagKey, partitioned ? OutcomePartitionedTag : OutcomeDeclinedTag),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            if (partitioned)
            {
                _repartitioned++;
            }
            else
            {
                _repartitionDeclined++;
            }
        }
    }

    /// <summary>Reads the cumulative counters.</summary>
    /// <returns>The snapshot.</returns>
    public RepoContextAnnPartitioningSnapshot Read()
    {
        lock (_gate)
        {
            return new RepoContextAnnPartitioningSnapshot(
                _partitioned, _belowMinimum, _aboveMinimum, _repartitioned, _repartitionDeclined);
        }
    }

    /// <summary>
    /// Classifies a completed build's partitioning against the corpus size that
    /// would justify one. This is the whole of the judgement the instrument
    /// encodes, kept in one place so the counter and any caller that wants the
    /// same reading cannot drift apart.
    /// </summary>
    /// <param name="partitionCount">The number of partitions the plane holds.</param>
    /// <param name="vectorCount">The number of vectors the plane holds.</param>
    /// <param name="minimumTrainingCount">The smallest corpus the plane will partition.</param>
    /// <returns>The partitioning state.</returns>
    internal static RepoContextAnnPartitioningState Classify(
        long partitionCount, long vectorCount, int minimumTrainingCount) =>
        partitionCount > 0 ? RepoContextAnnPartitioningState.Partitioned
        : vectorCount >= minimumTrainingCount ? RepoContextAnnPartitioningState.AboveMinimum
        : RepoContextAnnPartitioningState.BelowMinimum;

    /// <summary>
    /// The bounded tag value for a partitioning state. Resolved against a closed
    /// set so an unrecognised value can never reach the meter as
    /// unbounded-cardinality text, and so a new enum member fails onto the arm that
    /// asks for attention rather than onto the reassuring one.
    /// </summary>
    /// <param name="state">The state to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeState(RepoContextAnnPartitioningState state) => state switch
    {
        RepoContextAnnPartitioningState.Partitioned => StatePartitionedTag,
        RepoContextAnnPartitioningState.BelowMinimum => StateUnpartitionedSmallTag,
        _ => StateUnpartitionedLargeTag,
    };

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
