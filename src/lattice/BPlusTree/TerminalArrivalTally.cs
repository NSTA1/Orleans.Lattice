namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, deterministic completeness gate for the registry's gated terminal
/// tally: a saga's per-source-shard terminals arrive one at a time, and the
/// registry must decide, on each arrival, whether every expected terminal has now
/// been seen so it can flip the per-tree linearization mark exactly once. Extracted
/// from <c>TxRegistryGrain.RecordTerminalArrivalAsync</c> so the completeness rule
/// is one shared, testable function rather than an inline comparison, exactly like
/// <see cref="SagaCoordinatorCore"/> and <see cref="TerminalDecisionGuard"/>.
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no timers, no wall-clock, and no
/// Orleans types: it is a total function of integer counts and allocates nothing.
/// It models only the count arithmetic; the idempotent dedup of which source
/// shards have arrived is the registry grain's own in-memory
/// <see cref="System.Collections.Generic.HashSet{T}"/> and is not part of this
/// core (see the exclusion note in the testing instructions).
/// </para>
/// </summary>
internal static class TerminalArrivalTally
{
    /// <summary>
    /// Folds an incoming expected-terminal count into the saga's recorded expected
    /// count. The expected count is monotonic non-decreasing: a later arrival that
    /// carries a larger gate stamp raises it, but a smaller (or stale) stamp never
    /// lowers it, so a duplicate delivery with an out-of-date expectation cannot
    /// shrink the quorum.
    /// </summary>
    /// <param name="hadPrevious">
    /// <see langword="true"/> when the registry already holds an expected count for
    /// the saga.
    /// </param>
    /// <param name="previousExpected">
    /// The expected count already recorded. Meaningful only when
    /// <paramref name="hadPrevious"/> is <see langword="true"/>.
    /// </param>
    /// <param name="incomingExpected">
    /// The expected count carried by the arriving terminal.
    /// </param>
    /// <returns>
    /// The merged expected count: the larger of the two when a previous count
    /// exists, otherwise <paramref name="incomingExpected"/>.
    /// </returns>
    public static int MergeExpected(bool hadPrevious, int previousExpected, int incomingExpected) =>
        hadPrevious ? Math.Max(previousExpected, incomingExpected) : incomingExpected;

    /// <summary>
    /// Reports whether the tally is complete: every expected per-source-shard
    /// terminal has arrived. The comparison is <c>&gt;=</c> rather than <c>==</c>
    /// so a gate whose expected count was lowered by a corrected stamp (which
    /// <see cref="MergeExpected"/> forbids) or an over-count from a benign
    /// duplicate can still resolve, never latching the saga open.
    /// </summary>
    /// <param name="arrivalCount">
    /// The number of distinct source-shard terminals observed so far.
    /// </param>
    /// <param name="expectedCount">
    /// The expected total terminal count, as merged by <see cref="MergeExpected"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="arrivalCount"/> has reached
    /// <paramref name="expectedCount"/>.
    /// </returns>
    public static bool IsFinalArrival(int arrivalCount, int expectedCount) =>
        arrivalCount >= expectedCount;
}
