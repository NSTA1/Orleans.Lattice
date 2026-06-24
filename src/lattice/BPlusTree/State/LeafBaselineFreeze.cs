namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A single leaf's point-in-time projection frozen during the first pass of
/// a per-shard snapshot-baseline capture. Carries everything the second
/// (fold) pass needs to materialise this leaf's contribution to the shard
/// baseline at the uniform <c>capturedHead</c>, without re-reading the live
/// leaf's cache (which advances between the two passes as foreground writes
/// continue).
/// <para>
/// Shipping the frozen state out to the shard root and back in - rather than
/// holding it transiently on the leaf between the two calls - keeps the
/// capture robust against a leaf reactivation in the window between freeze
/// and fold: the authoritative copy lives in the shard root's single capture
/// turn, so an idle-evicted-and-reactivated leaf cannot lose it and force an
/// overshooting re-freeze past <c>capturedHead</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafBaselineFreeze)]
internal sealed class LeafBaselineFreeze
{
    /// <summary>
    /// The leaf's committed projection rows (live values plus tombstones) at
    /// the moment of the freeze, ordered by key. This is the per-leaf
    /// checkpoint baseline the tail fold is layered on top of. Never
    /// <see langword="null"/>; empty for a leaf with no committed entries.
    /// </summary>
    [Id(0)] public IReadOnlyList<LeafSnapshotRow> Rows { get; set; } = Array.Empty<LeafSnapshotRow>();

    /// <summary>
    /// The leaf's current per-partition projection frontier (the offset its
    /// cache already reflects, max of pending and persisted checkpoint),
    /// indexed by WAL partition number. The fold replays only
    /// <c>(FrontierPerPartition[p], capturedHead[p]]</c> for each partition so
    /// every WAL record is applied exactly once relative to this leaf's cache.
    /// </summary>
    [Id(1)] public IReadOnlyList<long> FrontierPerPartition { get; set; } = Array.Empty<long>();

    /// <summary>
    /// The leaf's in-flight prepared-saga mutations at the moment of the
    /// freeze, flattened from the per-transaction pending buckets. Seeded back
    /// into the fold before the tail replay so a terminal that lands in the
    /// tail for a saga prepared at or before the frontier resolves correctly.
    /// Never <see langword="null"/>; empty when the leaf holds no pending
    /// sagas.
    /// </summary>
    [Id(2)] public IReadOnlyList<LeafBaselinePendingEntry> Pending { get; set; } = Array.Empty<LeafBaselinePendingEntry>();
}
