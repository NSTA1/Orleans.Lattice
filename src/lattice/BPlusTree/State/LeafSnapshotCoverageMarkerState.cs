namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// The persisted state of a leaf's kept-snapshot coverage marker sidecar
/// (<see cref="ILeafSnapshotCoverageMarkerGrain"/>, issue #4634).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotCoverageMarkerState)]
internal sealed class LeafSnapshotCoverageMarkerState : ILatticeBinaryPersistedState
{
    /// <summary>
    /// The highest WAL offset, per partition, a snapshot the leaf's store kept
    /// has ever covered; <c>-1</c> or an absent slot where none has.
    /// <see langword="null"/> when no snapshot was ever kept.
    /// </summary>
    [Id(0)] public long[]? CoveredOffsetsByPartition { get; set; }
}
