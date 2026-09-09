namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Wire-level carrier for the atomic
/// <c>(Decisions snapshot, decisions revision)</c> pair returned by
/// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.SnapshotWithRevisionAsync"/>. Both fields
/// are captured inside the registry's single-turn token in the same
/// synchronous code block, so the returned <see cref="Revision"/> is
/// guaranteed to be the revision that produced the returned
/// <see cref="Decisions"/> dictionary (no skew). Used by the
/// reader-side double-checked retry in <c>LatticeGrain</c> to feed the
/// cheap <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.GetDecisionsRevisionAsync"/>
/// stability probe.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistrySnapshot)]
[Immutable]
internal readonly record struct TxRegistrySnapshot
{
    /// <summary>
    /// Defensive copy of the registry's recorded-decisions dictionary
    /// at the moment the snapshot was captured. Expired tombstones are
    /// filtered out so the dictionary reflects observable status
    /// (consistent with <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.GetStatusAsync"/>).
    /// </summary>
    [Id(0)] public Dictionary<Guid, TxStatus> Decisions { get; init; }

    /// <summary>
    /// Monotonic comparison token for the readable surface that produced
    /// <see cref="Decisions"/>, stamped at the same instant and against
    /// the same retention window as the mask that filtered it. A
    /// subsequent
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.GetDecisionsRevisionAsync"/>
    /// probe returning the same value is proof that the surface did not
    /// change in the intervening window - which covers a decision
    /// mutation and, because the token folds in a live-expired tombstone
    /// count, also covers a tombstone silently ageing past its retention
    /// boundary with no write at all. Opaque: compare it, never do
    /// arithmetic on it.
    /// </summary>
    [Id(1)] public long Revision { get; init; }
}
