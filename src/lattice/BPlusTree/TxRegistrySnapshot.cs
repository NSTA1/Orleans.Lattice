namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Wire-level carrier for the atomic
/// <c>(Decisions snapshot, decisions revision)</c> pair returned by
/// <see cref="ITxRegistryGrain.SnapshotWithRevisionAsync"/>. Both fields
/// are captured inside the registry's single-turn token in the same
/// synchronous code block, so the returned <see cref="Revision"/> is
/// guaranteed to be the revision that produced the returned
/// <see cref="Decisions"/> dictionary (no skew). Used by the
/// reader-side double-checked retry in <c>LatticeGrain</c> to feed the
/// cheap <see cref="ITxRegistryGrain.GetDecisionsRevisionAsync"/>
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
    /// (consistent with <see cref="ITxRegistryGrain.GetStatusAsync"/>).
    /// </summary>
    [Id(0)] public Dictionary<Guid, TxStatus> Decisions { get; init; }

    /// <summary>
    /// Monotonic revision counter value at the moment
    /// <see cref="Decisions"/> was captured. A subsequent
    /// <see cref="ITxRegistryGrain.GetDecisionsRevisionAsync"/> probe
    /// returning the same value is proof that no decision mutation
    /// occurred in the intervening window.
    /// </summary>
    [Id(1)] public long Revision { get; init; }
}
