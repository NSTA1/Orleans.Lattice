namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One work-bounded batch of a shard leaf-footprint re-anchor walk (see
/// <see cref="IShardRootGrain.RefreshLeafByteFootprintsBoundedAsync"/>).
/// <para>
/// The shard sums per-leaf byte footprints across a bounded number of leaves
/// and then returns, releasing the non-reentrant shard so other traffic can
/// interleave. The caller sums <see cref="Usage"/> across the batches to obtain
/// the shard total, which is the figure the single-call walk returned
/// (issue 1972).
/// </para>
/// <para>
/// <b>The activation-scoped running totals are re-anchored only on the final
/// batch</b> - the one that reports no <see cref="ResumeFromInclusive"/> - and
/// only from the accumulated total the driver hands back, never from a batch's
/// partial sum. Re-anchoring per batch would leave the shard advertising a
/// fraction of its own footprint to every concurrent reader for the rest of the
/// walk, which is worse than the staleness the re-anchor exists to remove. If
/// the shard deactivates mid-walk the re-anchor simply does not happen and the
/// totals rebuild from leaf publishes, which is the documented behaviour of a
/// freshly-reactivated shard.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardStorageUsagePage)]
[Immutable]
internal readonly record struct ShardStorageUsagePage
{
    /// <summary>
    /// The byte footprint of the leaves this batch visited. A batch total, not
    /// a shard total.
    /// </summary>
    [Id(0)] public ShardStorageUsage Usage { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when this shard's
    /// chain has been walked to its end.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
