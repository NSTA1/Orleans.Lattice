namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One work-bounded batch of a shard materialiser-lag walk (see
/// <see cref="IShardRootGrain.GetShardMaterialiserLagBoundedAsync"/>).
/// <para>
/// The shard reduces the projection checkpoint across a bounded number of
/// leaves and then returns, releasing the non-reentrant shard so other traffic
/// can interleave. The caller takes the minimum of
/// <see cref="MinCheckpointOffset"/> across the batches and subtracts it from
/// the WAL heads to obtain the shard's lag, which is the same reduction the
/// single-call walk performed (issue 1972).
/// </para>
/// <para>
/// <b>The WAL heads are captured once, on the first batch.</b> Lag is
/// <c>head - checkpoint</c>, so a head re-read on a later batch would be
/// measured against checkpoints gathered earlier and inflate the reported lag
/// by whatever the tree committed mid-walk - turning a bounded walk into a
/// false alarm. <see cref="WalHeadOffsets"/> is therefore populated only on the
/// batch requested with a <see langword="null"/> resume position, and is empty
/// on a resumed one; the driver keeps the first batch's array.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardMaterialiserLagPage)]
[Immutable]
internal readonly record struct ShardMaterialiserLagPage
{
    /// <summary>
    /// The per-partition WAL head offsets, captured once at the start of the
    /// walk. Populated on the first batch only; empty on a resumed batch.
    /// </summary>
    [Id(0)] public long[] WalHeadOffsets { get; init; }

    /// <summary>
    /// The lowest projection checkpoint offset observed across the leaves this
    /// batch visited, or <see cref="long.MaxValue"/> when it visited none. The
    /// driver reduces these with <c>min</c>; a walk that visited no leaf at all
    /// leaves <see cref="long.MaxValue"/> standing, which the driver reads as
    /// "no projection state exists" and reports the heads themselves as the
    /// lag - the empty-shard answer the single-call walk gave.
    /// </summary>
    [Id(1)] public long MinCheckpointOffset { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when this shard's
    /// chain has been walked to its end.
    /// </summary>
    [Id(2)] public string? ResumeFromInclusive { get; init; }
}
