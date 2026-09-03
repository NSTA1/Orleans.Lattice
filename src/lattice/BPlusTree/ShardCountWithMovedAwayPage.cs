namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One bounded batch of a shard-level count that also reports the moved-away
/// virtual slots it observed (see
/// <see cref="Grains.IShardRootGrain.CountWithMovedAwayBoundedAsync"/>).
/// <para>
/// The caller sums <see cref="Count"/> across batches and <b>unions</b>
/// <see cref="MovedAwaySlots"/>, so the completed walk reports exactly what the
/// single unbounded walk did. Unioning matters: a slot observed only in a later
/// batch is just as real as one seen in the first, so taking the last batch's
/// set would silently lose slots.
/// </para>
/// <para>
/// The resume position is a <b>key</b>, never a leaf grain id, for the reason
/// set out on <see cref="ShardCountPage"/> (issue 1955).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardCountWithMovedAwayPage)]
[Immutable]
internal readonly record struct ShardCountWithMovedAwayPage
{
    /// <summary>Live, still-owned entries counted by this batch.</summary>
    [Id(0)] public int Count { get; init; }

    /// <summary>
    /// The distinct moved-away virtual slots observed in this batch, ascending,
    /// or <see langword="null"/> when none were seen. Union these across
    /// batches.
    /// </summary>
    [Id(1)] public int[]? MovedAwaySlots { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when this shard's
    /// chain is exhausted.
    /// </summary>
    [Id(2)] public string? ResumeFromInclusive { get; init; }
}
