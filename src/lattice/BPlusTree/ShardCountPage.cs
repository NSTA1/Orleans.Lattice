namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One bounded batch of a shard-level count (see
/// <see cref="Grains.IShardRootGrain.CountBoundedAsync"/>).
/// <para>
/// The shard counts matching entries across a bounded number of leaves and then
/// returns, releasing the non-reentrant shard so other traffic can interleave.
/// <see cref="ResumeFromInclusive"/> is the key the caller passes back as the
/// next batch's <c>startInclusive</c>, or <see langword="null"/> when this
/// shard's portion of the range is complete. The caller sums the batches, so
/// the logical count is unchanged - only the number of shard calls it takes
/// differs (issue 1971).
/// </para>
/// <para>
/// A partial count is a <b>wrong</b> answer rather than a short one, which is
/// why this carries a resume position instead of a "has more" flag: the caller
/// must be unable to mistake a bounded batch for a complete result.
/// </para>
/// <para>
/// The resume position is a <b>key</b>, never a leaf grain id. Orleans grains
/// are virtual, so a leaf reclaimed between two batches would silently activate
/// as an empty grain with no siblings and the walk would conclude it had
/// reached the end of the chain, quietly under-counting. A key is re-descended
/// to whatever leaf now holds it (issue 1955).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardCountPage)]
[Immutable]
internal readonly record struct ShardCountPage
{
    /// <summary>Live entries counted by this batch.</summary>
    [Id(0)] public int Count { get; init; }

    /// <summary>
    /// The key to resume from, as the next batch's inclusive lower bound, or
    /// <see langword="null"/> when this shard has no more of the range to
    /// count. A non-null value is always strictly greater than the
    /// <c>startInclusive</c> the batch was called with, so the walk cannot
    /// stall re-counting the same leaves - which would also double-count them.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
