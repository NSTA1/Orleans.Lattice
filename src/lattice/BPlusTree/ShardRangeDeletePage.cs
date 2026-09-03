namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One bounded batch of a shard-level range delete (see
/// <see cref="Grains.IShardRootGrain.DeleteRangeBoundedAsync"/>).
/// <para>
/// The shard tombstones matching entries across a bounded number of leaves and
/// then returns, releasing the non-reentrant shard so other traffic can
/// interleave. <see cref="ResumeFromInclusive"/> is the key the caller passes
/// back as the next batch's <c>startInclusive</c>, or <see langword="null"/>
/// when this shard's portion of the range is complete.
/// </para>
/// <para>
/// The resume position is a <b>key</b>, never a leaf grain id. Orleans grains
/// are virtual, so a leaf reclaimed between two batches would silently
/// activate as an empty grain with no siblings and the walk would conclude it
/// had reached the end of the chain, quietly leaving part of the range
/// undeleted. A key is re-descended to whatever leaf now holds it (issue 1955).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardRangeDeletePage)]
[Immutable]
internal readonly record struct ShardRangeDeletePage
{
    /// <summary>Live entries tombstoned by this batch.</summary>
    [Id(0)] public int Deleted { get; init; }

    /// <summary>
    /// The key to resume from, as the next batch's inclusive lower bound, or
    /// <see langword="null"/> when this shard has no more of the range to
    /// delete. A non-null value is always strictly greater than the
    /// <c>startInclusive</c> the batch was called with, so the walk cannot
    /// stall re-deleting the same leaves.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
