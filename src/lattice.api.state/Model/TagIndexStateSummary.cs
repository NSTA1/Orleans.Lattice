namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only summary of a tag-index membership tree as surfaced by the cluster
/// state API's tag-index discovery endpoint
/// (<see cref="ILatticeStateQuery.ListTagIndexesAsync"/>).
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TagIndexStateSummary)]
[Immutable]
public sealed record TagIndexStateSummary
{
    /// <summary>
    /// The logical index name (the membership tree id with its reserved
    /// <c>tag-</c> prefix removed).
    /// </summary>
    [Id(0)] public required string IndexName { get; init; }

    /// <summary>The backing membership tree id (<c>tag-{indexName}</c>).</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The number of physical shards configured for the membership tree.</summary>
    [Id(2)] public int ShardCount { get; init; }
}
