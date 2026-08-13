using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Read-only status of a single tag index: its identity, backing membership tree,
/// the subject trees it currently covers, and whether its background reconciliation
/// coordinator is idle. A pure projection with no side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeTagIndexStatus)]
[Immutable]
public sealed record TreeTagIndexStatus
{
    /// <summary>The logical tag-index name; the backing membership tree is <c>tag-{IndexName}</c>.</summary>
    [Id(0)] public required string IndexName { get; init; }

    /// <summary>The backing membership tree id (<c>tag-{IndexName}</c>).</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The number of physical shards configured for the backing membership tree.</summary>
    [Id(2)] public int ShardCount { get; init; }

    /// <summary>The subject trees this index currently covers (has membership rows for).</summary>
    [Id(3)] public ImmutableArray<string> CoveredTrees { get; init; } = ImmutableArray<string>.Empty;

    /// <summary>
    /// Whether the index's background reconciliation coordinator is idle - <c>true</c>
    /// when no sweep has started or the last one ran to completion, <c>false</c> while
    /// a sweep is in flight.
    /// </summary>
    [Id(4)] public bool ReconcileIdle { get; init; }
}
