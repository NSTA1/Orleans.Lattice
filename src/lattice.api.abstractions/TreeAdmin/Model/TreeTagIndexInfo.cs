using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A single entry in the tag-index catalog listing, describing one tag index's
/// identity, backing membership tree, and the subject trees it currently covers.
/// A pure projection with no side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeTagIndexInfo)]
[Immutable]
public sealed record TreeTagIndexInfo
{
    /// <summary>The logical tag-index name; the backing membership tree is <c>tag-{IndexName}</c>.</summary>
    [Id(0)] public required string IndexName { get; init; }

    /// <summary>The backing membership tree id (<c>tag-{IndexName}</c>).</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The number of physical shards configured for the backing membership tree.</summary>
    [Id(2)] public int ShardCount { get; init; }

    /// <summary>The subject trees this index currently covers (has membership rows for).</summary>
    [Id(3)] public ImmutableArray<string> CoveredTrees { get; init; } = ImmutableArray<string>.Empty;
}
