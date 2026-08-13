using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The result of listing the cluster's tag indexes: one <see cref="TreeTagIndexInfo"/>
/// per index discovered in the tree registry (its backing membership tree carries
/// the reserved <c>tag-</c> prefix). A pure projection with no side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeTagIndexCatalog)]
[Immutable]
public sealed record TreeTagIndexCatalog
{
    /// <summary>The tag indexes registered on this cluster, in ascending index-name order.</summary>
    [Id(0)] public ImmutableArray<TreeTagIndexInfo> Indexes { get; init; } = ImmutableArray<TreeTagIndexInfo>.Empty;
}
