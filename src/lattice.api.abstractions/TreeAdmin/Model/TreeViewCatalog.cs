using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The result of listing the cluster's runtime-registered materialised views: a
/// snapshot of every durable runtime-view registration. A pure projection with no
/// side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewCatalog)]
[Immutable]
public sealed record TreeViewCatalog
{
    /// <summary>
    /// The runtime-registered views, in the order the registry returned them. Empty
    /// when no runtime views are registered on the cluster.
    /// </summary>
    [Id(0)] public ImmutableArray<TreeViewInfo> Views { get; init; }
}
