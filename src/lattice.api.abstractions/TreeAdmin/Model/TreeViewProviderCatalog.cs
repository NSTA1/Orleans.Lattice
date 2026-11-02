using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The runtime-view projection providers a caller may create a view with over one
/// source tree: every provider the cluster's host registered, with its description
/// and the shape of its payload. A pure projection with no side effects; provider
/// code is never run to produce it.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewProviderCatalog)]
[Immutable]
public sealed record TreeViewProviderCatalog
{
    /// <summary>
    /// The providers, ordered by key. Empty when the host registered none, in which
    /// case no view can be created at runtime.
    /// </summary>
    [Id(0)] public ImmutableArray<TreeViewProviderInfo> Providers { get; init; }
}
