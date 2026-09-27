using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>A scope relative to <c>a/{app}/</c>, composed before the outer tenant namespace.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppScopeTemplate), Immutable]
public sealed record AppScopeTemplate
{
    /// <summary>App-local tree name; absolute paths and wildcards are not accepted.</summary>
    [Id(0)] public required string Tree { get; init; }

    /// <summary>Other app to observe or access; null means this manifest's app. Requires consent.</summary>
    [Id(1)] public AppSlug? App { get; init; }

    /// <summary>Tree, key or prefix scope, matching the ordinary authorization model.</summary>
    [Id(2)] public LatticeScopeKind Kind { get; init; } = LatticeScopeKind.Tree;

    /// <summary>Non-empty exact key or prefix for those scope kinds; null for a tree scope.</summary>
    [Id(3)] public string? KeyOrPrefix { get; init; }
}
