using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps;

/// <summary>A requested role scope using an app-local tree name, never a composed physical id.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppRoleScope), Immutable]
public sealed record AppRoleScope
{
    /// <summary>The manifest's app-local tree name.</summary>
    [Id(0)] public required string Tree { get; init; }
    /// <summary>The target app slug; null means the described app.</summary>
    [Id(1)] public string? App { get; init; }
    /// <summary>Whole-tree, exact-key, or key-prefix extent.</summary>
    [Id(2)] public LatticeScopeKind Kind { get; init; } = LatticeScopeKind.Tree;
    /// <summary>The exact key or prefix; null for a whole-tree scope.</summary>
    [Id(3)] public string? KeyOrPrefix { get; init; }
}
