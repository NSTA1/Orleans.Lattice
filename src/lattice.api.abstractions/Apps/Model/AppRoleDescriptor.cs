using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>A manifest role's requested operations and app-local scopes, never composed physical ids.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppRoleDescriptor), Immutable]
public sealed record AppRoleDescriptor
{
    /// <summary>The declared role name.</summary>
    [Id(0)] public required string Name { get; init; }
    /// <summary>The requested operation mask, before intersection with the ceiling.</summary>
    [Id(1)] public LatticeOperation Operations { get; init; }
    /// <summary>The requested local or cross-app scopes, not effective authorization grants.</summary>
    [Id(2)] public ImmutableArray<AppRoleScope> Scopes { get; init; } = [];
}
