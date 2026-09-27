namespace Orleans.Lattice.Apps;

/// <summary>A flat role's requested operations and scopes, later bound to a membership group.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppRoleDeclaration)]
public sealed record AppRoleDeclaration
{
    /// <summary>Unique app-local role name.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>
    /// Requested tree-scoped operation mask, represented in JSON as an array of individual names.
    /// Scopeless Telemetry and AppInstall are forbidden; other requests still require install-time consent.
    /// </summary>
    [Id(1)] public required LatticeOperation Operations { get; init; }

    /// <summary>Non-empty list of structural scope templates; never executable interpolation.</summary>
    [Id(2)] public required AppScopeTemplate[] Scopes { get; init; }
}
