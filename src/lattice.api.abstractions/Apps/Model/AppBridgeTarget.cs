namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The only address an app UI may use for data: its app slug, the install revision it
/// was launched against, and an app-local tree name. It never carries a physical tree id.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppBridgeTarget), Immutable]
public sealed record AppBridgeTarget
{
    /// <summary>The app slug whose owned tree is addressed.</summary>
    [Id(0)] public required string AppSlug { get; init; }
    /// <summary>The install revision the frame was launched against; a stale revision is refused.</summary>
    [Id(1)] public long InstallRevision { get; init; }
    /// <summary>The app-local tree name, resolved to its effective tree server-side.</summary>
    [Id(2)] public required string LogicalTree { get; init; }
}
