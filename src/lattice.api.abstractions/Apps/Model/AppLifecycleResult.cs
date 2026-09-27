namespace Orleans.Lattice.Api.Apps;

/// <summary>The outcome of an app lifecycle operation, with no physical tree identifiers.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppLifecycleResult), Immutable]
public sealed record AppLifecycleResult
{
    /// <summary>The affected app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The affected installation's exact version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>
    /// The resulting Installed, Enabled, Disabled, or Uninstalled state.
    /// NotInstalled and Failed are inspection-only states and never mutation results;
    /// unsuccessful mutations throw rather than return a success-shaped result.
    /// </summary>
    [Id(2)] public AppLifecycleState State { get; init; }
    /// <summary>Whether this operation changed the installation.</summary>
    [Id(3)] public bool Changed { get; init; }
}
