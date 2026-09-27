namespace Orleans.Lattice.Api.Apps;

/// <summary>Advisory caller permissions for the app control surface; every flag defaults to denied.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.LatticeAppsCapabilities), Immutable]
public sealed record LatticeAppsCapabilities
{
    /// <summary>Whether the caller may install apps.</summary>
    [Id(0)] public bool CanInstall { get; init; }
    /// <summary>Whether the caller may enable installed apps.</summary>
    [Id(1)] public bool CanEnable { get; init; }
    /// <summary>Whether the caller may disable installed apps.</summary>
    [Id(2)] public bool CanDisable { get; init; }
    /// <summary>Whether the caller may uninstall apps without physical purge.</summary>
    [Id(3)] public bool CanUninstall { get; init; }
    /// <summary>Whether the caller may list app installations.</summary>
    [Id(4)] public bool CanList { get; init; }
    /// <summary>Whether the caller may inspect manifests and installation metadata.</summary>
    [Id(5)] public bool CanDescribe { get; init; }
    /// <summary>Whether the caller may read version-pinned consent.</summary>
    [Id(6)] public bool CanGetConsent { get; init; }
    /// <summary>Whether the caller may replace version-pinned consent.</summary>
    [Id(7)] public bool CanUpdateConsent { get; init; }
}
