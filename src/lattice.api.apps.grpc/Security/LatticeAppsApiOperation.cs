namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>App-control operations classified by the transport, not by caller-supplied data.</summary>
public enum LatticeAppsApiOperation
{
    /// <summary>An unknown or mismatched request; always rejected.</summary>
    Unknown = 0,
    /// <summary>Install an app version with consent.</summary>
    Install = 1,
    /// <summary>Enable an installed app.</summary>
    Enable = 2,
    /// <summary>Disable an installed app.</summary>
    Disable = 3,
    /// <summary>Uninstall without physical purge.</summary>
    Uninstall = 4,
    /// <summary>List visible installed apps.</summary>
    List = 5,
    /// <summary>Inspect a manifest without loading code.</summary>
    Describe = 6,
    /// <summary>Read pinned consent.</summary>
    GetConsent = 7,
    /// <summary>Replace version-pinned consent.</summary>
    UpdateConsent = 8,
    /// <summary>Probe advisory permissions.</summary>
    GetCapabilities = 9,
    /// <summary>List the configured app sources (catalogue).</summary>
    ListSources = 10,
    /// <summary>List the apps the sources make available (catalogue).</summary>
    ListAvailable = 11,
    /// <summary>Describe an app version from a source before install (catalogue).</summary>
    DescribeFromSource = 12,
    /// <summary>Read an app version's icon from a source before install (catalogue).</summary>
    GetSourceIcon = 13,
    /// <summary>Probe advisory catalogue permissions.</summary>
    GetCatalogCapabilities = 14,
    /// <summary>List the caller's apps (workspace).</summary>
    ListMyApps = 15,
    /// <summary>Describe one of the caller's apps (workspace).</summary>
    DescribeMyApp = 16,
    /// <summary>Read one of the caller's apps' icon (workspace).</summary>
    GetMyAppIcon = 17,
    /// <summary>Read one UI bundle asset of one of the caller's apps (workspace).</summary>
    GetUiAsset = 18,
}
