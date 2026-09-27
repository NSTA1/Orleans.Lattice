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
}
