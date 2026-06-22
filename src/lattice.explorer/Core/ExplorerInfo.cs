namespace Orleans.Lattice.Explorer.Core;

/// <summary>
/// Static metadata about the Orleans.Lattice.Explorer application, shared by
/// every head (Windows desktop and web) through the common UI layer.
/// </summary>
public static class ExplorerInfo
{
    /// <summary>The product / application name shown in the UI shell.</summary>
    public const string ApplicationName = "Orleans.Lattice.Explorer";

    /// <summary>
    /// One-line description of what the application does, suitable for a
    /// window subtitle or about box.
    /// </summary>
    public const string Description =
        "Read-only explorer for an Orleans.Lattice cluster, over the state API.";
}
