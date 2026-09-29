namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>A lifecycle action on an installed app.</summary>
internal enum AppLifecycleVerb
{
    /// <summary>Enable the installed app.</summary>
    Enable,

    /// <summary>Disable the installed app.</summary>
    Disable,

    /// <summary>Uninstall the app.</summary>
    Uninstall,

    /// <summary>Start the upgrade flow to a newer version.</summary>
    Upgrade,
}
