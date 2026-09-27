namespace Orleans.Lattice.Apps;

/// <summary>The lifecycle transitions the app registry applies.</summary>
internal enum AppLifecycleAction
{
    /// <summary>Record a new install.</summary>
    Install,

    /// <summary>Re-consent a live install with a new (or the same) version and ceiling.</summary>
    Upgrade,

    /// <summary>Activate an install.</summary>
    Enable,

    /// <summary>Deactivate an enabled install.</summary>
    Disable,

    /// <summary>Retire an install, keeping its record.</summary>
    Uninstall,
}
