namespace Orleans.Lattice.Api.Apps;

/// <summary>Wire-visible installation lifecycle, independent of the registry implementation.</summary>
public enum AppLifecycleState
{
    /// <summary>The described source version has no matching installation.</summary>
    NotInstalled = 0,
    /// <summary>The app is installed but has not been enabled.</summary>
    Installed = 1,
    /// <summary>The app is enabled.</summary>
    Enabled = 2,
    /// <summary>The installed app is disabled.</summary>
    Disabled = 3,
    /// <summary>The app was uninstalled; this does not imply physical data purge.</summary>
    Uninstalled = 4,
    /// <summary>The app failed activation and is not enabled.</summary>
    Failed = 5,
}
