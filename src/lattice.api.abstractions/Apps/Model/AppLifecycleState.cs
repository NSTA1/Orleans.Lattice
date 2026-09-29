namespace Orleans.Lattice.Api.Apps;

/// <summary>Wire-visible installation lifecycle, independent of the registry implementation.</summary>
public enum AppLifecycleState
{
    /// <summary>
    /// A source version without a matching installation, returned only by
    /// <see cref="ILatticeAppsControl.DescribeAsync"/>. Never returned by list or lifecycle mutations.
    /// </summary>
    NotInstalled = 0,
    /// <summary>The app is installed but has not been enabled.</summary>
    Installed = 1,
    /// <summary>The app is enabled.</summary>
    Enabled = 2,
    /// <summary>The installed app is disabled.</summary>
    Disabled = 3,
    /// <summary>The app was uninstalled; this does not imply physical data purge.</summary>
    Uninstalled = 4,
    /// <summary>
    /// An installed app's last activation evidence records a failed run. Inspection-only:
    /// <see cref="ILatticeAppsControl.DescribeAsync"/> and
    /// <see cref="ILatticeAppsControl.ListAsync"/> may report this when failure evidence
    /// is available; implementations must not infer it from a disabled registry state,
    /// and the underlying install may still be enabled. Lifecycle mutations report
    /// failures by exception, not by returning this state.
    /// </summary>
    Failed = 5,
}
