namespace Orleans.Lattice.Apps;

/// <summary>
/// The lifecycle state of an app install recorded in the app registry. The legal
/// transitions are: absent or <see cref="Uninstalled"/> to <see cref="Installed"/>
/// (install); <see cref="Installed"/> or <see cref="Disabled"/> to
/// <see cref="Enabled"/> (enable); <see cref="Enabled"/> to <see cref="Disabled"/>
/// (disable); and any installed state to <see cref="Uninstalled"/> (uninstall).
/// Every transition is gated on <c>LatticeOperation.AppInstall</c>.
/// </summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppRegistryLifecycleState)]
public enum AppRegistryLifecycleState
{
    /// <summary>
    /// The app is registered with a pinned capability ceiling and role bindings, but
    /// is not yet active.
    /// </summary>
    Installed = 1,

    /// <summary>The app is active: its compiled roles and surfaces are in effect.</summary>
    Enabled = 2,

    /// <summary>The app was enabled and has been administratively deactivated; its record is retained.</summary>
    Disabled = 3,

    /// <summary>
    /// The app was uninstalled. The record is retained (not deleted) so the install
    /// history stays auditable by prefix scan and its revision never regresses on a
    /// later re-install.
    /// </summary>
    Uninstalled = 4,
}
