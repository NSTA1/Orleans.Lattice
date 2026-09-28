namespace Orleans.Lattice.Apps;

/// <summary>
/// One enabled install as the shared app-role evaluation sees it: the registry record it was compiled from,
/// its installed manifest, and the manifest's roles compiled for the install's tenant.
/// </summary>
internal sealed class AppRoleGrantInstall
{
    /// <summary>Initializes a new <see cref="AppRoleGrantInstall"/>.</summary>
    /// <param name="record">The install record.</param>
    /// <param name="manifest">The installed manifest.</param>
    /// <param name="roles">The compiled roles, in manifest order.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public AppRoleGrantInstall(AppRegistryRecord record, AppManifest manifest, AppRoleGate[] roles)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(roles);
        Record = record;
        Manifest = manifest;
        Roles = roles;
    }

    /// <summary>The install record the roles were compiled from.</summary>
    public AppRegistryRecord Record { get; }

    /// <summary>The installed manifest.</summary>
    public AppManifest Manifest { get; }

    /// <summary>The manifest's roles compiled for the install's tenant, in manifest order.</summary>
    public AppRoleGate[] Roles { get; }
}
