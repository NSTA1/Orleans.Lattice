namespace Orleans.Lattice.Apps;

/// <summary>
/// The admin audit anchor of the app registry: the installed-app record whose write
/// is the commit point of every app lifecycle mutation (#3638).
/// </summary>
internal static class AppAuditAnchors
{
    /// <summary>The registry record of <paramref name="slug"/> for <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="slug">The app slug.</param>
    internal static LatticeAdminAuditAnchor App(TenantId tenant, AppSlug slug) =>
        new(AppRegistryTreeNames.RegistryTree, AppRegistryTreeNames.ComposeKey(tenant, slug));
}
