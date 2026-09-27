using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The reserved tree and key layout of the app registry. The registry lives in the
/// <c>sys-app-</c> system-data namespace (<see cref="LatticeConstants.AppRegistryTreePrefix"/>),
/// so it is catalog-hidden, guarded against user-origin writes, and read-isolated from
/// data-plane grants. Keys are <c>{tenantId}/{appSlug}</c>; neither segment can contain
/// <c>/</c>, so the key is unambiguous and one tenant's installs are a single prefix scan.
/// </summary>
internal static class AppRegistryTreeNames
{
    /// <summary>The tree holding one <see cref="AppRegistryRecord"/> per install.</summary>
    internal const string RegistryTree = LatticeConstants.AppRegistryTreePrefix + "registry";

    /// <summary>The separator between the tenant and slug key segments.</summary>
    internal const char KeySeparator = '/';

    /// <summary>
    /// The character immediately after <see cref="KeySeparator"/>, used as the exclusive
    /// upper bound of a tenant prefix scan.
    /// </summary>
    private const char KeySeparatorSuccessor = (char)(KeySeparator + 1);

    /// <summary>Composes the registry key for one install.</summary>
    /// <param name="tenant">The owning tenant; must be initialised.</param>
    /// <param name="slug">The app slug; must be initialised.</param>
    /// <returns>The key <c>{tenantId}/{appSlug}</c>.</returns>
    /// <exception cref="ArgumentException">Either value is uninitialised.</exception>
    internal static string ComposeKey(TenantId tenant, AppSlug slug) =>
        string.Concat(RequireTenant(tenant), "/", RequireSlug(slug));

    /// <summary>The inclusive lower bound of a tenant's key range.</summary>
    /// <param name="tenant">The owning tenant; must be initialised.</param>
    /// <returns>The bound <c>{tenantId}/</c>.</returns>
    internal static string TenantRangeStart(TenantId tenant) => RequireTenant(tenant) + KeySeparator;

    /// <summary>The exclusive upper bound of a tenant's key range.</summary>
    /// <param name="tenant">The owning tenant; must be initialised.</param>
    /// <returns>The bound <c>{tenantId}0</c>, the first key after every <c>{tenantId}/...</c>.</returns>
    internal static string TenantRangeEnd(TenantId tenant) => RequireTenant(tenant) + KeySeparatorSuccessor;

    /// <summary>Returns the tenant text, rejecting the uninitialised "no tenant" value.</summary>
    internal static string RequireTenant(TenantId tenant) =>
        tenant.Value ?? throw new ArgumentException(
            "The uninitialised 'no tenant' value cannot address an app registry record.", nameof(tenant));

    /// <summary>Returns the slug text, rejecting the uninitialised slug.</summary>
    internal static string RequireSlug(AppSlug slug) =>
        slug.Value ?? throw new ArgumentException(
            "The uninitialised app slug cannot address an app registry record.", nameof(slug));
}
