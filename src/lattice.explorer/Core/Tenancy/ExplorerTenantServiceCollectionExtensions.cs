using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// Registration helper that opts the Explorer into tenant-aware view scoping.
/// </summary>
/// <remarks>
/// This is opt-in. When it is <em>not</em> called, no
/// <see cref="IExplorerTenantView"/> is registered, so
/// <see cref="Catalog.CatalogReader"/> resolves its optional dependency to the
/// inactive <see cref="NullExplorerTenantView"/> and every listing is byte-for-byte
/// identical to a non-tenant cluster. A head calls
/// <see cref="AddExplorerTenantView"/> only when it also establishes the caller's
/// active tenant on <see cref="IExplorerTenantContext"/>; until an active tenant is
/// set, the active view fails closed to the per-tenant default (revealing nothing
/// beyond the caller's own tenant, and nothing at all for a caller with no active
/// tenant).
/// </remarks>
public static class ExplorerTenantServiceCollectionExtensions
{
    /// <summary>
    /// Registers the fail-closed tenant-view seam - the per-circuit tenant context,
    /// the platform-operator gate, the active
    /// <see cref="IExplorerTenantView"/>, the identity-to-tenant resolver that
    /// establishes the caller's active tenant from their sign-in, and the
    /// operator-gated tenant switcher behind the shell's tenant selector - so the
    /// Explorer scopes its listings to the caller's active tenant and grants the
    /// all-tenant view only to a validated platform operator. Registrations are
    /// scoped per Blazor circuit so each connection carries its own active tenant.
    /// <para>
    /// The operator gate is registered with <c>TryAdd</c> and defaults to the
    /// fail-closed <see cref="DeniedExplorerTenantOperatorGate"/>, because a real
    /// platform-operator signal is a probed decision owned by the plugin that
    /// performs the probe rather than by the navigation core. Call this
    /// <em>after</em> the administrative surface that supplies one (the Access
    /// feature registers a gate backed by its own administrator decision), so the
    /// real gate wins the <c>TryAdd</c>.
    /// </para>
    /// </summary>
    /// <remarks>
    /// The identity resolver and switcher exist only when this method is called, so
    /// they light up on exactly the same switch as the active
    /// <see cref="IExplorerTenantView"/> - there is no separate opt-in flag - and a
    /// non-tenant deployment registers none of them, leaving the Explorer UI
    /// byte-for-byte unchanged. The resolver is registered with <c>TryAdd</c>, so a
    /// production multi-tenant head can register its own
    /// <see cref="IExplorerTenantIdentityResolver"/> (reading its identity
    /// provider's tenant claim) before this call to replace the single-tenant
    /// default.
    /// </remarks>
    /// <param name="services">The service collection to add to. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddExplorerTenantView(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IExplorerTenantContext, ExplorerTenantContext>();
        services.TryAddScoped<IExplorerTenantOperatorGate>(_ => DeniedExplorerTenantOperatorGate.Instance);
        services.TryAddScoped<IExplorerTenantView, ExplorerTenantView>();
        services.TryAddScoped<IExplorerTenantIdentityResolver, DefaultExplorerTenantIdentityResolver>();
        services.TryAddScoped<IExplorerTenantSwitcher, ExplorerTenantSwitcher>();

        return services;
    }
}
