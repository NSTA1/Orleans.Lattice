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
    /// the capability-backed platform-operator gate, and the active
    /// <see cref="IExplorerTenantView"/> - so the Explorer scopes its listings to
    /// the caller's active tenant and grants the all-tenant view only to a validated
    /// platform operator. Call after <c>AddExplorerCatalog</c> (which registers the
    /// capability store the operator gate reads). Registrations are scoped per Blazor
    /// circuit so each connection carries its own active tenant.
    /// </summary>
    /// <param name="services">The service collection to add to. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddExplorerTenantView(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IExplorerTenantContext, ExplorerTenantContext>();
        services.TryAddScoped<IExplorerTenantOperatorGate, CapabilityExplorerTenantOperatorGate>();
        services.TryAddScoped<IExplorerTenantView, ExplorerTenantView>();

        return services;
    }
}
