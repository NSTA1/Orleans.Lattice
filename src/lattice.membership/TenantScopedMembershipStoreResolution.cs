using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Resolves the <see cref="ITenantScopedMembershipStore"/> from a service provider.
/// The store is the registered <see cref="ILatticeMembershipDirectory"/> itself
/// (the default directory implements both), so no separate registration exists
/// that could drift from the directory a host actually runs.
/// </summary>
internal static class TenantScopedMembershipStoreResolution
{
    /// <summary>
    /// Returns the registered <see cref="ILatticeMembershipDirectory"/> as an
    /// <see cref="ITenantScopedMembershipStore"/>.
    /// </summary>
    /// <param name="services">The service provider. Must not be <c>null</c>.</param>
    /// <returns>The tenant-scoped store.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// No membership directory is registered, or the registered directory does not
    /// implement the tenant-scoped operations (a host replaced it). Fails closed
    /// rather than running the tenant tier without its invariants.
    /// </exception>
    internal static ITenantScopedMembershipStore GetTenantScopedMembershipStore(this IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var directory = services.GetService<ILatticeMembershipDirectory>()
            ?? throw new InvalidOperationException(
                "No ILatticeMembershipDirectory is registered. Call AddLatticeMembership() before using the tenant tier.");

        return directory as ITenantScopedMembershipStore
            ?? throw new InvalidOperationException(
                $"The registered ILatticeMembershipDirectory ({directory.GetType().FullName}) does not support the "
                + "tenant-scoped membership operations the tenant tier requires. Use the default membership directory "
                + "registered by AddLatticeMembership().");
    }
}
