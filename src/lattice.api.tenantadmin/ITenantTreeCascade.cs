using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Internal seam that cascades a tenant delete to the tenant's trees: it
/// enumerates every tree owned by the tenant (the tenant's
/// <c>t/{tenantId}/*</c> namespace) and soft-deletes each one, returning the
/// count deleted. Abstracted behind an interface so the facade's delete-with-
/// cascade logic is unit-testable with an injected fake, while the production
/// implementation reaches the real grain surface (and is exercised by
/// integration tests).
/// </summary>
internal interface ITenantTreeCascade
{
    /// <summary>
    /// Soft-deletes every tree owned by <paramref name="tenant"/> and returns the
    /// number of trees deleted.
    /// </summary>
    /// <param name="tenant">The tenant whose trees to cascade-delete. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the cascade.</param>
    /// <returns>The number of the tenant's trees that were soft-deleted.</returns>
    Task<int> DeleteTenantTreesAsync(TenantId tenant, CancellationToken cancellationToken = default);
}
