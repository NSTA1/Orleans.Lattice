namespace Orleans.Lattice;

/// <summary>
/// The per-silo seam that resolves the active <see cref="TenantId"/> for the
/// current operation from ambient context. The interface lives in core so later
/// tenant-aware choke points can resolve the active tenant without depending on
/// the tenancy add-on; the core library ships only the
/// <see cref="NullTenantContextResolver"/> fallback (which always resolves to
/// the reserved <see cref="TenantId.Default"/> tenant, so core behaves
/// byte-for-byte as it did before tenancy existed), and the real
/// context-reading implementation is contributed by the tenancy package.
/// </summary>
public interface ITenantContextResolver
{
    /// <summary>
    /// Resolves the active tenant for the current operation. Returns
    /// <see cref="TenantId.Default"/> when no tenant is present in context.
    /// </summary>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved tenant, or <see cref="TenantId.Default"/>.</returns>
    ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to resolve the active tenant <em>synchronously</em>, from ambient
    /// context alone with no asynchronous work. Lets a tenant-aware choke point
    /// skip the async path on the warm case.
    /// </summary>
    /// <param name="tenant">
    /// The resolved tenant when this returns <see langword="true"/>; otherwise
    /// <c>default</c>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the tenant was resolved synchronously;
    /// <see langword="false"/> when an asynchronous resolution via
    /// <see cref="ResolveCurrentAsync"/> is required. The default implementation
    /// always returns <see langword="false"/>, so a resolver that cannot resolve
    /// synchronously safely falls back to the async path.
    /// </returns>
    bool TryResolveCurrent(out TenantId tenant)
    {
        tenant = default;
        return false;
    }
}
