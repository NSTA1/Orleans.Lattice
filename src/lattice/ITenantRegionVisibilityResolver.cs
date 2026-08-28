namespace Orleans.Lattice;

/// <summary>
/// The seam that resolves one tenant's per-region standing - its
/// operator-authorized <b>allowed</b> set and its per-region residency status - so
/// a region-discovery surface can prune the regions a tenant caller has no
/// relationship with and annotate the ones it keeps. The core library ships only
/// the no-op default (which reports itself inactive and resolves nothing), and the
/// real, registry-backed implementation is contributed by the tenancy package.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tenancy off costs nothing.</b> A discovery choke point reads
/// <see cref="IsActive"/> (or, cheaper still, short-circuits on the absence of an
/// ambient active tenant) before it resolves this service at all, so a cluster
/// with no tenancy add-on never calls <see cref="ResolveAsync"/> and its region
/// list is returned byte-for-byte unchanged, on the same allocation-free path.
/// </para>
/// <para>
/// <b>Fail-closed.</b> A resolver that cannot establish the tenant's standing must
/// return <see cref="TenantRegionVisibilityMap.Unresolved"/> rather than an empty
/// resolved map or a permissive one, so the choke point degrades to the
/// tenant-scoped minimal answer instead of disclosing the full topology.
/// </para>
/// </remarks>
public interface ITenantRegionVisibilityResolver
{
    /// <summary>
    /// Returns <c>true</c> when a tenancy engine is wired in and can answer
    /// per-tenant region standing; <c>false</c> for the core no-op default. The
    /// result is expected to be cheap and stable so a choke point can cache it.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Resolves <paramref name="tenant"/>'s standing in every region it is allowed
    /// into or carries a residency status for. Invoked only when
    /// <see cref="IsActive"/> is <c>true</c> and a non-default tenant is asserted.
    /// </summary>
    /// <param name="tenant">The tenant whose region standing is being resolved.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>
    /// The tenant's per-region standing, or
    /// <see cref="TenantRegionVisibilityMap.Unresolved"/> when it could not be
    /// established (fail-closed). Never <c>null</c>.
    /// </returns>
    ValueTask<TenantRegionVisibilityMap> ResolveAsync(
        TenantId tenant, CancellationToken cancellationToken = default);
}
