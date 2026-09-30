namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The internal extension of <see cref="ITenantResidencyResolver"/> that lets the
/// tenant gate and the replication isolation gate tell an authoritative residency
/// answer from one read off a snapshot that may be stale, and confirm the latter
/// against the tenant registry (issue #4051). Implemented by
/// <see cref="TenantResidencyResolver"/>; a resolver that does not implement it is
/// consulted through the public synchronous seam as before.
/// </summary>
internal interface ITenantResidencyConfirmation
{
    /// <summary>
    /// Answers from the in-memory residency view when, and only when, that view is
    /// authoritative. Allocation-free.
    /// </summary>
    /// <param name="tenant">The tenant to test.</param>
    /// <param name="online">The answer when this returns <c>true</c>; otherwise <c>false</c>.</param>
    /// <returns><c>true</c> when <paramref name="online"/> is authoritative; <c>false</c> when the caller must confirm.</returns>
    bool TryResolveOnline(TenantId tenant, out bool online);

    /// <summary>
    /// Confirms against the tenant's authoritative registry record whether it is
    /// online in this serving region. An unregistered tenant is not online.
    /// </summary>
    /// <param name="tenant">The tenant to test.</param>
    /// <param name="cancellationToken">Cancels the registry read.</param>
    /// <returns><c>true</c> when the tenant is online in this serving region.</returns>
    ValueTask<bool> ConfirmOnlineAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether a tenant is online in this serving region according to its registry
    /// <paramref name="record"/>, by the same rule the residency snapshot applies: an
    /// unconfigured tenant is online everywhere; a configured one only where its
    /// status is <see cref="TenantRegionStatus.Online"/>.
    /// </summary>
    /// <param name="record">The tenant's authoritative registry record.</param>
    /// <returns><c>true</c> when the tenant is online in this serving region.</returns>
    bool IsOnline(TenantRecord record);
}
