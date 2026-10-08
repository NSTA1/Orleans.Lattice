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
    /// Answers whether inbound replication is admissible in the local region.
    /// Unlike client serving, replication may enter a region while it is
    /// <see cref="TenantRegionStatus.Backfilling"/> so the replica can converge
    /// before it becomes online.
    /// </summary>
    /// <param name="tenant">The tenant to test.</param>
    /// <param name="admissible">The answer when this returns <c>true</c>; otherwise <c>false</c>.</param>
    /// <returns><c>true</c> when the answer is authoritative; otherwise the caller must confirm.</returns>
    bool TryResolveReplicationAdmissible(TenantId tenant, out bool admissible);

    /// <summary>
    /// Answers whether a region may ship as a source for a tenant from the
    /// authoritative in-memory view. A draining region may finish shipping writes
    /// accepted while it was online.
    /// </summary>
    /// <param name="tenant">The tenant to test.</param>
    /// <param name="regionId">The direct sender's region id, or <see langword="null"/> when unavailable.</param>
    /// <param name="configured">Whether residency is configured for the tenant.</param>
    /// <param name="allowed">The answer when this returns <c>true</c>.</param>
    /// <returns><c>true</c> when the answer is authoritative; otherwise the caller must confirm.</returns>
    bool TryResolveSourceResidency(TenantId tenant, string? regionId, out bool configured, out bool allowed);

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

    /// <summary>
    /// Whether inbound replication may be applied to the tenant in the local
    /// region, by the same rule the residency snapshot applies.
    /// </summary>
    /// <param name="record">The tenant's authoritative registry record.</param>
    /// <returns><c>true</c> when unconfigured, backfilling, or online locally.</returns>
    bool IsReplicationAdmissible(TenantRecord record);

    /// <summary>
    /// Whether the given region may ship tenant writes according to the
    /// authoritative registry record. A draining region may finish shipping writes
    /// accepted while it was online.
    /// </summary>
    /// <param name="record">The tenant's authoritative registry record.</param>
    /// <param name="regionId">The direct sender's region id.</param>
    /// <returns><c>true</c> when the tenant is unconfigured or the region may ship its final writes.</returns>
    bool IsReplicationSource(TenantRecord record, string regionId);
}
