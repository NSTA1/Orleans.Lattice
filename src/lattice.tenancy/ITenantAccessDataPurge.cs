namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The access-data purge step of the tenant deletion pipeline (epic #4154, D12):
/// removes a deleted tenant's tenant-tier rules, tenant groups, and every
/// membership edge into or out of those groups. The tenant's member set lives on
/// its <see cref="TenantRecord"/> and goes with the record.
/// <see cref="LatticeTenantRegistry.DeleteAsync"/> runs this step before it
/// removes the record, so a failure leaves the tenant visible (suspended, mid
/// delete) rather than orphaning its access data, and a re-run finishes the purge.
/// </summary>
internal interface ITenantAccessDataPurge
{
    /// <summary>
    /// Purges the tenant's access data. Runs regardless of the delegated tenant
    /// access flag, under system origin, and performs no authorization of its own:
    /// the deletion pipeline has already authorized the delete. Idempotent and
    /// resumable: a call after an interrupted call completes the purge, and a call
    /// after a completed purge removes nothing.
    /// </summary>
    /// <param name="tenant">The tenant being deleted. Must be initialised. The reserved <see cref="TenantId.Default"/> tenant owns no tenant-tier access data, so the call is a no-op for it.</param>
    /// <param name="cancellationToken">Cancels the purge.</param>
    /// <returns>What this call removed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised "no tenant" value.</exception>
    Task<TenantAccessPurgeResult> PurgeAsync(TenantId tenant, CancellationToken cancellationToken = default);
}
