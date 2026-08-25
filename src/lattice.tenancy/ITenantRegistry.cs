namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable, CRDT-backed tenant registry: the introspectable store of every
/// tenant definition in the cluster, persisted in the reserved
/// <c>sys-tenant-*</c> Lattice trees. Reads and writes address the backing trees
/// under system-origin; a write merges the supplied record into any stored one
/// with the record's own last-writer-wins join, so concurrent updates converge.
/// </summary>
/// <remarks>
/// The registry is the definition store only - the seat of tenant status,
/// quotas, placement, admin subjects, and cross-tenant grants. Lifecycle policy
/// (which transitions are legal, and the rule that the reserved default tenant
/// can never be suspended or deleted) and enforcement of the stored quotas and
/// grants are layered on top of it.
/// </remarks>
public interface ITenantRegistry
{
    /// <summary>
    /// Reads the record for a tenant, or <c>null</c> when no such tenant is
    /// registered.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's record, or <c>null</c> when absent.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>Returns <c>true</c> when a record for <paramref name="tenant"/> exists.</summary>
    /// <param name="tenant">The tenant to test. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns><c>true</c> when the tenant is registered.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every registered tenant's record.</summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of tenant records.</returns>
    IAsyncEnumerable<TenantRecord> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges <paramref name="record"/> into the stored record for its tenant
    /// (creating it when absent) and persists the merged result. Because the
    /// write is a last-writer-wins join, replaying an older write never regresses
    /// a field and re-applying the same write is idempotent.
    /// </summary>
    /// <param name="record">The record to merge in. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The stored record after the merge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is <c>null</c>.</exception>
    Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the record for a tenant from the registry. Removing an unknown
    /// tenant is a no-op.
    /// </summary>
    /// <param name="tenant">The tenant to remove. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns><c>true</c> when a record was removed; <c>false</c> when none existed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default);
}
