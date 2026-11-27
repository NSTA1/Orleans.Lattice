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
    /// Merges <paramref name="record"/> into the stored record for its tenant and
    /// persists the merged result. The record must already be stored: an update
    /// never creates one, so a caller that read a record before its tenant was
    /// deleted cannot re-create the tenant from that stale copy. Use
    /// <see cref="CreateAsync"/> to register a tenant. Because the
    /// write is a last-writer-wins join, replaying an older write never regresses
    /// a field and re-applying the same write is idempotent. The merge is written
    /// back conditionally on the version it was read at and, if a competing writer
    /// advanced the record in between, re-read and re-merged; that is retried
    /// immediately, without backoff, up to a bounded number of attempts.
    /// </summary>
    /// <param name="record">The record to merge in. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The stored record after the merge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="record"/> carries the uninitialised <c>default(TenantId)</c>.</exception>
    /// <exception cref="TenantRecordNotFoundException">No record is stored for the tenant, for example because it was deleted after <paramref name="record"/> was read.</exception>
    /// <exception cref="TenantRegistryConcurrencyException">Competing writers to the same tenant kept advancing its record, so every attempt lost the conditional write and the bounded attempt budget was exhausted without the merge landing.</exception>
    Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers <paramref name="record"/>: creates the stored record for its tenant
    /// when none exists, and otherwise merges into it exactly as
    /// <see cref="PutAsync"/> does. The one write that may create a record; tenant
    /// creation and seeding use it, and every update of an existing tenant uses
    /// <see cref="PutAsync"/>.
    /// </summary>
    /// <remarks>
    /// The default implementation calls <see cref="PutAsync"/>, which is correct for
    /// an implementation whose <see cref="PutAsync"/> still creates an absent record.
    /// </remarks>
    /// <param name="record">The record to register. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The stored record after the write.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="record"/> carries the uninitialised <c>default(TenantId)</c>.</exception>
    /// <exception cref="TenantRegistryConcurrencyException">Competing writers kept winning the conditional write until the bounded attempt budget was exhausted.</exception>
    Task<TenantRecord> CreateAsync(TenantRecord record, CancellationToken cancellationToken = default) =>
        PutAsync(record, cancellationToken);

    /// <summary>
    /// Removes the record for a tenant from the registry. Removing an unknown
    /// tenant is a no-op. Every cross-tenant grant another tenant issued to the
    /// removed tenant is removed with it, so a tenant later created under the same
    /// id inherits none of them.
    /// </summary>
    /// <param name="tenant">The tenant to remove. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns><c>true</c> when a record was removed; <c>false</c> when none existed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    /// <exception cref="TenantRegistryConcurrencyException">Competing writers kept advancing the record of a tenant that issued a grant to <paramref name="tenant"/>, so that grant could not be removed; the record of <paramref name="tenant"/> is left in place and a retry completes the delete.</exception>
    Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default);
}
