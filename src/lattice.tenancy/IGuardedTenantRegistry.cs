namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A tenant registry whose merge-and-commit can be guarded: the caller supplies a
/// validation that runs on the merged record <b>inside</b> the registry's
/// optimistic compare-and-set loop, after the incoming record is folded into the
/// freshly read stored one and before the conditional write. A validation that
/// throws aborts the attempt with nothing written, so an invariant over the
/// committed record (for example, that a tenant always keeps at least one admin
/// entry) holds for every commit rather than being repaired afterwards.
/// </summary>
/// <remarks>
/// Implemented by the built-in <see cref="LatticeTenantRegistry"/>. Because the
/// validation sees the record exactly as it would be committed at the version it
/// was read at, two concurrent writers whose changes each pass on their own but
/// together break the invariant cannot both commit: the second to commit re-reads
/// the first's committed change, re-merges, and is refused by the validation.
/// </remarks>
internal interface IGuardedTenantRegistry
{
    /// <summary>
    /// Merges <paramref name="record"/> into the stored record exactly as
    /// <see cref="ITenantRegistry.PutAsync"/> does, running
    /// <paramref name="validateMerged"/> on each attempt's merged record before the
    /// conditional write.
    /// </summary>
    /// <param name="record">The record to merge in. Must not be <c>null</c>.</param>
    /// <param name="validateMerged">
    /// Validates the merged record; throws to refuse the commit. Must not be <c>null</c>.
    /// It may run more than once (once per attempt) and must not mutate the record.
    /// </param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The stored record after the merge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="validateMerged"/> is <c>null</c>.</exception>
    /// <exception cref="TenantRegistryConcurrencyException">The bounded attempt budget was exhausted; nothing was written.</exception>
    Task<TenantRecord> PutGuardedAsync(
        TenantRecord record, Action<TenantRecord> validateMerged, CancellationToken cancellationToken = default);
}
