using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Commits a tenant record that removes an admin-set entry without ever committing
/// a record whose admin set is empty (D6: a tenant may never be left without an
/// admin).
/// </summary>
/// <remarks>
/// <para>
/// The facades check the last-admin guard against the record they read, but that
/// read-check-write alone cannot stop two concurrent removals of <i>different</i>
/// entries: each sees two live entries, each passes, and their tombstones land on
/// disjoint keys that both survive the per-entry merge. The guard is therefore
/// re-applied to the merged record inside the built-in registry's optimistic
/// compare-and-set loop (<see cref="IGuardedTenantRegistry"/>), before the
/// conditional write: the second racer to commit re-reads the first's committed
/// removal, re-merges, and is refused with nothing written. There is no
/// remove-then-re-grant step, so no second write can fail and strand the tenant.
/// </para>
/// <para>
/// A host-supplied <see cref="ITenantRegistry"/> without the guarded commit gets a
/// best-effort fallback: the guard is checked against a fresh read merged locally
/// before the write, and, should a racer still empty the set between that read and
/// the write, this call re-grants its own entry at a later stamp and is refused (the
/// pre-existing self-heal). Only the built-in registry gives the atomic guarantee.
/// </para>
/// </remarks>
internal static class TenantAdminSetCommit
{
    /// <summary>
    /// Commits <paramref name="record"/>, which carries this call's removal of the
    /// admin entry <paramref name="removedAdminEntry"/>, refusing with
    /// <see cref="TenantLastAdminSubjectException"/> when the committed admin set
    /// would be empty.
    /// </summary>
    /// <param name="registry">The tenant registry.</param>
    /// <param name="record">The record carrying the removal.</param>
    /// <param name="removedAdminEntry">The stored admin entry this call removes.</param>
    /// <param name="clock">The facade's clock, used only by the fallback's self-heal.</param>
    /// <param name="writerId">The writer id, used only by the fallback's self-heal.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The committed record.</returns>
    /// <exception cref="TenantLastAdminSubjectException">The committed admin set would have been empty; nothing was removed.</exception>
    internal static async Task<TenantRecord> CommitAsync(
        ITenantRegistry registry,
        TenantRecord record,
        string removedAdminEntry,
        ITenantAdminClock clock,
        string? writerId,
        CancellationToken cancellationToken)
    {
        var tenantId = record.Id.Value;
        void Guard(TenantRecord merged)
        {
            if (merged.AdminSubjectCount == 0)
            {
                throw new TenantLastAdminSubjectException(tenantId, removedAdminEntry);
            }
        }

        if (registry is IGuardedTenantRegistry guarded)
        {
            return await guarded.PutGuardedAsync(record, Guard, cancellationToken).ConfigureAwait(false);
        }

        if (await registry.GetAsync(record.Id, cancellationToken).ConfigureAwait(false) is { } current)
        {
            Guard(current.Clone().MergeFrom(record));
        }

        var committed = await registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
        if (committed.AdminSubjectCount == 0)
        {
            committed.AddAdminSubject(
                removedAdminEntry,
                HybridLogicalClock.Tick(Later(clock.Next(), record.Subjects[removedAdminEntry].Clock)),
                writerId);
            await registry.PutAsync(committed, cancellationToken).ConfigureAwait(false);
            throw new TenantLastAdminSubjectException(tenantId, removedAdminEntry);
        }

        return committed;
    }

    private static HybridLogicalClock Later(HybridLogicalClock left, HybridLogicalClock right) =>
        left > right ? left : right;
}
