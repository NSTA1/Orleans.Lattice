using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Commits a tenant record and reports whether a predicate held for the stored
/// record the commit merged into, so a facade reports its own change from the
/// merge rather than from its earlier read (#3638). Two concurrent removals of
/// one entry both pass a check against their own reads, and the CRDT merge
/// accepts both; only the stored record each merged into tells which one removed
/// the entry.
/// </summary>
internal static class TenantRecordProbedCommit
{
    /// <summary>
    /// Merges <paramref name="record"/> into the registry and evaluates
    /// <paramref name="storedMatches"/> against the stored record it merged into.
    /// A host-supplied registry without <see cref="IGuardedTenantRegistry"/> cannot
    /// report the record it merged into, so the predicate is assumed to hold, as the
    /// caller's own read showed; only the built-in registry reports from the merge.
    /// </summary>
    /// <param name="registry">The tenant registry.</param>
    /// <param name="record">The record carrying this call's change.</param>
    /// <param name="storedMatches">The predicate over the stored record; it must not mutate it.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The committed record, and whether the predicate held for the stored record.</returns>
    internal static async Task<(TenantRecord Merged, bool Matched)> PutAsync(
        ITenantRegistry registry,
        TenantRecord record,
        Func<TenantRecord, bool> storedMatches,
        CancellationToken cancellationToken)
    {
        if (registry is IGuardedTenantRegistry guarded)
        {
            var probe = new TenantStoredRecordProbe(storedMatches);
            var merged = await guarded.PutProbedAsync(record, probe, validateMerged: null, cancellationToken).ConfigureAwait(false);
            return (merged, probe.Matched);
        }

        var committed = await registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
        return (committed, true);
    }
}
