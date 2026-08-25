using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable store for per-tenant usage records. Dogfoods the reserved
/// <c>sys-tenant-usage</c> <see cref="ILattice"/> tree, storing one
/// <see cref="TenantUsageRecord"/> per tenant keyed by the tenant id text. Every
/// read and write runs under system-origin (both to skip the access gate and to
/// satisfy the reserved-prefix write guard); a publish merges this cluster's usage
/// slot into the stored record with the record's own last-writer-wins join, so
/// concurrent publishes from every cluster converge and a slower cluster's stale
/// slot never regresses a fresher one.
/// </summary>
/// <remarks>
/// This is the usage-side twin of <see cref="LatticeTenantRegistry"/>. It reuses
/// the exact <see cref="OrleansLatticeSerializer{T}"/> binary-serializer and
/// bounded optimistic-concurrency merge pattern the registry established, rather
/// than the lossy default JSON typed overloads that would silently hollow the
/// non-round-trippable CRDT slot map.
/// </remarks>
internal sealed class TenantUsageStore(
    IGrainFactory grainFactory,
    OrleansLatticeSerializer<TenantUsageRecord> serializer) : ITenantUsageStore
{
    /// <summary>
    /// The bounded optimistic-concurrency retry budget for a single publish,
    /// matching <see cref="LatticeTenantRegistry"/>. A merge only retries when a
    /// competing writer advanced the record's version between this call's read and
    /// write; exhausting the budget signals pathological sustained contention.
    /// </summary>
    private const int MaxPublishAttempts = 8;

    private readonly ILatticeSerializer<TenantUsageRecord> _serializer =
        serializer ?? throw new ArgumentNullException(nameof(serializer));

    private ILattice Usage => grainFactory.GetGrain<ILattice>(TenantTreeNames.UsageTree);

    /// <summary>
    /// Reads the usage record for a tenant, or <c>null</c> when no cluster has yet
    /// published a slot for it.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's usage record, or <c>null</c> when absent.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    public async Task<TenantUsageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var key = RequireTenantKey(tenant);
        using (LatticeSystemOrigin.Enter())
        {
            return await Usage.GetAsync(key, _serializer, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Enumerates every tenant's usage record.</summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of usage records.</returns>
    public async IAsyncEnumerable<TenantUsageRecord> ListAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await foreach (var entry in Usage.ScanEntriesAsync(_serializer, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (entry.Value is { } record)
                {
                    yield return record;
                }
            }
        }
    }

    /// <summary>
    /// Merges <paramref name="record"/> (this cluster's usage slot for a tenant)
    /// into the stored record and persists the converged result. Because the write
    /// is a last-writer-wins join over per-cluster slots, replaying an older
    /// publish never regresses a fresher slot and re-publishing the same slot is
    /// idempotent.
    /// </summary>
    /// <param name="record">The record carrying this cluster's slot. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    /// <returns>The stored record after the merge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="record"/> is <c>null</c>.</exception>
    /// <exception cref="TenantUsageConcurrencyException">The retry budget was exhausted under sustained contention.</exception>
    public async Task<TenantUsageRecord> PublishAsync(TenantUsageRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        var key = RequireTenantKey(record.Id);
        using (LatticeSystemOrigin.Enter())
        {
            return await PutMergeAsync(Usage, key, record, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The optimistic-concurrency read-merge-write against a supplied
    /// <paramref name="usage"/> tree, mirroring
    /// <see cref="LatticeTenantRegistry.PutMergeAsync"/>. Factored out of
    /// <see cref="PublishAsync"/> so the retry loop can be driven deterministically
    /// in a unit test against a substituted <see cref="ILattice"/>.
    /// </summary>
    internal async Task<TenantUsageRecord> PutMergeAsync(
        ILattice usage,
        string key,
        TenantUsageRecord record,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            var current = await usage
                .GetWithVersionAsync(key, _serializer, cancellationToken)
                .ConfigureAwait(false);

            // On a miss the version is HybridLogicalClock.Zero, which
            // SetIfVersionAsync treats as "create only if still absent".
            var merged = current.Value is null ? record : current.Value.MergeFrom(record);

            var applied = await usage
                .SetIfVersionAsync(key, merged, current.Version, _serializer, cancellationToken)
                .ConfigureAwait(false);
            if (applied)
            {
                return merged;
            }

            if (attempt >= MaxPublishAttempts)
            {
                throw new TenantUsageConcurrencyException(record.Id, MaxPublishAttempts);
            }
        }
    }

    private static string RequireTenantKey(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "The uninitialised 'no tenant' value cannot address a usage record.",
                nameof(tenant));
        }

        return tenant.Value;
    }
}
