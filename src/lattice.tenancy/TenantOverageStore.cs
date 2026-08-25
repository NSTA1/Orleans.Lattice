using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable store for per-tenant overage meters. Dogfoods the reserved
/// <c>sys-tenant-overage</c> <see cref="ILattice"/> tree, storing one
/// <see cref="TenantOverageRecord"/> per tenant keyed by the tenant id text. Every
/// read and write runs under system-origin (both to skip the access gate and to
/// satisfy the reserved-prefix write guard); a meter merges this cluster's grow-only
/// counter increment into the stored record with the record's own CRDT join, so
/// concurrent metering from every cluster converges and replaying a failed write
/// never double-counts.
/// </summary>
/// <remarks>
/// This is the overage-side twin of <see cref="TenantUsageStore"/>. It reuses the
/// exact <see cref="OrleansLatticeSerializer{T}"/> binary-serializer and bounded
/// optimistic-concurrency merge pattern the usage store established, rather than the
/// lossy default JSON typed overloads that would silently hollow the
/// non-round-trippable grow-only counter state.
/// </remarks>
internal sealed class TenantOverageStore(
    IGrainFactory grainFactory,
    OrleansLatticeSerializer<TenantOverageRecord> serializer) : ITenantOverageStore
{
    /// <summary>
    /// The bounded optimistic-concurrency retry budget for a single meter write,
    /// matching <see cref="TenantUsageStore"/>. A merge only retries when a competing
    /// writer advanced the record's version between this call's read and write;
    /// exhausting the budget signals pathological sustained contention.
    /// </summary>
    private const int MaxMeterAttempts = 8;

    private readonly ILatticeSerializer<TenantOverageRecord> _serializer =
        serializer ?? throw new ArgumentNullException(nameof(serializer));

    private ILattice Overage => grainFactory.GetGrain<ILattice>(TenantTreeNames.OverageTree);

    /// <summary>
    /// Reads the overage meter for a tenant, or <c>null</c> when no cluster has yet
    /// metered any overage for it.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's overage meter, or <c>null</c> when absent.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c>.</exception>
    public async Task<TenantOverageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var key = RequireTenantKey(tenant);
        using (LatticeSystemOrigin.Enter())
        {
            return await Overage.GetAsync(key, _serializer, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Enumerates every tenant's overage meter.</summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of overage meters.</returns>
    public async IAsyncEnumerable<TenantOverageRecord> ListAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await foreach (var entry in Overage.ScanEntriesAsync(_serializer, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (entry.Value is { } record)
                {
                    yield return record;
                }
            }
        }
    }

    /// <summary>
    /// Merges <paramref name="cluster"/>'s grow-only overage increment for
    /// <paramref name="tenant"/> into the stored meter and persists the converged
    /// result. An empty increment is a no-op: it neither writes nor creates a record,
    /// returning the stored meter (or a transient empty meter when absent), so a
    /// within-quota tenant never churns the tree.
    /// </summary>
    /// <param name="tenant">The tenant whose overage is metered. Must be an initialised tenant id.</param>
    /// <param name="cluster">The metering cluster's id (the replica key). Must not be <c>null</c> or empty.</param>
    /// <param name="increment">The overage observed this tick to add to the meter.</param>
    /// <param name="cancellationToken">Cancels the meter write.</param>
    /// <returns>The stored meter after the merge.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is uninitialised, or <paramref name="cluster"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantOverageConcurrencyException">The retry budget was exhausted under sustained contention.</exception>
    public async Task<TenantOverageRecord> MeterAsync(
        TenantId tenant,
        string cluster,
        TenantOverageSample increment,
        CancellationToken cancellationToken = default)
    {
        var key = RequireTenantKey(tenant);
        ArgumentException.ThrowIfNullOrEmpty(cluster);

        using (LatticeSystemOrigin.Enter())
        {
            if (increment.IsEmpty)
            {
                return await Overage.GetAsync(key, _serializer, cancellationToken).ConfigureAwait(false)
                       ?? TenantOverageRecord.Create(tenant);
            }

            return await MeterMergeAsync(Overage, tenant, key, cluster, increment, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The optimistic-concurrency read-merge-write against a supplied
    /// <paramref name="overage"/> tree, mirroring
    /// <see cref="TenantUsageStore.PutMergeAsync"/>. Factored out of
    /// <see cref="MeterAsync"/> so the retry loop can be driven deterministically in
    /// a unit test against a substituted <see cref="ILattice"/>. Each attempt applies
    /// the increment to the <em>freshly read</em> record, so a lost version race
    /// re-applies exactly one increment over the then-current stored value.
    /// </summary>
    internal async Task<TenantOverageRecord> MeterMergeAsync(
        ILattice overage,
        TenantId tenant,
        string key,
        string cluster,
        TenantOverageSample increment,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            var current = await overage
                .GetWithVersionAsync(key, _serializer, cancellationToken)
                .ConfigureAwait(false);

            // On a miss the version is HybridLogicalClock.Zero, which
            // SetIfVersionAsync treats as "create only if still absent". The
            // increment is applied to the freshly read (or freshly created) record,
            // never to a pre-built one, so a retry after a lost race adds exactly one
            // increment over the newest stored value - grow-only and never doubled.
            var merged = current.Value ?? TenantOverageRecord.Create(tenant);
            merged.MeterLocal(cluster, increment);

            var applied = await overage
                .SetIfVersionAsync(key, merged, current.Version, _serializer, cancellationToken)
                .ConfigureAwait(false);
            if (applied)
            {
                return merged;
            }

            if (attempt >= MaxMeterAttempts)
            {
                throw new TenantOverageConcurrencyException(tenant, MaxMeterAttempts);
            }
        }
    }

    private static string RequireTenantKey(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "The uninitialised 'no tenant' value cannot address an overage record.",
                nameof(tenant));
        }

        return tenant.Value;
    }
}
