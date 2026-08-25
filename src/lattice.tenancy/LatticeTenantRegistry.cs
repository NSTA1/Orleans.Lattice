using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantRegistry"/>. Dogfoods the reserved
/// <c>sys-tenant-registry</c> <see cref="ILattice"/> tree, storing one
/// <see cref="TenantRecord"/> per tenant keyed by the tenant id text. Every read
/// and write runs under system-origin (both to skip the access gate and to
/// satisfy the reserved-prefix write guard); a write merges the supplied record
/// into the stored one with the record's own last-writer-wins join, so
/// concurrent updates converge and replaying an older write never regresses a
/// field. The default tenant is seeded lazily on first use by the shared
/// initializer.
/// </summary>
internal sealed class LatticeTenantRegistry(
    IGrainFactory grainFactory,
    TenantRegistryInitializer initializer,
    OrleansLatticeSerializer<TenantRecord> serializer) : ITenantRegistry
{
    /// <summary>
    /// The bounded optimistic-concurrency retry budget for a single
    /// <see cref="PutAsync"/>. A merge only ever needs to retry when a competing
    /// writer advanced the record's version between this call's read and write,
    /// so a small budget absorbs realistic contention; exhausting it signals
    /// pathological sustained contention on one tenant.
    /// </summary>
    private const int MaxPutAttempts = 8;

    private readonly ILatticeSerializer<TenantRecord> _serializer =
        serializer ?? throw new ArgumentNullException(nameof(serializer));

    private ILattice Registry => grainFactory.GetGrain<ILattice>(TenantTreeNames.RegistryTree);

    /// <inheritdoc />
    public async Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var key = RequireTenantKey(tenant);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeSystemOrigin.Enter())
        {
            return await Registry.GetAsync(key, _serializer, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
        await GetAsync(tenant, cancellationToken).ConfigureAwait(false) is not null;

    /// <inheritdoc />
    public async IAsyncEnumerable<TenantRecord> ListAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeSystemOrigin.Enter())
        {
            // Use the resilient ScanEntriesAsync wrapper (not the low-level
            // EntriesAsync): a full registry scan can outlive the remote enumerator
            // when the registry tree grain deactivates mid-stream (idle expiry, cold
            // start, failover, or concurrent scan activity under load), and the
            // wrapper transparently reopens the scan from the last yielded key with
            // no duplicates or gaps, so every consumer (the snapshot maintainers, and
            // callers listing tenants directly) sees one deterministic enumeration.
            await foreach (var entry in Registry.ScanEntriesAsync(_serializer, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (entry.Value is { } record)
                {
                    yield return record;
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        var key = RequireTenantKey(record.Id);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        using (LatticeSystemOrigin.Enter())
        {
            return await PutMergeAsync(Registry, key, record, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The optimistic-concurrency read-merge-write against a supplied
    /// <paramref name="registry"/> tree. Capture the stored record's version,
    /// fold the incoming record into it with the CRDT join, and write back
    /// conditionally on that version. A competing writer that advanced the
    /// version between the read and the write loses the conditional write, so we
    /// re-read (now seeing the other writer's committed change) and merge again,
    /// guaranteeing no field is dropped. This is what makes concurrent
    /// <see cref="PutAsync"/> converge rather than last-writer-wins overwrite.
    /// Factored out of <see cref="PutAsync"/> so the retry loop can be driven
    /// deterministically in a unit test against a substituted <see cref="ILattice"/>.
    /// </summary>
    internal async Task<TenantRecord> PutMergeAsync(
        ILattice registry,
        string key,
        TenantRecord record,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            var current = await registry
                .GetWithVersionAsync(key, _serializer, cancellationToken)
                .ConfigureAwait(false);

            // On a miss the version is HybridLogicalClock.Zero, which
            // SetIfVersionAsync treats as "create only if still absent".
            var merged = current.Value is null ? record : current.Value.MergeFrom(record);

            var applied = await registry
                .SetIfVersionAsync(key, merged, current.Version, _serializer, cancellationToken)
                .ConfigureAwait(false);
            if (applied)
            {
                return merged;
            }

            if (attempt >= MaxPutAttempts)
            {
                throw new TenantRegistryConcurrencyException(record.Id, MaxPutAttempts);
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        var key = RequireTenantKey(tenant);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeSystemOrigin.Enter())
        {
            return await Registry.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the registry key (the tenant id text) for an initialised tenant,
    /// throwing when the tenant is the uninitialised "no tenant" value.
    /// </summary>
    private static string RequireTenantKey(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "The uninitialised 'no tenant' value cannot address a registry record.",
                nameof(tenant));
        }

        return tenant.Value;
    }
}
