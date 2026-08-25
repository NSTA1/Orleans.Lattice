using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantOverageBilling"/>. Reads the durable per-tenant
/// overage meters from the <see cref="ITenantOverageStore"/> (backed by the reserved
/// <c>sys-tenant-overage</c> tree) and folds each tenant's grow-only counters into a
/// converged cross-cluster overage a billing consumer can poll. It is a thin,
/// stateless read facade: the store owns the durable state and the CRDT join, and
/// this type only projects the fold.
/// </summary>
internal sealed class LatticeTenantOverageBilling(ITenantOverageStore store) : ITenantOverageBilling
{
    private readonly ITenantOverageStore _store = store ?? throw new ArgumentNullException(nameof(store));

    /// <inheritdoc />
    public async Task<TenantOverageSample> GetMeteredOverageAsync(
        TenantId tenant,
        CancellationToken cancellationToken = default)
    {
        var record = await _store.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        return record is null ? TenantOverageSample.Empty : record.Fold();
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TenantMeteredOverage> ListMeteredOverageAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var record in _store.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return new TenantMeteredOverage(record.Id, record.Fold());
        }
    }
}
