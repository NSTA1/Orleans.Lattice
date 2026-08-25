using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Accrues a tenant's observed quota overage into this cluster's grow-only meter
/// component: each call projects the current usage above the tenant's steady-state
/// caps (<see cref="TenantOverageSample.Above"/>) and, when non-empty, adds it to
/// the tenant's per-cluster overage counter through the store. This is the
/// low-frequency, cadence-driven side of the overage layer (the caller supplies the
/// cadence), so it may allocate; it is deliberately separate from the warm,
/// allocation-free admission read, which overage never sits on.
/// </summary>
/// <remarks>
/// <para>
/// The meter is a Riemann sum: each tick adds the overage observed at that tick, so
/// the running total grows with sustained overage - the correct, monotone semantics
/// for a billing meter, and exactly what a grow-only <see cref="GCounter"/> models.
/// The caller owns the cadence and its calibration (a faster cadence integrates the
/// area under the overage curve more finely); the meter itself keeps no timer and
/// no wall-clock state, so it is deterministic and race-free under test.
/// </para>
/// <para>
/// The meter advances only <em>this</em> cluster's component (keyed by
/// <see cref="ClusterOptions.ClusterId"/>); the store's CRDT merge converges it with
/// every other cluster's component, and the billing reader folds them.
/// </para>
/// </remarks>
internal sealed class TenantOverageMeter
{
    private readonly ITenantOverageStore _store;
    private readonly string _clusterId;

    /// <summary>Initializes a new <see cref="TenantOverageMeter"/>.</summary>
    /// <param name="store">The durable overage store this cluster's component is metered into.</param>
    /// <param name="cluster">The cluster options supplying this cluster's id (the counter replica key).</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantOverageMeter(ITenantOverageStore store, IOptions<ClusterOptions> cluster)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(cluster);
        _store = store;
        _clusterId = cluster.Value.ClusterId;
    }

    /// <summary>The id of the cluster this meter advances the overage component for.</summary>
    public string ClusterId => _clusterId;

    /// <summary>
    /// Projects <paramref name="usage"/> above <paramref name="quotas"/>'
    /// steady-state caps and, when the tenant is in overage on any dimension, adds
    /// that overage to this cluster's grow-only counter component. A within-quota
    /// observation is a no-op that neither writes nor creates a record.
    /// </summary>
    /// <param name="tenant">The tenant whose overage is metered. Must be an initialised tenant id.</param>
    /// <param name="usage">The tenant's current usage sample (the local or global fold, per the metering scope).</param>
    /// <param name="quotas">The tenant's declared quotas, whose base ceilings define where overage begins.</param>
    /// <param name="cancellationToken">Cancels the meter write.</param>
    /// <returns>The overage that was metered this tick; <see cref="TenantOverageSample.Empty"/> when within quota.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised 'no tenant' value.</exception>
    public async Task<TenantOverageSample> AccrueAsync(
        TenantId tenant,
        LocalUsageSample usage,
        TenantQuotas quotas,
        CancellationToken cancellationToken = default)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "The uninitialised 'no tenant' value cannot meter overage.",
                nameof(tenant));
        }

        var overage = TenantOverageSample.Above(usage, quotas);
        if (overage.IsEmpty)
        {
            return TenantOverageSample.Empty;
        }

        await _store.MeterAsync(tenant, _clusterId, overage, cancellationToken).ConfigureAwait(false);
        return overage;
    }
}
