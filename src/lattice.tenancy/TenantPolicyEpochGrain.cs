using Microsoft.Extensions.Options;
using Orleans.Concurrency;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The single cluster-wide <see cref="ITenantPolicyEpochGrain"/> activation. A thin
/// Orleans host over <see cref="TenantPolicyEpochLedger{TSubscriber}"/>, which
/// holds the whole protocol; each activation mints a fresh
/// <see cref="TenantPolicyEpoch.Incarnation"/>, so no storage provider is needed.
/// </summary>
/// <remarks>
/// Reentrant so that a lease renewal is never queued behind an advance that is
/// waiting out an unresponsive silo's lease: a silo that could not renew would
/// lose its authority and fall back to the registry for no reason. Every ledger
/// operation mutates state under its own lock in a single synchronous section, so
/// interleaving is safe.
/// </remarks>
[Reentrant]
internal sealed class TenantPolicyEpochGrain(
    IOptions<LatticeTenancyOptions> options,
    TimeProvider timeProvider) : Grain, ITenantPolicyEpochGrain
{
    private readonly TenantPolicyEpochLedger<ITenantPolicyEpochObserver> _ledger = new(
        options.Value.PolicySnapshotLeaseDuration,
        timeProvider,
        Guid.NewGuid());

    /// <inheritdoc />
    public Task<TenantPolicyEpochLease> LeaseAsync(ITenantPolicyEpochObserver observer)
    {
        ArgumentNullException.ThrowIfNull(observer);
        return Task.FromResult(_ledger.Lease(observer));
    }

    /// <inheritdoc />
    public Task<TenantPolicyEpoch> AdvanceAsync() =>
        _ledger.AdvanceAsync(static (observer, epoch) => observer.OnEpochAdvancedAsync(epoch));
}
