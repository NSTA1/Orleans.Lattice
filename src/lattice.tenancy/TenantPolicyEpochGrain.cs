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
/// <para>
/// Reentrant so that a lease renewal is never queued behind an advance that is
/// waiting out an unresponsive silo's lease: a silo that could not renew would
/// lose its authority and fall back to the registry for no reason. Every ledger
/// operation mutates state under its own lock in a single synchronous section, and
/// the grain's own set is touched only between awaits, so interleaving is safe.
/// </para>
/// <para>
/// A fresh activation holds every advance open for one lease plus the ledger's
/// clock-rate margin (one tenth of the lease) in case a previous activation
/// granted leases that are still live. It ends that
/// grace early once every silo cluster membership does not report dead has leased
/// from this activation: each such silo has then observed the new incarnation (so
/// treats its snapshot as out of date) and is in the lease table every advance
/// pushes to, so no lease from a previous activation can still make a silo
/// authoritative. That keeps the first registry write after an activation - the
/// lazily seeded default tenant, at cluster start - from waiting a whole lease.
/// The membership view is this silo's; a silo that joined, leased from a previous
/// activation, and is not yet in that view is the one case the early release does
/// not see, which needs membership to lag a join by longer than it takes the
/// previous activation's silo to be declared dead and this one to activate.
/// </para>
/// </remarks>
[Reentrant]
internal sealed class TenantPolicyEpochGrain(
    IOptions<LatticeTenancyOptions> options,
    TimeProvider timeProvider,
    IClusterMembershipService membership) : Grain, ITenantPolicyEpochGrain
{
    private readonly TenantPolicyEpochLedger<ITenantPolicyEpochObserver> _ledger = new(
        options.Value.PolicySnapshotLeaseDuration,
        timeProvider,
        Guid.NewGuid());

    private readonly HashSet<SiloAddress> _leasedSilos = [];

    /// <inheritdoc />
    public Task<TenantPolicyEpochLease> LeaseAsync(ITenantPolicyEpochObserver observer, SiloAddress silo)
    {
        ArgumentNullException.ThrowIfNull(observer);
        ArgumentNullException.ThrowIfNull(silo);

        var lease = _ledger.Lease(observer);
        if (!_ledger.IsGraceReleased)
        {
            _leasedSilos.Add(silo);
            TryReleaseGrace();
        }

        return Task.FromResult(lease);
    }

    /// <inheritdoc />
    public Task<TenantPolicyEpoch> AdvanceAsync()
    {
        TryReleaseGrace();
        return _ledger.AdvanceAsync(static (observer, epoch) => observer.OnEpochAdvancedAsync(epoch));
    }

    /// <summary>
    /// <c>true</c> when every silo <paramref name="snapshot"/> does not report dead
    /// appears in <paramref name="leased"/>. An empty snapshot proves nothing and
    /// answers <c>false</c>.
    /// </summary>
    /// <param name="snapshot">The cluster membership view.</param>
    /// <param name="leased">The silos that have leased from this activation.</param>
    /// <returns><c>true</c> when the fresh-incarnation grace may end early.</returns>
    internal static bool EveryLiveSiloHasLeased(ClusterMembershipSnapshot snapshot, IReadOnlySet<SiloAddress> leased)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(leased);

        if (snapshot.Members.Count == 0)
        {
            return false;
        }

        foreach (var member in snapshot.Members.Values)
        {
            if (member.Status != SiloStatus.Dead && !leased.Contains(member.SiloAddress))
            {
                return false;
            }
        }

        return true;
    }

    private void TryReleaseGrace()
    {
        if (_ledger.IsGraceReleased || !EveryLiveSiloHasLeased(membership.CurrentSnapshot, _leasedSilos))
        {
            return;
        }

        _ledger.ReleaseGrace();
        _leasedSilos.Clear();
    }
}
