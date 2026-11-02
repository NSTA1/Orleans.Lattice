using Orleans.Concurrency;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The single cluster-wide <see cref="IAppRegistryEpochGrain"/> activation. A thin Orleans host over
/// <see cref="AppRegistryEpochLedger{TSubscriber}"/>, which holds the lease-and-push protocol (#5713).
/// It holds the epoch in memory only: each activation mints a fresh
/// <see cref="AppRegistryEpoch.Incarnation"/>, so a reactivation reads to every silo as a registry change
/// and no storage provider is needed.
/// </summary>
/// <remarks>
/// <para>
/// Reentrant so that a lease renewal, or a fallback read, is never queued behind an advance that is
/// waiting out an unresponsive silo's lease. Every ledger operation mutates state under its own lock in a
/// single synchronous section, and the grain's own set is touched only between awaits.
/// </para>
/// <para>
/// A fresh activation holds every advance open for one lease plus the ledger's margin, in case a previous
/// activation granted leases that are still live. It ends that grace early once every silo cluster
/// membership does not report dead has leased from this activation, as the tenant-policy epoch grain does
/// (#4030).
/// </para>
/// </remarks>
[Reentrant]
internal sealed class AppRegistryEpochGrain : Grain, IAppRegistryEpochGrain
{
    private readonly AppRegistryEpochLedger<IAppRegistryEpochObserver> _ledger;
    private readonly IClusterMembershipService? _membership;
    private readonly HashSet<SiloAddress> _leasedSilos = [];

    /// <summary>Initializes the grain.</summary>
    /// <param name="timeProvider">The clock leases and waits are measured on; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <param name="membership">Cluster membership, used to end the fresh-activation grace early; null when unavailable.</param>
    public AppRegistryEpochGrain(TimeProvider? timeProvider = null, IClusterMembershipService? membership = null)
    {
        _ledger = new(AppRegistryEpochLedger<IAppRegistryEpochObserver>.DefaultLeaseDuration, timeProvider ?? TimeProvider.System, Guid.NewGuid());
        _membership = membership;
    }

    /// <inheritdoc />
    public Task<AppRegistryEpoch> GetAsync() => Task.FromResult(_ledger.Current);

    /// <inheritdoc />
    public Task<AppRegistryEpochLease> LeaseAsync(IAppRegistryEpochObserver observer, SiloAddress silo)
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
    public Task<AppRegistryEpoch> AdvanceAsync()
    {
        TryReleaseGrace();
        return _ledger.AdvanceAsync(static (observer, epoch) => observer.OnEpochAdvancedAsync(epoch));
    }

    /// <summary>
    /// <c>true</c> when every silo <paramref name="snapshot"/> does not report dead appears in
    /// <paramref name="leased"/>. An empty snapshot proves nothing and answers <c>false</c>.
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
        if (_ledger.IsGraceReleased
            || _membership is null
            || !EveryLiveSiloHasLeased(_membership.CurrentSnapshot, _leasedSilos))
        {
            return;
        }

        _ledger.ReleaseGrace();
        _leasedSilos.Clear();
    }
}
