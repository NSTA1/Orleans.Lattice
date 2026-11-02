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
/// single synchronous section, and the grain's own collections are touched only between awaits.
/// </para>
/// <para>
/// A fresh activation holds every advance open for one lease plus the ledger's margin, in case a previous
/// activation granted leases that are still live. It ends that grace early only once every silo cluster
/// membership lists has either leased from this activation - it has then observed the new incarnation and
/// is in the table every advance pushes to - or has been seen <see cref="SiloStatus.Dead"/> by this
/// activation for at least one lease plus the margin. A silo declared dead is not assumed to have
/// stopped: one falsely declared dead can keep evaluating on a lease from the previous activation until
/// that lease runs out, so its death releases nothing until the lease it could hold has expired.
/// </para>
/// </remarks>
[Reentrant]
internal sealed class AppRegistryEpochGrain : Grain, IAppRegistryEpochGrain
{
    private readonly AppRegistryEpochLedger<IAppRegistryEpochObserver> _ledger;
    private readonly TimeProvider _time;
    private readonly IClusterMembershipService? _membership;
    private readonly HashSet<SiloAddress> _leasedSilos = [];
    private readonly Dictionary<SiloAddress, long> _deadSince = [];

    /// <summary>Initializes the grain.</summary>
    /// <param name="timeProvider">The clock leases and waits are measured on; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <param name="membership">Cluster membership, used to end the fresh-activation grace early; null when unavailable.</param>
    public AppRegistryEpochGrain(TimeProvider? timeProvider = null, IClusterMembershipService? membership = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _ledger = new(AppRegistryEpochLedger<IAppRegistryEpochObserver>.DefaultLeaseDuration, _time, Guid.NewGuid());
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
    /// <c>true</c> when the fresh-activation grace may end early: every silo <paramref name="snapshot"/>
    /// lists has leased from this activation, or has been seen dead for at least <paramref name="window"/>
    /// (one lease plus the margin). Records in <paramref name="deadSince"/> when each dead silo was first
    /// seen dead, so the window runs from this activation's first sight of the death, never from an earlier
    /// moment it cannot vouch for. An empty snapshot proves nothing and answers <c>false</c>.
    /// </summary>
    /// <param name="snapshot">The cluster membership view.</param>
    /// <param name="leased">The silos that have leased from this activation.</param>
    /// <param name="deadSince">When this activation first saw each silo dead, on <paramref name="time"/>'s timestamp scale.</param>
    /// <param name="now">The current timestamp on <paramref name="time"/>.</param>
    /// <param name="time">The clock the timestamps are measured on.</param>
    /// <param name="window">How long a silo must have been seen dead before its death releases the grace.</param>
    /// <returns><c>true</c> when the fresh-incarnation grace may end early.</returns>
    internal static bool CanReleaseGrace(
        ClusterMembershipSnapshot snapshot,
        IReadOnlySet<SiloAddress> leased,
        Dictionary<SiloAddress, long> deadSince,
        long now,
        TimeProvider time,
        TimeSpan window)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(leased);
        ArgumentNullException.ThrowIfNull(deadSince);
        ArgumentNullException.ThrowIfNull(time);

        if (snapshot.Members.Count == 0)
        {
            return false;
        }

        // Visit every member, so each death is recorded the first time it is seen.
        var release = true;
        foreach (var member in snapshot.Members.Values)
        {
            if (leased.Contains(member.SiloAddress))
            {
                continue;
            }

            if (member.Status != SiloStatus.Dead)
            {
                release = false;
                continue;
            }

            if (!deadSince.TryGetValue(member.SiloAddress, out var since))
            {
                deadSince[member.SiloAddress] = since = now;
            }

            if (time.GetElapsedTime(since, now) < window)
            {
                release = false;
            }
        }

        return release;
    }

    private void TryReleaseGrace()
    {
        if (_ledger.IsGraceReleased
            || _membership is null
            || !CanReleaseGrace(
                _membership.CurrentSnapshot,
                _leasedSilos,
                _deadSince,
                _time.GetTimestamp(),
                _time,
                _ledger.LeaseDuration + _ledger.Margin))
        {
            return;
        }

        _ledger.ReleaseGrace();
        _leasedSilos.Clear();
        _deadSince.Clear();
    }
}
