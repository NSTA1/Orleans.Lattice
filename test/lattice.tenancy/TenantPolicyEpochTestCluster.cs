using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// An in-process stand-in for the cross-silo tenant-policy currency protocol: one
/// <see cref="TenantPolicyEpochLedger{TSubscriber}"/> playing the epoch grain and
/// any number of <see cref="CompiledTenantPolicySnapshotMaintainer"/>s playing the
/// silos, all on one <see cref="FakeTimeProvider"/>, so leases, acknowledgements,
/// lease lapses and a grain restart are driven deterministically - no sleeps, no
/// polling. A silo's push can be dropped to model an unreachable silo.
/// </summary>
internal sealed class TenantPolicyEpochTestCluster
{
    /// <summary>The lease every silo is granted.</summary>
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(10);

    private readonly HashSet<CompiledTenantPolicySnapshotMaintainer> _unreachable = [];

    /// <summary>Creates a cluster whose epoch grain is past its fresh-incarnation grace.</summary>
    public TenantPolicyEpochTestCluster()
    {
        Time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        Ledger = NewLedger();
        Time.Advance(LeaseDuration * 2);
    }

    /// <summary>The shared fake clock.</summary>
    public FakeTimeProvider Time { get; }

    /// <summary>The current incarnation of the epoch grain.</summary>
    public TenantPolicyEpochLedger<CompiledTenantPolicySnapshotMaintainer> Ledger { get; private set; }

    /// <summary>The number of advances the silos have published.</summary>
    public int Advances { get; private set; }

    /// <summary>Creates a silo over <paramref name="registry"/> that is not yet leased.</summary>
    public CompiledTenantPolicySnapshotMaintainer AddSilo(ITenantRegistry registry) =>
        new(registry, new Publisher(this), Time, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);

    /// <summary>
    /// Creates a silo, leases it, and waits for the snapshot build the lease
    /// schedules, so the silo is authoritative on return.
    /// </summary>
    public async Task<CompiledTenantPolicySnapshotMaintainer> AddLeasedSiloAsync(ITenantRegistry registry)
    {
        var silo = AddSilo(registry);
        Renew(silo);
        await silo.BackgroundRebuild;
        Assert.That(silo.IsSnapshotAuthoritative, Is.True, "precondition: a leased, built silo is authoritative");
        return silo;
    }

    /// <summary>Renews <paramref name="silo"/>'s lease against the current incarnation, as its subscription does.</summary>
    public void Renew(CompiledTenantPolicySnapshotMaintainer silo)
    {
        var requestedAt = Time.GetTimestamp();
        silo.ApplyLease(Ledger.Lease(silo), requestedAt);
    }

    /// <summary>Makes every push to <paramref name="silo"/> fail, as for a silo the grain cannot reach.</summary>
    public void MakeUnreachable(CompiledTenantPolicySnapshotMaintainer silo) => _unreachable.Add(silo);

    /// <summary>
    /// Replaces the epoch grain with a fresh activation (a new incarnation with an
    /// empty lease table) at the current time, as a grain restart would.
    /// </summary>
    public void RestartEpochGrain() => Ledger = NewLedger();

    /// <summary>The time an advance may have to wait out a lease: one lease plus the ledger's margin.</summary>
    public TimeSpan LeaseWaitOut => LeaseDuration + Ledger.Margin;

    /// <summary>
    /// A maintainer that is never leased, so it is never authoritative. For tests
    /// that exercise only the snapshot's content or the non-authoritative path.
    /// </summary>
    public static CompiledTenantPolicySnapshotMaintainer Unleased(ITenantRegistry registry) =>
        new TenantPolicyEpochTestCluster().AddSilo(registry);

    /// <summary>A single leased, built (and so authoritative) maintainer.</summary>
    public static Task<CompiledTenantPolicySnapshotMaintainer> LeasedAsync(ITenantRegistry registry) =>
        new TenantPolicyEpochTestCluster().AddLeasedSiloAsync(registry);

    private TenantPolicyEpochLedger<CompiledTenantPolicySnapshotMaintainer> NewLedger() =>
        new(LeaseDuration, Time, Guid.NewGuid());

    private Task NotifyAsync(CompiledTenantPolicySnapshotMaintainer silo, TenantPolicyEpoch epoch)
    {
        if (_unreachable.Contains(silo))
        {
            return Task.FromException(new TimeoutException("silo unreachable"));
        }

        silo.ObserveEpoch(epoch);
        return Task.CompletedTask;
    }

    private sealed class Publisher(TenantPolicyEpochTestCluster cluster) : ITenantPolicyEpochPublisher
    {
        public Task AdvanceAsync(CancellationToken cancellationToken)
        {
            cluster.Advances++;
            return cluster.Ledger.AdvanceAsync(cluster.NotifyAsync, cancellationToken);
        }
    }
}
