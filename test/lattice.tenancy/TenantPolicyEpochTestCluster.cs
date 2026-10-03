using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// An in-process stand-in for the cross-silo tenant-policy currency protocol: one
/// <see cref="TenantPolicyEpochLedger{TSubscriber}"/> playing the epoch grain and
/// any number of per-silo snapshots (<see cref="ITenantEpochSubscriber"/>s: the
/// compiled tenant-policy, residency and placement maintainers) playing the silos,
/// all on one <see cref="FakeTimeProvider"/>, so leases, acknowledgements,
/// lease lapses and a grain restart are driven deterministically - no sleeps, no
/// polling. A silo's push can be dropped to model an unreachable silo.
/// </summary>
internal sealed class TenantPolicyEpochTestCluster
{
    /// <summary>The lease every silo is granted.</summary>
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(10);

    private readonly HashSet<ITenantEpochSubscriber> _unreachable = [];

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
    public TenantPolicyEpochLedger<ITenantEpochSubscriber> Ledger { get; private set; }

    /// <summary>The number of advances the silos have published.</summary>
    public int Advances { get; private set; }

    /// <summary>Creates a silo over <paramref name="registry"/> that is not yet leased.</summary>
    public CompiledTenantPolicySnapshotMaintainer AddSilo(ITenantRegistry registry, DelegatedTenantAccessFlag? delegatedAccess = null) =>
        new(registry, new Publisher(this), Time, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance, delegatedAccess);

    /// <summary>
    /// Creates a silo, leases it, and waits for the snapshot build the lease
    /// schedules, so the silo is authoritative on return.
    /// </summary>
    public async Task<CompiledTenantPolicySnapshotMaintainer> AddLeasedSiloAsync(
        ITenantRegistry registry,
        DelegatedTenantAccessFlag? delegatedAccess = null)
    {
        var silo = AddSilo(registry, delegatedAccess);
        Renew(silo);
        await silo.BackgroundRebuild;
        Assert.That(silo.IsSnapshotAuthoritative, Is.True, "precondition: a leased, built silo is authoritative");
        return silo;
    }

    /// <summary>Creates a residency snapshot for region <paramref name="regionId"/> that is not yet leased.</summary>
    public TenantResidencySnapshotMaintainer AddResidencySilo(ITenantRegistry registry, string regionId = "eu") =>
        new(registry,
            Options.Create(new ClusterOptions { ClusterId = regionId }),
            [],
            Time,
            NullLogger<TenantResidencySnapshotMaintainer>.Instance);

    /// <summary>Creates a residency snapshot, leases it, and waits for the build, so it is authoritative on return.</summary>
    public async Task<TenantResidencySnapshotMaintainer> AddLeasedResidencySiloAsync(ITenantRegistry registry, string regionId = "eu")
    {
        var silo = AddResidencySilo(registry, regionId);
        Renew(silo);
        await silo.BackgroundRebuild;
        Assert.That(silo.IsSnapshotAuthoritative, Is.True, "precondition: a leased, built residency snapshot is authoritative");
        return silo;
    }

    /// <summary>Creates a placement snapshot that is not yet leased.</summary>
    public TenantPlacementSnapshotMaintainer AddPlacementSilo(ITenantRegistry registry) =>
        new(registry, Time, NullLogger<TenantPlacementSnapshotMaintainer>.Instance);

    /// <summary>Creates a placement snapshot, leases it, and waits for the build, so it is authoritative on return.</summary>
    public async Task<TenantPlacementSnapshotMaintainer> AddLeasedPlacementSiloAsync(ITenantRegistry registry)
    {
        var silo = AddPlacementSilo(registry);
        Renew(silo);
        await silo.BackgroundRebuild;
        Assert.That(silo.IsSnapshotAuthoritative, Is.True, "precondition: a leased, built placement snapshot is authoritative");
        return silo;
    }

    /// <summary>Renews <paramref name="silo"/>'s lease against the current incarnation, as its subscription does.</summary>
    public void Renew(ITenantEpochSubscriber silo)
    {
        var requestedAt = Time.GetTimestamp();
        silo.ApplyLease(Ledger.Lease(silo), requestedAt);
    }

    /// <summary>Makes every push to <paramref name="silo"/> fail, as for a silo the grain cannot reach.</summary>
    public void MakeUnreachable(ITenantEpochSubscriber silo) => _unreachable.Add(silo);

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
    public static CompiledTenantPolicySnapshotMaintainer Unleased(
        ITenantRegistry registry,
        DelegatedTenantAccessFlag? delegatedAccess = null) =>
        new TenantPolicyEpochTestCluster().AddSilo(registry, delegatedAccess);

    /// <summary>A single leased, built (and so authoritative) maintainer.</summary>
    public static Task<CompiledTenantPolicySnapshotMaintainer> LeasedAsync(
        ITenantRegistry registry,
        DelegatedTenantAccessFlag? delegatedAccess = null) =>
        new TenantPolicyEpochTestCluster().AddLeasedSiloAsync(registry, delegatedAccess);

    /// <summary>A residency snapshot that is never leased, so it is never authoritative.</summary>
    public static TenantResidencySnapshotMaintainer UnleasedResidency(ITenantRegistry registry, string regionId = "eu") =>
        new TenantPolicyEpochTestCluster().AddResidencySilo(registry, regionId);

    /// <summary>A single leased, built (and so authoritative) residency snapshot.</summary>
    public static Task<TenantResidencySnapshotMaintainer> LeasedResidencyAsync(ITenantRegistry registry, string regionId = "eu") =>
        new TenantPolicyEpochTestCluster().AddLeasedResidencySiloAsync(registry, regionId);

    /// <summary>A placement snapshot that is never leased, so it is never authoritative.</summary>
    public static TenantPlacementSnapshotMaintainer UnleasedPlacement(ITenantRegistry registry) =>
        new TenantPolicyEpochTestCluster().AddPlacementSilo(registry);

    /// <summary>A single leased, built (and so authoritative) placement snapshot.</summary>
    public static Task<TenantPlacementSnapshotMaintainer> LeasedPlacementAsync(ITenantRegistry registry) =>
        new TenantPolicyEpochTestCluster().AddLeasedPlacementSiloAsync(registry);

    private TenantPolicyEpochLedger<ITenantEpochSubscriber> NewLedger() =>
        new(LeaseDuration, Time, Guid.NewGuid());

    private Task NotifyAsync(ITenantEpochSubscriber silo, TenantPolicyEpoch epoch)
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
