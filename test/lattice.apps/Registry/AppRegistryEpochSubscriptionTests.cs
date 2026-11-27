using System.Collections.Immutable;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using Orleans.Configuration;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The membership half of <see cref="AppRegistryEpochSubscription"/> (#6216). A silo newly declared dead
/// may have committed a registry write it never published, so the epoch does not show it: the snapshot is
/// invalidated and rebuilt from the registry. A silo falsely declared dead can still commit after the
/// declaration, so the snapshot is re-invalidated on a fixed cadence until the false-death horizon passes.
/// </summary>
[TestFixture]
public sealed class AppRegistryEpochSubscriptionTests
{
    private static readonly TimeSpan Interval = AppRegistryEpochLedger<IAppRegistryEpochObserver>.DefaultLeaseDuration;

    private FakeTimeProvider _time = null!;
    private AppRegistry _registry = null!;
    private InMemoryAppRegistryEpochSource _epoch = null!;
    private CompiledAppRegistrySnapshotMaintainer _maintainer = null!;
    private IGrainFactory _grainFactory = null!;
    private SteppedMembership _membership = null!;
    private AppRegistryEpochSubscription? _subscription;

    [SetUp]
    public void SetUp()
    {
        _time = new FakeTimeProvider(AppRegistryTestData.Start);
        _registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        _epoch = new InMemoryAppRegistryEpochSource();
        _maintainer = new CompiledAppRegistrySnapshotMaintainer(_registry, NullLogger<CompiledAppRegistrySnapshotMaintainer>.Instance, _time, _epoch);

        var grain = Substitute.For<IAppRegistryEpochGrain>();
        grain.LeaseAsync(Arg.Any<IAppRegistryEpochObserver>(), Arg.Any<SiloAddress>())
            .Returns(_ => new AppRegistryEpochLease(_epoch.Current, Interval));
        _grainFactory = Substitute.For<IGrainFactory>();
        _grainFactory.CreateObjectReference<IAppRegistryEpochObserver>(Arg.Any<IGrainObserver>()).Returns(Substitute.For<IAppRegistryEpochObserver>());
        _grainFactory.GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey, Arg.Any<string?>()).Returns(grain);
        _membership = new SteppedMembership();
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_subscription is not null)
        {
            await _subscription.StopAsync(CancellationToken.None);
        }

        _maintainer.Dispose();
    }

    [Test]
    public async Task A_newly_dead_silo_rebuilds_the_snapshot_from_the_registry_although_the_epoch_is_unchanged()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await StartLeasedAsync();
        await _membership.PublishAsync(Member(1, SiloStatus.Active), Member(2, SiloStatus.Active));

        // Silo 2 committed this write and stopped before publishing it: the epoch never moved.
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await _membership.PublishAsync(Member(1, SiloStatus.Active), Member(2, SiloStatus.Dead));

        await _maintainer.BackgroundRebuild;
        await _maintainer.EnsureCurrentAsync();
        Assert.That(_maintainer.Current.GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_write_committed_after_a_false_declaration_is_picked_up_within_one_interval()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await StartLeasedAsync();
        await _membership.PublishAsync(Member(1, SiloStatus.Active), Member(2, SiloStatus.Active));
        await _membership.PublishAsync(Member(1, SiloStatus.Active), Member(2, SiloStatus.Dead));
        await _maintainer.BackgroundRebuild;
        await _maintainer.EnsureCurrentAsync();

        // Silo 2 was alive: it commits after its declaration, fails to publish, then stops.
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        _time.Advance(Interval);
        await WaitForAsync(() => !_maintainer.IsLeasedCurrent || _maintainer.Current.GetEnabledTenantApps(TenantId.Default).Count == 1);
        await _maintainer.BackgroundRebuild;
        await _maintainer.EnsureCurrentAsync();

        Assert.That(_maintainer.Current.GetEnabledTenantApps(TenantId.Default), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Re_invalidation_stops_once_the_horizon_has_passed()
    {
        await StartLeasedAsync();
        await _membership.PublishAsync(Member(1, SiloStatus.Active));
        await _membership.PublishAsync(Member(1, SiloStatus.Dead));
        var subscription = _subscription!;
        await _maintainer.BackgroundRebuild;

        // A 25 s horizon on a 10 s interval: invalidations at 10 s, 20 s and a last one at 30 s.
        for (var i = 0; i < 3; i++)
        {
            var before = _maintainer.CurrentEpoch;
            _time.Advance(Interval);
            await WaitForAsync(() => _maintainer.CurrentEpoch > before);
            await _maintainer.BackgroundRebuild;
        }

        await subscription.HorizonLoop.WaitAsync(TimeSpan.FromSeconds(30));
        var rebuilds = _maintainer.CurrentEpoch;
        _time.Advance(Interval * 3);
        await _maintainer.BackgroundRebuild;

        Assert.That(_maintainer.CurrentEpoch, Is.EqualTo(rebuilds), "nothing re-invalidates once the horizon has passed");
    }

    [Test]
    public void EffectiveFalseDeathHorizon_is_the_table_refresh_plus_the_probe_timeout_and_never_shorter_than_the_interval()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                AppRegistryEpochSubscription.EffectiveFalseDeathHorizon(Membership(TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(5)), Interval),
                Is.EqualTo(TimeSpan.FromSeconds(65)));
            Assert.That(
                AppRegistryEpochSubscription.EffectiveFalseDeathHorizon(Membership(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1)), Interval),
                Is.EqualTo(Interval));
        });
    }

    private static ClusterMembershipOptions Membership(TimeSpan refresh, TimeSpan probe) =>
        new() { TableRefreshTimeout = refresh, ProbeTimeout = probe };

    private async Task StartLeasedAsync()
    {
        var localSilo = Substitute.For<ILocalSiloDetails>();
        localSilo.SiloAddress.Returns(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 7));
        _subscription = new AppRegistryEpochSubscription(
            _grainFactory,
            localSilo,
            _maintainer,
            _time,
            NullLogger<AppRegistryEpochSubscription>.Instance,
            _membership.Service,
            Options.Create(Membership(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(5))));
        await _subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;
        await _maintainer.BackgroundRebuild;
        await _maintainer.EnsureCurrentAsync();
        Assert.That(_maintainer.IsLeasedCurrent, Is.True, "precondition: the silo is leased and current");
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.That(condition(), Is.True);
    }

    private static ClusterMember Member(int port, SiloStatus status) =>
        new(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 1), status, $"silo-{port}");

    /// <summary>
    /// An <see cref="IClusterMembershipService"/> whose updates are published one at a time;
    /// <see cref="PublishAsync"/> completes only once the subscriber has asked for the next one.
    /// </summary>
    private sealed class SteppedMembership
    {
        private readonly Channel<ClusterMembershipSnapshot> _updates = Channel.CreateUnbounded<ClusterMembershipSnapshot>();
        private readonly Channel<bool> _handled = Channel.CreateUnbounded<bool>();
        private long _version;

        public SteppedMembership()
        {
            Service = Substitute.For<IClusterMembershipService>();
            Service.MembershipUpdates.Returns(Stream());
        }

        public IClusterMembershipService Service { get; }

        public async Task PublishAsync(params ClusterMember[] members)
        {
            var snapshot = new ClusterMembershipSnapshot(
                members.ToImmutableDictionary(m => m.SiloAddress),
                new MembershipVersion(++_version));
            await _updates.Writer.WriteAsync(snapshot);
            await _handled.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));
        }

        private async IAsyncEnumerable<ClusterMembershipSnapshot> Stream(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (true)
            {
                var snapshot = await _updates.Reader.ReadAsync(cancellationToken);
                yield return snapshot;
                _handled.Writer.TryWrite(true);
            }
        }
    }
}
