using System.Collections.Immutable;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantPolicyEpochSubscription"/>, the per-silo half of
/// the cross-silo tenant-policy currency protocol (issue #4030): leasing and
/// renewing against the epoch grain, applying pushed epochs, invalidating on a silo
/// declared dead, and warming the snapshot at start-up. The grain factory, grain and
/// membership service are substitutes; timers run on a
/// <see cref="TimerSignalingTimeProvider"/> and membership updates are stepped, so
/// every wait is gated rather than timed.
/// </summary>
[TestFixture]
public sealed class TenantPolicyEpochSubscriptionTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(9);
    private static readonly Guid Incarnation = Guid.NewGuid();

    private TimerSignalingTimeProvider _time = null!;
    private IGrainFactory _grainFactory = null!;
    private ITenantPolicyEpochGrain _grain = null!;
    private ITenantPolicyEpochObserver _reference = null!;
    private SteppedMembership _membership = null!;
    private ILocalSiloDetails _localSilo = null!;
    private HoldableTenantRegistry _registry = null!;
    private CompiledTenantPolicySnapshotMaintainer _maintainer = null!;
    private TenantPolicyEpochSubscription? _subscription;

    [SetUp]
    public void SetUp()
    {
        _time = new TimerSignalingTimeProvider();
        _grain = Substitute.For<ITenantPolicyEpochGrain>();
        _reference = Substitute.For<ITenantPolicyEpochObserver>();
        _grainFactory = Substitute.For<IGrainFactory>();
        _grainFactory.CreateObjectReference<ITenantPolicyEpochObserver>(Arg.Any<IGrainObserver>()).Returns(_reference);
        _grainFactory.GetGrain<ITenantPolicyEpochGrain>(ITenantPolicyEpochGrain.Key, Arg.Any<string?>()).Returns(_grain);
        _grain.LeaseAsync(_reference, Arg.Any<SiloAddress>()).Returns(new TenantPolicyEpochLease(new TenantPolicyEpoch(Incarnation, 0), Lease));
        _membership = new SteppedMembership();
        _localSilo = Substitute.For<ILocalSiloDetails>();
        _localSilo.SiloAddress.Returns(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 7));

        _registry = new HoldableTenantRegistry();
        _registry.Records.Add(Record("acme", admins: ["alice"]));
        _maintainer = new CompiledTenantPolicySnapshotMaintainer(
            _registry,
            Substitute.For<ITenantPolicyEpochPublisher>(),
            _time,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_subscription is not null)
        {
            await _subscription.StopAsync(CancellationToken.None);
        }
    }

    private TenantPolicyEpochSubscription Create() =>
        _subscription = new TenantPolicyEpochSubscription(
            _grainFactory,
            [_maintainer],
            _membership.Service,
            _localSilo,
            _time,
            Options.Create(new LatticeTenancyOptions { PolicySnapshotLeaseDuration = Lease }),
            NullLogger<TenantPolicyEpochSubscription>.Instance);

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var options = Options.Create(new LatticeTenancyOptions());
        var logger = NullLogger<TenantPolicyEpochSubscription>.Instance;
        ITenantEpochSubscriber[] subscribers = [_maintainer];
        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantPolicyEpochSubscription(null!, subscribers, _membership.Service, _localSilo, _time, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantPolicyEpochSubscription(_grainFactory, null!, _membership.Service, _localSilo, _time, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantPolicyEpochSubscription(_grainFactory, subscribers, null!, _localSilo, _time, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantPolicyEpochSubscription(_grainFactory, subscribers, _membership.Service, null!, _time, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantPolicyEpochSubscription(_grainFactory, subscribers, _membership.Service, _localSilo, null!, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantPolicyEpochSubscription(_grainFactory, subscribers, _membership.Service, _localSilo, _time, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantPolicyEpochSubscription(_grainFactory, subscribers, _membership.Service, _localSilo, _time, options, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task StartAsync_leases_through_an_observer_reference_and_makes_the_snapshot_authoritative()
    {
        var subscription = Create();

        await subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;
        await subscription.Warmup;
        await _maintainer.BackgroundRebuild;

        _grainFactory.Received(1).CreateObjectReference<ITenantPolicyEpochObserver>(subscription);
        await _grain.Received(1).LeaseAsync(_reference, _localSilo.SiloAddress);
        Assert.That(_maintainer.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task Lease_is_renewed_every_third_of_its_duration()
    {
        var subscription = Create();
        await subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;
        await NextLeaseTimerAsync();

        _time.Advance(Lease / 3);
        await NextLeaseTimerAsync();

        await _grain.Received(2).LeaseAsync(_reference, _localSilo.SiloAddress);
    }

    [Test]
    public async Task Failed_lease_is_retried_after_a_tenth_of_the_lease_capped_at_one_second()
    {
        _grain.LeaseAsync(_reference, Arg.Any<SiloAddress>()).Returns(
            _ => Task.FromException<TenantPolicyEpochLease>(new TimeoutException("grain unreachable")),
            _ => Task.FromResult(new TenantPolicyEpochLease(new TenantPolicyEpoch(Incarnation, 0), Lease)));
        var subscription = Create();
        await subscription.StartAsync(CancellationToken.None);
        await NextLeaseTimerAsync();
        Assert.That(_maintainer.LeaseEstablished.IsCompleted, Is.False, "the first lease request failed");

        _time.Advance(TimeSpan.FromSeconds(0.9));
        await _maintainer.LeaseEstablished;

        await _grain.Received(2).LeaseAsync(_reference, _localSilo.SiloAddress);
    }

    [Test]
    public async Task OnEpochAdvancedAsync_marks_the_snapshot_out_of_date_before_acknowledging()
    {
        var subscription = Create();
        await subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;
        await _maintainer.BackgroundRebuild;

        _registry.HoldScans();

        var ack = subscription.OnEpochAdvancedAsync(new TenantPolicyEpoch(Incarnation, 1));

        Assert.Multiple(() =>
        {
            Assert.That(ack.IsCompletedSuccessfully, Is.True);
            Assert.That(_maintainer.IsSnapshotAuthoritative, Is.False);
        });
        _registry.ReleaseScans();
        await _maintainer.BackgroundRebuild;
    }

    [Test]
    public async Task A_silo_declared_dead_invalidates_the_snapshot_but_the_baseline_does_not()
    {
        var subscription = Create();
        await subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;
        await _maintainer.BackgroundRebuild;

        await _membership.PublishAsync(Member(1, SiloStatus.Active), Member(2, SiloStatus.Dead));
        Assert.That(_maintainer.IsSnapshotAuthoritative, Is.True, "a silo already dead at start-up is the baseline");

        await _membership.PublishAsync(Member(1, SiloStatus.Active), Member(2, SiloStatus.Dead));
        Assert.That(_maintainer.IsSnapshotAuthoritative, Is.True, "an unchanged dead set invalidates nothing");

        _registry.HoldScans();
        await _membership.PublishAsync(Member(1, SiloStatus.Dead), Member(2, SiloStatus.Dead));
        Assert.That(_maintainer.IsSnapshotAuthoritative, Is.False, "a newly dead silo may have left a write unpublished");
        _registry.ReleaseScans();
        await _maintainer.BackgroundRebuild;
        Assert.That(_maintainer.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task Failed_membership_watch_is_treated_as_a_missed_death()
    {
        // No lease (grain unreachable), so the only builds are the start-up warm-up
        // and whatever the failed membership watch forces.
        _grain.LeaseAsync(_reference, Arg.Any<SiloAddress>()).ThrowsAsync(new TimeoutException("grain unreachable"));
        _membership.Service.MembershipUpdates.Returns(Failing());
        var subscription = Create();

        await subscription.StartAsync(CancellationToken.None);
        await subscription.Warmup;
        await _maintainer.BackgroundRebuild;

        Assert.That(_maintainer.CurrentEpoch, Is.EqualTo(2), "the warm-up build plus the rebuild the watch failure forced");
    }

    [Test]
    public async Task Warmup_builds_the_snapshot_even_when_the_epoch_grain_is_unreachable()
    {
        _grain.LeaseAsync(_reference, Arg.Any<SiloAddress>()).ThrowsAsync(new TimeoutException("grain unreachable"));
        var subscription = Create();

        await subscription.StartAsync(CancellationToken.None);
        await subscription.Warmup;

        Assert.Multiple(() =>
        {
            Assert.That(_maintainer.CurrentEpoch, Is.GreaterThan(0), "a cold silo does not report tenants unregistered");
            Assert.That(_maintainer.IsSnapshotAuthoritative, Is.False, "but without a lease it is not authoritative");
        });
    }

    [Test]
    public async Task Every_registered_snapshot_is_warmed_leased_pushed_and_invalidated()
    {
        var other = Substitute.For<ITenantEpochSubscriber>();
        var subscription = _subscription = new TenantPolicyEpochSubscription(
            _grainFactory,
            [_maintainer, other],
            _membership.Service,
            _localSilo,
            _time,
            Options.Create(new LatticeTenancyOptions { PolicySnapshotLeaseDuration = Lease }),
            NullLogger<TenantPolicyEpochSubscription>.Instance);

        await subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;
        await subscription.Warmup;
        var pushed = new TenantPolicyEpoch(Incarnation, 7);
        await subscription.OnEpochAdvancedAsync(pushed);
        await _membership.PublishAsync(Member(1, SiloStatus.Active));
        await _membership.PublishAsync(Member(1, SiloStatus.Dead));

        await other.Received(1).EnsureWarmAsync(Arg.Any<CancellationToken>());
        other.Received(1).ApplyLease(Arg.Is<TenantPolicyEpochLease>(l => l.Duration == Lease), Arg.Any<long>());
        other.Received(1).ObserveEpoch(pushed);
        other.Received(1).InvalidateClusterView();
    }

    [Test]
    public async Task StopAsync_stops_every_loop_and_deletes_the_observer_reference()
    {
        var subscription = Create();
        await subscription.StartAsync(CancellationToken.None);
        await _maintainer.LeaseEstablished;

        await subscription.StopAsync(CancellationToken.None);
        _subscription = null;

        _grainFactory.Received(1).DeleteObjectReference<ITenantPolicyEpochObserver>(_reference);
        Assert.Multiple(() =>
        {
            Assert.That(subscription.LeaseLoop.IsCompleted, Is.True);
            Assert.That(subscription.MembershipLoop.IsCompleted, Is.True);
            Assert.That(subscription.Warmup.IsCompleted, Is.True);
        });
    }

    [Test]
    public async Task StopAsync_before_StartAsync_is_a_no_op()
    {
        var subscription = Create();

        await subscription.StopAsync(CancellationToken.None);
        _subscription = null;

        _grainFactory.DidNotReceive().DeleteObjectReference<ITenantPolicyEpochObserver>(Arg.Any<IGrainObserver>());
    }

    /// <summary>
    /// Waits for the lease loop to arm its next delay. The warm-up never arms a
    /// timer here (its registry is healthy), so every timer is the lease loop's.
    /// </summary>
    private Task NextLeaseTimerAsync() => _time.NextTimerAsync();

    private static ClusterMember Member(int port, SiloStatus status) =>
        new(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 1), status, $"silo-{port}");

#pragma warning disable CS1998 // the throw is the point; no await is reachable
    private static async IAsyncEnumerable<ClusterMembershipSnapshot> Failing(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        throw new InvalidOperationException("membership unavailable");
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998

    /// <summary>
    /// An <see cref="IClusterMembershipService"/> whose updates are published one at
    /// a time; <see cref="PublishAsync"/> completes only once the subscriber has
    /// finished handling the snapshot (it has asked for the next one).
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
