using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the per-tree maintenance grain. Tests bypass
/// <c>StartCoordinatorAsync</c> (which requires a real grain
/// scheduler) by constructing the grain directly and calling
/// <c>ProcessNextPhaseAsync</c>; the cadence-tracking persistent
/// state is pre-seeded through <see cref="FakePersistentState{T}"/>.
/// </summary>
[TestFixture]
public class ReplicationMaintenanceGrainTests
{
    private const string Tree = "maintenance-tree";

    private static (
        ReplicationMaintenanceGrain Grain,
        FakePersistentState<ReplicationMaintenanceState> State,
        IOptionsMonitor<LatticeReplicationOptions> Monitor,
        ILatticeReplicationGc Gc,
        ILatticeFallOffLogDetector Detector,
        ILatticeWalIntrospection Introspection,
        LatticeReplicationOptions Options,
        IGrainFactory GrainFactory) Create(
            LatticeReplicationOptions? options = null,
            ReplicationMaintenanceState? seed = null,
            string treeName = Tree)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("maintenance-grain", treeName));
        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var resolved = options ?? new LatticeReplicationOptions { ClusterId = "site-a" };
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);
        var gc = Substitute.For<ILatticeReplicationGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationGcReport(
                treeName,
                MinCursor: null,
                TtlCeilingHlc: null,
                CausalStable: null,
                BlockedFloor: null,
                ShardsScanned: 0,
                EntriesTrimmed: 0));
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.CheckAndTriggerAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(new FallOffLogDecision(false, HybridLogicalClock.Zero, false));
        var introspection = Substitute.For<ILatticeWalIntrospection>();
        var fakeState = new FakePersistentState<ReplicationMaintenanceState>();
        if (seed is not null)
        {
            fakeState.State = seed;
        }
        var grainFactory = Substitute.For<IGrainFactory>();
        var grain = new ReplicationMaintenanceGrain(
            context, reminders, NullLogger<ReplicationMaintenanceGrain>.Instance,
            monitor, gc, detector, introspection, grainFactory, fakeState);
        return (grain, fakeState, monitor, gc, detector, introspection, resolved, grainFactory);
    }

    // --- Constructor null guards ---

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("maintenance-grain", Tree));
        Assert.That(
            () => new ReplicationMaintenanceGrain(
                ctx, Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationMaintenanceGrain>.Instance,
                null!, Substitute.For<ILatticeReplicationGc>(),
                Substitute.For<ILatticeFallOffLogDetector>(),
                Substitute.For<ILatticeWalIntrospection>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationMaintenanceState>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_gc_is_null()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("maintenance-grain", Tree));
        Assert.That(
            () => new ReplicationMaintenanceGrain(
                ctx, Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationMaintenanceGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                null!, Substitute.For<ILatticeFallOffLogDetector>(),
                Substitute.For<ILatticeWalIntrospection>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationMaintenanceState>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_fall_off_detector_is_null()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("maintenance-grain", Tree));
        Assert.That(
            () => new ReplicationMaintenanceGrain(
                ctx, Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationMaintenanceGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<ILatticeReplicationGc>(),
                null!, Substitute.For<ILatticeWalIntrospection>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationMaintenanceState>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_wal_introspection_is_null()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("maintenance-grain", Tree));
        Assert.That(
            () => new ReplicationMaintenanceGrain(
                ctx, Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationMaintenanceGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<ILatticeReplicationGc>(),
                Substitute.For<ILatticeFallOffLogDetector>(),
                null!,
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationMaintenanceState>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("maintenance-grain", Tree));
        Assert.That(
            () => new ReplicationMaintenanceGrain(
                ctx, Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationMaintenanceGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<ILatticeReplicationGc>(),
                Substitute.For<ILatticeFallOffLogDetector>(),
                Substitute.For<ILatticeWalIntrospection>(),
                null!,
                new FakePersistentState<ReplicationMaintenanceState>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // --- EnsureActiveAsync ---

    [Test]
    public void EnsureActiveAsync_throws_when_grain_key_is_empty()
    {
        var ctx = Substitute.For<IGrainContext>();
        // default GrainId has Key.ToString() == "" — exactly the
        // empty-key shape the grain refuses to operate on.
        ctx.GrainId.Returns(default(GrainId));
        var grain = new ReplicationMaintenanceGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationMaintenanceGrain>.Instance,
            Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
            Substitute.For<ILatticeReplicationGc>(),
            Substitute.For<ILatticeFallOffLogDetector>(),
            Substitute.For<ILatticeWalIntrospection>(),
            Substitute.For<IGrainFactory>(),
            new FakePersistentState<ReplicationMaintenanceState>());

        Assert.That(
            async () => await grain.EnsureActiveAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void EnsureActiveAsync_observes_pre_cancelled_token()
    {
        var (grain, _, _, _, _, _, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await grain.EnsureActiveAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- ProcessNextPhaseAsync — first-tick fires both cadences ---

    [Test]
    public async Task ProcessNextPhaseAsync_runs_gc_on_first_tick()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = Array.Empty<string>(),
        };
        var (grain, state, _, gc, _, _, _, _) = Create(opts);

        await grain.ProcessNextPhaseAsync();

        await gc.Received(1).RunOnceAsync(Tree, Arg.Any<CancellationToken>());
        Assert.That(state.State.LastGcTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_does_not_advance_last_gc_ticks_when_gc_throws()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = Array.Empty<string>(),
        };
        var (grain, state, _, gc, _, _, _, _) = Create(opts);
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationGcReport>(_ => throw new InvalidOperationException("gc-failed"));

        // Failure is swallowed and logged; the cadence stamp does NOT
        // advance so the next phase tick retries rather than waiting
        // a full cadence interval. The keepalive reminder is the
        // backstop against a deterministically-failing GC.
        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.LastGcTicks, Is.EqualTo(0L));
        await gc.Received(1).RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_fall_off_probe_when_peers_null()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ReplicationPeers = null };
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts);

        await grain.ProcessNextPhaseAsync();

        await introspection.DidNotReceive().GetOldestAvailableHlcAsync(
            Arg.Any<string>(), Arg.Any<CancellationToken>());
        await detector.DidNotReceive().CheckAndTriggerAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_fall_off_probe_when_peers_empty()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = Array.Empty<string>(),
        };
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts);

        await grain.ProcessNextPhaseAsync();

        await introspection.DidNotReceive().GetOldestAvailableHlcAsync(
            Arg.Any<string>(), Arg.Any<CancellationToken>());
        await detector.DidNotReceive().CheckAndTriggerAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_fall_off_probe_when_oldest_hlc_is_null()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = new[] { "site-b" },
        };
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts);
        introspection.GetOldestAvailableHlcAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((HybridLogicalClock?)null);

        await grain.ProcessNextPhaseAsync();

        await detector.DidNotReceive().CheckAndTriggerAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_invokes_detector_for_each_peer()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = new[] { "site-b", "site-c", "site-d" },
        };
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts);
        var hlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        introspection.GetOldestAvailableHlcAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(hlc);

        await grain.ProcessNextPhaseAsync();

        await detector.Received(1).CheckAndTriggerAsync(Tree, "site-b", hlc, Arg.Any<CancellationToken>());
        await detector.Received(1).CheckAndTriggerAsync(Tree, "site-c", hlc, Arg.Any<CancellationToken>());
        await detector.Received(1).CheckAndTriggerAsync(Tree, "site-d", hlc, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_null_or_empty_peers()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = new[] { "", "  ", "site-b", null! },
        };
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts);
        introspection.GetOldestAvailableHlcAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });

        await grain.ProcessNextPhaseAsync();

        // Empty / null peer entries are skipped; only "site-b" gets a probe.
        await detector.Received(1).CheckAndTriggerAsync(
            Tree, "site-b", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
        await detector.DidNotReceive().CheckAndTriggerAsync(
            Tree, "", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_isolates_per_peer_detector_failures()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = new[] { "site-b", "site-c" },
        };
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts);
        introspection.GetOldestAvailableHlcAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        detector.CheckAndTriggerAsync(Tree, "site-b", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<FallOffLogDecision>(_ => throw new InvalidOperationException("peer-b-failed"));
        detector.CheckAndTriggerAsync(Tree, "site-c", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(new FallOffLogDecision(false, HybridLogicalClock.Zero, false));

        await grain.ProcessNextPhaseAsync();

        // Failure on site-b does not prevent site-c from being probed.
        await detector.Received(1).CheckAndTriggerAsync(
            Tree, "site-c", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    // --- Cadence respects intervals after the first run ---

    [Test]
    public async Task ProcessNextPhaseAsync_skips_gc_within_interval_window()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaintenanceGcInterval = TimeSpan.FromHours(1),
            MaintenanceFallOffCheckInterval = TimeSpan.FromHours(1),
            ReplicationPeers = Array.Empty<string>(),
        };
        var seed = new ReplicationMaintenanceState
        {
            LastGcTicks = DateTime.UtcNow.Ticks,
            LastFallOffCheckTicks = DateTime.UtcNow.Ticks,
        };
        var (grain, _, _, gc, _, _, _, _) = Create(opts, seed);

        await grain.ProcessNextPhaseAsync();

        await gc.DidNotReceive().RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_runs_gc_after_interval_elapses()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaintenanceGcInterval = TimeSpan.FromMilliseconds(1),
            MaintenanceFallOffCheckInterval = TimeSpan.FromHours(1),
            ReplicationPeers = Array.Empty<string>(),
        };
        // Seed last-run far in the past; the cadence check should fire.
        var seed = new ReplicationMaintenanceState
        {
            LastGcTicks = DateTime.UtcNow.Ticks - TimeSpan.FromHours(1).Ticks,
            LastFallOffCheckTicks = DateTime.UtcNow.Ticks,
        };
        var (grain, _, _, gc, _, _, _, _) = Create(opts, seed);

        await grain.ProcessNextPhaseAsync();

        await gc.Received(1).RunOnceAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_runs_gc_and_fall_off_independently_when_one_cadence_fired()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaintenanceGcInterval = TimeSpan.FromMilliseconds(1),
            MaintenanceFallOffCheckInterval = TimeSpan.FromHours(1),
            ReplicationPeers = new[] { "site-b" },
        };
        // GC is overdue; fall-off is still within its window.
        var seed = new ReplicationMaintenanceState
        {
            LastGcTicks = DateTime.UtcNow.Ticks - TimeSpan.FromHours(1).Ticks,
            LastFallOffCheckTicks = DateTime.UtcNow.Ticks,
        };
        var (grain, _, _, gc, detector, introspection, _, _) = Create(opts, seed);
        introspection.GetOldestAvailableHlcAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });

        await grain.ProcessNextPhaseAsync();

        await gc.Received(1).RunOnceAsync(Tree, Arg.Any<CancellationToken>());
        await detector.DidNotReceive().CheckAndTriggerAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }
}
