using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the shipper's administrative and coordinator lifecycle
/// surface: the durable admin pause/resume saga hooks
/// (<see cref="ReplicationShipperGrain.ResumeShippingAsync"/> re-arming the
/// coordinator), the keepalive reminder re-arm (the perpetual
/// <c>InProgress</c> path), the in-flight pump re-entrancy guard, and the
/// grain-key parser's malformed-key rejections. These paths need a real
/// <see cref="ITimerRegistry"/> in the activation service provider (the
/// shared <c>Create</c> factory deliberately omits it and bypasses
/// <c>StartCoordinatorAsync</c>), so this file builds the grain with one wired.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>
    /// Builds a shipper whose injected grain context exposes an
    /// <see cref="ITimerRegistry"/> through its activation services, so the
    /// base-class <c>StartCoordinatorAsync</c> / <c>StartPhaseTimer</c> path
    /// (which resolves the timer registry off the grain context) runs to
    /// completion instead of throwing "No service for type ITimerRegistry".
    /// Returns the reminder and timer registries so callers can assert the
    /// coordinator actually re-armed.
    /// </summary>
    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain Feed,
        IReplicationTransport Transport,
        IReminderRegistry Reminders,
        ITimerRegistry TimerRegistry) CreateWithCoordinator(
            LatticeReplicationOptions? options = null,
            ReplicationShipperState? seedState = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var timerRegistry = Substitute.For<ITimerRegistry>();
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITimerRegistry)).Returns(timerRegistry);
        ctx.ActivationServices.Returns(services);

        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Monitor(options);
        var walRecordEncoder = new StubWalRecordEncoder();
        var feed = new StubReplogShardGrain(walRecordEncoder);
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new TestEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        if (seedState is not null)
        {
            fakeState.State = seedState;
        }

        var factory = BuildGrainFactory(null, new[] { feed }, Tree);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walRecordEncoder, registry, factory, fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport(),
            null, null, null);
        grain.InitializeForTesting(Tree, Peer);
        return (grain, fakeState, feed, transport, reminders, timerRegistry);
    }

    [Test]
    public async Task ResumeShippingAsync_when_saga_matches_clears_pause_and_rearms_coordinator()
    {
        var (grain, state, _, _, reminders, timerRegistry) = CreateWithCoordinator(
            seedState: new ReplicationShipperState { AdminPauseSagaId = "saga-42" });
        var writesBefore = state.WriteCount;

        await grain.ResumeShippingAsync("saga-42", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.AdminPauseSagaId, Is.Null, "resume must clear the durable pause saga id");
            Assert.That(state.WriteCount, Is.GreaterThan(writesBefore), "resume must persist the cleared state");
        });
        // StartCoordinatorAsync re-registers the keepalive reminder and arms the phase timer.
        await reminders.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shipper-keepalive", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        Assert.That(timerRegistry.ReceivedCalls(), Is.Not.Empty,
            "resume must re-arm the phase timer via the coordinator");
    }

    [Test]
    public async Task ResumeShippingAsync_when_saga_does_not_match_is_a_noop()
    {
        var (grain, state, _, _, reminders, _) = CreateWithCoordinator(
            seedState: new ReplicationShipperState { AdminPauseSagaId = "saga-42" });

        await grain.ResumeShippingAsync("different-saga", CancellationToken.None);

        Assert.That(state.State.AdminPauseSagaId, Is.EqualTo("saga-42"),
            "a mismatched saga id must leave the pause in place");
        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ReceiveReminder_keepalive_rearms_phase_timer_because_shipper_is_always_in_progress()
    {
        var (grain, _, _, _, _, timerRegistry) = CreateWithCoordinator();

        await grain.ReceiveReminder("shipper-keepalive", default);

        // InProgress is hard-wired true for the perpetual shipper, so the
        // keepalive reminder always re-arms the phase timer rather than
        // unregistering and deactivating.
        Assert.That(timerRegistry.ReceivedCalls(), Is.Not.Empty,
            "the keepalive reminder must re-arm the phase timer while InProgress is true");
    }

    [Test]
    public async Task ReceiveReminder_for_unknown_name_does_not_rearm()
    {
        var (grain, _, _, _, _, timerRegistry) = CreateWithCoordinator();

        await grain.ReceiveReminder("some-other-reminder", default);

        Assert.That(timerRegistry.ReceivedCalls(), Is.Empty,
            "a non-keepalive reminder name must be ignored");
    }

    [Test]
    public async Task ProcessNextPhase_reentrant_call_returns_without_double_shipping()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
        };
        var (grain, _, feed, transport, _, _) = CreateWithCoordinator(options);
        feed.Append(MakeEntry("k1"));

        var enteredSend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSend = new TaskCompletionSource<ReplicationAck>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                enteredSend.TrySetResult();
                return releaseSend.Task;
            });

        // Pump 1 suspends inside SendAsync with _pumpInFlight == true.
        var pump1 = grain.PumpForTestingAsync(CancellationToken.None);
        await enteredSend.Task;

        // Pump 2 observes the in-flight guard and returns synchronously
        // without entering the ship path at all.
        await grain.PumpForTestingAsync(CancellationToken.None);

        releaseSend.SetResult(new ReplicationAck
        {
            Accepted = true,
            HighestAppliedHlc = HybridLogicalClock.Zero,
        });
        await pump1;

        var sends = transport.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync));
        Assert.That(sends, Is.EqualTo(1),
            "the re-entrant pump must not ship a second batch while the first is in flight");
    }

    [Test]
    public void ParseGrainKey_rejects_empty_activation_key()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(default(GrainId));
        var grain = ConstructWith(ctx: ctx);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.EnsureActiveAsync(CancellationToken.None));
        Assert.That(ex!.Message, Does.Contain("activation key is empty"));
    }

    [Test]
    public void ParseGrainKey_rejects_key_without_peer_segment()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", "noslash"));
        var grain = ConstructWith(ctx: ctx);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.EnsureActiveAsync(CancellationToken.None));
        Assert.That(ex!.Message, Does.Contain("not in the expected"));
    }
}
