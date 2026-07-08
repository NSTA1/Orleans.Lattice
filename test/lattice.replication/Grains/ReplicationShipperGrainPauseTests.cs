using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage for the durable administrative shipping pause on
/// <see cref="ReplicationShipperGrain"/> (issue #1173). This pause is distinct
/// from the transient flow-control backoff: it survives a restart, is keyed by
/// the engaging saga, and while engaged the pump ships nothing and never
/// advances the cursor, so shipping resumes from the same point.
/// </summary>
[TestFixture]
public class ReplicationShipperGrainPauseTests
{
    private const string Tree = "orders";
    private const string Peer = "site-b";

    private static (ReplicationShipperGrain Grain,
                    FakePersistentState<ReplicationShipperState> State,
                    IReplicationTransport Transport) CreateGrain()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);

        var transport = Substitute.For<IReplicationTransport>();
        var persistent = new FakePersistentState<ReplicationShipperState>();

        var grain = new ReplicationShipperGrain(
            ctx,
            Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor,
            transport,
            Substitute.For<IReplicationBatchEncoder>(),
            Substitute.For<IWalRecordEncoder>(),
            Substitute.For<IWalCursorRegistry>(),
            Substitute.For<IGrainFactory>(),
            persistent,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(),
            new NoOpReplicationDigestProbeTransport());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, persistent, transport);
    }

    [Test]
    public async Task Pause_marks_shipping_paused_and_persists()
    {
        var (grain, state, _) = CreateGrain();

        await grain.PauseShippingAsync("saga-1", CancellationToken.None);

        Assert.That(await grain.IsShippingPausedAsync(), Is.True);
        Assert.That(state.State.AdminPauseSagaId, Is.EqualTo("saga-1"));
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task IsShippingPaused_false_on_fresh_grain()
    {
        var (grain, _, _) = CreateGrain();

        Assert.That(await grain.IsShippingPausedAsync(), Is.False);
    }

    [Test]
    public async Task Pause_is_idempotent_for_the_same_saga()
    {
        var (grain, state, _) = CreateGrain();

        await grain.PauseShippingAsync("saga-1", CancellationToken.None);
        await grain.PauseShippingAsync("saga-1", CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Pause_does_not_touch_the_cursor()
    {
        var (grain, state, _) = CreateGrain();
        var cursorBefore = state.State.Cursor;

        await grain.PauseShippingAsync("saga-1", CancellationToken.None);

        // The durable admin pause must never advance the resume cursor; shipping
        // resumes from exactly where it paused.
        Assert.That(state.State.Cursor, Is.EqualTo(cursorBefore));
    }

    [Test]
    public async Task Resume_for_a_non_owning_saga_is_a_no_op()
    {
        var (grain, state, _) = CreateGrain();
        await grain.PauseShippingAsync("saga-1", CancellationToken.None);

        // A superseded saga's late resume must not unpause a pause a newer saga
        // owns. This path returns before re-arming the pump.
        await grain.ResumeShippingAsync("saga-2", CancellationToken.None);

        Assert.That(await grain.IsShippingPausedAsync(), Is.True);
        Assert.That(state.State.AdminPauseSagaId, Is.EqualTo("saga-1"));
    }

    [Test]
    public async Task Paused_pump_ships_nothing_and_leaves_the_cursor_intact()
    {
        var (grain, state, transport) = CreateGrain();
        await grain.PauseShippingAsync("saga-1", CancellationToken.None);
        var cursorBefore = state.State.Cursor;

        // A pump tick while paused must short-circuit before touching the
        // transport, so no post-cut entry leaves the cluster.
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(transport.ReceivedCalls(), Is.Empty);
        Assert.That(state.State.Cursor, Is.EqualTo(cursorBefore));
    }

    [Test]
    public void Pause_rejects_null_or_empty_saga()
    {
        var (grain, _, _) = CreateGrain();

        Assert.That(() => grain.PauseShippingAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(() => grain.PauseShippingAsync(string.Empty, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }
}
