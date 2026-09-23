using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    private static async Task<(
        ReplicationShipperGrain Grain,
        IPersistentState<ReplicationShipperState> State,
        IWalCursorRegistry Registry,
        ILogger<ReplicationShipperGrain> Logger)> CreatePendingDeactivationAsync()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var state = Substitute.For<IPersistentState<ReplicationShipperState>>();
        state.State.Returns(new ReplicationShipperState());
        state.WriteStateAsync().Returns(Task.CompletedTask);
        var encoder = new StubWalRecordEncoder();
        var feed = new StubReplogShardGrain(encoder);
        feed.Append(MakeEntry("pending", ticks: 1));
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true });
        var registry = Substitute.For<IWalCursorRegistry>();
        var logger = Substitute.For<ILogger<ReplicationShipperGrain>>();
        var grain = new ReplicationShipperGrain(
            context, Substitute.For<IReminderRegistry>(), logger,
            Monitor(new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 100,
                ShipCursorWriteMaxDelay = Timeout.InfiniteTimeSpan,
            }),
            transport, new TestEncoder(), encoder, registry,
            BuildGrainFactory(null, [feed], Tree), state, new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(), new WireVersionNegotiationState(),
            new NoOpReplicationDigestProbeTransport());
        grain.InitializeForTesting(Tree, Peer);
        await grain.PumpForTestingAsync();
        await state.DidNotReceive().WriteStateAsync();
        Assert.That(state.State.PartitionCursors[0], Is.EqualTo(1));
        return (grain, state, registry, logger);
    }

    [Test]
    public async Task OnDeactivate_expired_deadline_skips_storage_and_logs_replay_risk()
    {
        var (grain, state, _, logger) = await CreatePendingDeactivationAsync();
        using var deadline = new CancellationTokenSource();
        await deadline.CancelAsync();

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"), deadline.Token);

        await state.DidNotReceive().WriteStateAsync();
        Assert.That(logger.ReceivedCalls().Any(call =>
            call.GetMethodInfo().Name == nameof(ILogger.Log)
            && Equals(call.GetArguments()[0], LogLevel.Warning)), Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task OnDeactivate_deadline_bounds_uncooperative_cursor_flush(bool stallRegistry)
    {
        var (grain, state, registry, logger) = await CreatePendingDeactivationAsync();
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (stallRegistry)
        {
            registry.ReportCursorAsync(
                    Arg.Any<string>(), Arg.Any<string>(),
                    Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
                .Returns(blocked.Task);
        }
        else
        {
            state.WriteStateAsync().Returns(blocked.Task);
        }
        using var deadline = new CancellationTokenSource();
        var deactivation = ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"), deadline.Token);
        try
        {
            Assert.That(deactivation.IsCompleted, Is.False, "The flush must reach the blocked operation.");
            await deadline.CancelAsync();
            await deactivation.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(blocked.Task.IsCompleted, Is.False,
                "Deactivation must finish without waiting for the storage/registry operation.");
            Assert.That(logger.ReceivedCalls().Any(call =>
                call.GetMethodInfo().Name == nameof(ILogger.Log)
                && Equals(call.GetArguments()[0], LogLevel.Warning)), Is.True);
        }
        finally
        {
            blocked.TrySetResult();
            await deactivation.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }
}
