using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the schema-shaped encode-failure legs on both ship paths. When
/// framing-header construction throws an <see cref="System.ArgumentException"/>
/// or <see cref="System.InvalidOperationException"/> (modelled here with a merge
/// mode resolver that throws), the offending batch is parked on the per-tree
/// dead-letter queue and the cursor advances strictly past it so a poison batch
/// never stalls the stream. A batch that cannot be parked - the queue is full,
/// or the enqueue fails - is never advanced past (#4603): the cursor stays put and
/// the next tick re-parks it, because advancing would lose it for the peer.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static ILatticeMergeModeResolver ThrowingModeResolver()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns<LatticeMergeMode?>(
            _ => throw new InvalidOperationException("schema-shaped-encode-boom"));
        return resolver;
    }

    private static (IGrainFactory Factory, IReplicationDeadLetterGrain Dlq) FactoryWithDeadLetters(
        Task<long>? enqueueResult = null)
    {
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        if (enqueueResult is not null)
        {
            dlq.EnqueueAsync(
                    Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(),
                    Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(enqueueResult);
        }
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);
        return (factory, dlq);
    }

    [Test]
    public async Task ShipMergedSerialBatchAsync_encode_failure_routes_to_dlq_and_advances_cursor()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
        };
        var (factory, dlq) = FactoryWithDeadLetters();
        var (grain, state, feed, transport, _, _, _) =
            Create(opts, grainFactory: factory, modeResolver: ThrowingModeResolver());
        feed.Append(MakeEntry("enc1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            Arg.Is<WalRecord>(r => r.Key == "enc1"),
            Arg.Any<string>(), 0, Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "an un-encodable batch must not ship an envelope");
            Assert.That(state.State.Cursor,
                Is.EqualTo(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }),
                "the cursor must advance strictly past the DLQ-parked batch");
        });
    }

    [Test]
    public async Task RouteBatchToDeadLetterAsync_enqueue_failure_holds_the_cursor_so_the_batch_is_not_lost()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
        };
        var (factory, dlq) = FactoryWithDeadLetters(
            Task.FromException<long>(new InvalidOperationException("dlq-unavailable")));
        var (grain, state, feed, transport, _, _, _) =
            Create(opts, grainFactory: factory, modeResolver: ThrowingModeResolver());
        feed.Append(MakeEntry("enc1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(),
            Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero),
            "an entry that could not be parked must not be advanced past (#4603)");
    }

    [Test]
    public async Task RouteBatchToDeadLetterAsync_full_queue_holds_the_cursor_and_reports_the_link_stalled()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
        };
        var (factory, _) = FactoryWithDeadLetters(
            Task.FromException<long>(new ReplicationDeadLetterQueueFullException(Tree, 1)));
        var peerStats = new ReplicationPeerStats();
        var (grain, state, feed, _, _, _, _) =
            Create(opts, grainFactory: factory, modeResolver: ThrowingModeResolver(), peerStats: peerStats);
        feed.Append(MakeEntry("enc1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        var row = peerStats.ReadStatusPage(new ReplicationPeerStatusReadRequest { TreeId = Tree, Limit = 10 })
            .Single(r => r.Direction == ReplicationContactDirection.Outbound);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero), "a full dead-letter queue holds the cursor");
            Assert.That(row.DeadLetterFullSeconds, Is.Not.Null, "the link reports as stalled, not quiet");
        });
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_deferred_encode_failure_routes_to_dlq_and_advances_cursor()
    {
        // Pipelined window (ShipMaxInFlight 2) with one-entry batches
        // (ShipBatchSize 1). The single batch fails encode, so the deferred
        // DLQ + AdvanceCursorPipelinedAsync leg runs once the (empty) in-flight
        // window has drained.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
            ShipMaxInFlight = 2,
            ShipBatchSize = 1,
        };
        var (factory, dlq) = FactoryWithDeadLetters();
        var (grain, state, feed, transport, _, _, _) =
            Create(opts, grainFactory: factory, modeResolver: ThrowingModeResolver());
        feed.Append(MakeEntry("penc1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            Arg.Is<WalRecord>(r => r.Key == "penc1"),
            Arg.Any<string>(), 0, Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "an un-encodable pipelined batch must not ship an envelope");
            Assert.That(state.State.Cursor,
                Is.EqualTo(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }),
                "the deferred encode-failure handler must advance the cursor past the parked batch");
        });
    }
}
