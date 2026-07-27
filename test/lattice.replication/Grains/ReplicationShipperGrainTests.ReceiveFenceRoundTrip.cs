using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// End-to-end ship -> ack -> cursor round-trip coverage for the durable inbound
/// receive fence (issue #1173). Proves the data-loss guard: while a tree's
/// receive fence is engaged the receiver defers the batch, the sender gets a
/// not-accepted ack and does NOT advance its per-peer cursor past the deferred
/// entries, and once the fence lifts the SAME entries are re-shipped and applied
/// (only then does the cursor advance). Exercises the real applier defer gate
/// and the real durable-cursor re-ship path, not the fence gate in isolation.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>Toggleable inbound receive fence used to open / close the fence mid-test.</summary>
    private sealed class ToggleReceiveGate : IReplicationReceiveGate
    {
        private volatile bool _paused;

        public bool Paused
        {
            get => _paused;
            set => _paused = value;
        }

        public ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default)
            => new(_paused);
    }

    /// <summary>
    /// In-process transport that decodes each shipped batch through the shared
    /// <see cref="IWalRecordEncoder"/> and replays it against a real
    /// <see cref="ReplicationApplier"/>, then honours
    /// <see cref="ApplyResult.Deferred"/> exactly as the production gRPC and
    /// loopback receive paths do: a fence-deferred batch yields a not-accepted
    /// ack; every other result stays accepted with the receiver frontier.
    /// </summary>
    private sealed class ReplayingApplierTransport(IWalRecordEncoder walEncoder, IReplicationApplier applier)
        : IReplicationTransport
    {
        public int SendCount { get; private set; }

        public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            SendCount++;
            var encoded = batch.EncodedEnvelope!.Value;
            var segments = encoded.EncodedEntries.Span;
            var decoded = new WalRecord[segments.Length];
            for (var i = 0; i < segments.Length; i++)
            {
                decoded[i] = walEncoder.Decode(segments[i].AsSpan());
            }

            var result = await applier.ApplyBatchAsync(decoded, cancellationToken).ConfigureAwait(false);
            return new ReplicationAck
            {
                Accepted = !result.Deferred,
                HighestAppliedHlc = result.HighWaterMark,
            };
        }
    }

    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain Feed,
        IReplicationApplyGrain ApplyGrain,
        ReplayingApplierTransport Transport,
        ToggleReceiveGate Gate) CreateReplayingApplierHarness()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));

        // Zero backoff so the not-accepted ack from the fenced pump does not
        // stall the immediate re-pump after the fence lifts. The durable
        // partition cursor - not a timer - is what governs whether the deferred
        // entries re-ship; this only removes the retry-window wait.
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipBackoffInitial = TimeSpan.Zero,
            ShipBackoffMax = TimeSpan.Zero,
            ShipBackoffJitter = 0.0,
        };
        var monitor = Monitor(options);

        var walRecordEncoder = new StubWalRecordEncoder();
        var feed = new StubReplogShardGrain(walRecordEncoder);
        var factory = BuildGrainFactory(null, new[] { feed }, Tree);

        // Receiver-side applier with its own cluster identity (the peer) so the
        // locally-authored entries are remote-origin on the receiver and thus
        // eligible to apply, fronted by the toggleable receive fence.
        var gate = new ToggleReceiveGate { Paused = true };
        var applyGrain = Substitute.For<IReplicationApplyGrain>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwmGrain.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwmGrain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        var applierFactory = Substitute.For<IGrainFactory>();
        applierFactory.GetGrain<IReplicationApplyGrain>(Tree).Returns(applyGrain);
        applierFactory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwmGrain);
        var applierMonitor = Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationOptions>>();
        var applierOptions = new LatticeReplicationOptions { ClusterId = Peer };
        applierMonitor.CurrentValue.Returns(applierOptions);
        applierMonitor.Get(Arg.Any<string>()).Returns(applierOptions);
        var applier = new ReplicationApplier(applierFactory, applierMonitor, receiveGate: gate, replicationContext: new OverridesReplicationContext());

        var transport = new ReplayingApplierTransport(walRecordEncoder, applier);

        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var grain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(), NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, new TestEncoder(), walRecordEncoder, Substitute.For<IWalCursorRegistry>(),
            factory, fakeState, new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, fakeState, feed, applyGrain, transport, gate);
    }

    [Test]
    public async Task Pump_keeps_cursor_while_receive_fenced_then_reships_and_applies_after_lift()
    {
        var (grain, state, feed, applyGrain, transport, gate) = CreateReplayingApplierHarness();
        var entryHlc = new HybridLogicalClock { WallClockTicks = 42, Counter = 0 };
        feed.Append(MakeEntry("k", ticks: 42));

        // Fence engaged: the batch ships, the receiver defers it (nothing
        // applied), the ack is not-accepted, and the sender must NOT advance its
        // cursor past the deferred entry.
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(transport.SendCount, Is.EqualTo(1), "the fenced batch must have shipped once");
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero),
            "a receive-fence defer must not advance the sender cursor past the deferred entry");
        await applyGrain.DidNotReceiveWithAnyArgs()
            .ApplySetAsync(default!, default!, default, default!, default, default);

        // Fence lifts: the SAME entry is re-shipped and applied end-to-end, and
        // only now does the cursor advance - so no entry was lost during the
        // pause window.
        gate.Paused = false;
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(transport.SendCount, Is.EqualTo(2),
            "the deferred entry must be re-shipped after the fence lifts");
        await applyGrain.Received(1).ApplySetAsync(
            "k", Arg.Any<byte[]>(), entryHlc, LocalCluster, Arg.Any<VersionVector?>(), Arg.Any<long>());
        Assert.That(state.State.Cursor, Is.EqualTo(entryHlc),
            "the cursor advances only after the entry is actually applied post-lift");
    }
}
