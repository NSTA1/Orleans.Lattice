using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4533: a saga the origin decided, forgot and then purged - before the
/// decision-purge guard existed, or on a registry activation that predates it -
/// can keep records in the retained log. A replay (a re-seed's rewind to the
/// lowest retained entry, or a rebind's restart on a new log) re-ships them,
/// and the peer stages a prepare nothing can settle. The shipper must withhold
/// such a saga whole, decided from the origin's participant row and then its
/// decision, while shipping a saga still in flight and one whose decision the
/// origin keeps; and where the replay cannot carry a withheld saga's effects,
/// it must re-seed the peer instead. Runs the real shipper; the registry is the
/// stand-in.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private static ITxRegistryGrain ReplayRegistry(Guid purged, Guid kept, Guid live)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetParticipantsAsync(Arg.Any<Guid>()).Returns(Task.FromResult<IReadOnlyList<int>>(Array.Empty<int>()));
        registry.GetParticipantsAsync(live).Returns(Task.FromResult<IReadOnlyList<int>>(new[] { 0 }));
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.InFlight));
        registry.GetRecordedStatusAsync(kept).Returns(Task.FromResult(TxStatus.Committed));
        return registry;
    }

    [Test]
    public async Task Reseed_replay_withholds_a_purged_saga_whole_and_ships_live_and_decided_sagas()
    {
        const string tree = "ccv-replay-filter";
        var purged = Guid.NewGuid();
        var kept = Guid.NewGuid();
        var live = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        // Partition 0: a record the TTL trimmed, then the three sagas' prepares.
        // Partition 1: the purged and kept sagas' terminals; the live saga has
        // not decided.
        feeds[0].Append(PreparedSet(tree, "gone", 0, Guid.NewGuid(), Hlc(ticks, 1), index: 0));
        feeds[0].Append(PreparedSet(tree, "purged-key", 1, purged, Hlc(ticks, 2), index: 0) with { AtomicBatchSize = 1 });
        feeds[0].Append(PreparedSet(tree, "kept-key", 2, kept, Hlc(ticks, 3), index: 0) with { AtomicBatchSize = 1 });
        feeds[0].Append(PreparedSet(tree, "live-key", 3, live, Hlc(ticks, 4), index: 0) with { AtomicBatchSize = 1 });
        feeds[0].TrimmedThrough = 1;
        feeds[1].Append(CommitTerminal(tree, purged, 0, Hlc(ticks, 5), atomicShardCount: 1));
        feeds[1].Append(CommitTerminal(tree, kept, 0, Hlc(ticks, 6), atomicShardCount: 1));

        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var state = new FakePersistentState<ReplicationShipperState>();
        var registry = ReplayRegistry(purged, kept, live);
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state, configureFactory: factory =>
            factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        // The trim takes the peer off the log; the peer then re-seeds.
        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");
        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 12);

        var prepares = shipped.Where(r => r.IsPrepared).Select(r => r.TransactionId).ToList();
        var terminals = shipped.Where(r => r.Op is MutationKind.TxCommit or MutationKind.TxAbort).Select(r => r.TransactionId).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.False, "precondition: the re-seed completed");
            Assert.That(prepares, Does.Not.Contain(purged), "a purged saga's prepare must not reach the peer, where nothing can settle it");
            Assert.That(terminals, Does.Not.Contain(purged), "a purged saga is withheld whole: its terminal too");
            Assert.That(prepares, Does.Contain(kept), "a saga whose decision the origin keeps ships; the re-seed carried its decision");
            Assert.That(terminals, Does.Contain(kept), "and its terminal ships");
            Assert.That(prepares, Does.Contain(live), "a saga still in flight (its participants registered) ships; its decision will be kept");
            Assert.That(state.State.ReplayFilterHorizon, Is.Null, "the filter clears once every cursor passes the horizon");
        });
    }

    [Test]
    public async Task Rebind_replay_that_meets_a_purged_saga_re_seeds_the_peer()
    {
        const string tree = "ccv-replay-rebind";
        const string rebound = tree + "-v2";
        var purged = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        var newFeeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        newFeeds[0].Append(PreparedSet(rebound, "purged-key", 0, purged, Hlc(ticks, 1), index: 0) with { AtomicBatchSize = 1 });
        newFeeds[1].Append(CommitTerminal(rebound, purged, 0, Hlc(ticks, 2), atomicShardCount: 1));

        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => null);
        var state = new FakePersistentState<ReplicationShipperState>();
        state.State.BoundPhysicalTreeId = tree;
        var registry = ReplayRegistry(purged, Guid.NewGuid(), Guid.NewGuid());
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state, configureFactory: factory =>
        {
            for (var p = 0; p < newFeeds.Length; p++)
            {
                factory.GetGrain<IWalShardGrain>($"{rebound}/{p}").Returns(newFeeds[p]);
            }

            factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);
        });

        await shipper.NotifySourceIdentityChangedAsync(rebound, CancellationToken.None);
        Assert.That(state.State.ReplayFilterHorizon, Is.Not.Null, "precondition: a rebind starts a replay");
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True,
                "a rebind replay has no snapshot to carry a withheld saga's effects, so the peer must be re-seeded");
            Assert.That(shipped.Where(r => r.TransactionId == purged), Is.Empty, "no record of the purged saga ships");
        });
    }

    [Test]
    public async Task Replay_resumed_by_a_new_activation_that_meets_a_purged_saga_re_seeds_the_peer()
    {
        const string tree = "ccv-replay-resumed";
        var purged = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        feeds[0].Append(PreparedSet(tree, "purged-key", 0, purged, Hlc(ticks, 1), index: 0) with { AtomicBatchSize = 1 });
        feeds[1].Append(CommitTerminal(tree, purged, 0, Hlc(ticks, 2), atomicShardCount: 1));

        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => null);

        // An earlier activation started the replay; its verdicts died with it.
        var state = new FakePersistentState<ReplicationShipperState>();
        state.State.BoundPhysicalTreeId = tree;
        state.State.ReplayFilterHorizon = [1, 1];
        var registry = ReplayRegistry(purged, Guid.NewGuid(), Guid.NewGuid());
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state, configureFactory: factory =>
            factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True,
                "part of the saga may have shipped under a verdict an earlier activation held, so the peer must be re-seeded");
            Assert.That(shipped.Where(r => r.TransactionId == purged), Is.Empty, "no record of the purged saga ships");
        });
    }

    [Test]
    public async Task Reseed_holds_decision_purges_from_before_the_marker_until_the_replay_clears()
    {
        // A saga in flight at the re-seed's export can be decided, forgotten and
        // purged while the replay runs; the hold keeps its decision, so its
        // terminal reads as decided and ships (#4533).
        const string tree = "ccv-replay-hold";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var hold = Substitute.For<IWalPurgeHoldGrain>();
        var state = new FakePersistentState<ReplicationShipperState>();
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state, configureFactory: factory =>
        {
            factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);
            factory.GetGrain<IWalPurgeHoldGrain>(tree).Returns(hold);
        });

        await PumpAsync(shipper, ticks: 2);
        Assert.Multiple(async () =>
        {
            Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");
            Assert.That(state.State.ReplayHoldLog, Is.EqualTo(tree), "the replay hold is recorded with the marker");
            await hold.Received(1).AddAsync(Arg.Is<string>(k => k.EndsWith("#replay", StringComparison.Ordinal)), Arg.Any<long[]>());
            await hold.DidNotReceiveWithAnyArgs().RemoveAsync(default!);
        });

        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 6);

        Assert.Multiple(async () =>
        {
            Assert.That(state.State.ReplayFilterHorizon, Is.Null, "precondition: the replay passed its horizon");
            Assert.That(state.State.ReplayHoldLog, Is.Null, "the hold is released once the replay clears");
            await hold.Received(1).RemoveAsync(Arg.Is<string>(k => k.EndsWith("#replay", StringComparison.Ordinal)));
        });
    }

    private static IReplicationTransport RecordingTransport(
        ReplicationShipperGrainTests.StubWalRecordEncoder walEncoder, List<WalRecord> shipped, Func<long?> echo)
    {
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batch = call.Arg<ReplicationBatch>();
                var segments = batch.EncodedEnvelope?.EncodedEntries.ToArray() ?? [];
                shipped.AddRange(segments.Select(s => walEncoder.Decode(s.AsSpan())));
                return Task.FromResult(new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = HybridLogicalClock.Zero,
                    BootstrapEpoch = echo(),
                });
            });
        return transport;
    }
}
