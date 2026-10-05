using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586 part 2b: the applied low watermark a shipper vouches for at its
/// peer. Every write the sender authored to the tree and stamped below it must
/// have been acknowledged, and visible, at the peer in the peer's current
/// lineage of the tree. Runs the real shipper over stub WAL partitions that
/// publish a clock floor, and the real per-peer aggregate.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private sealed class FrontierTransport(ReplicationShipperGrainTests.StubWalRecordEncoder encoder)
    {
        public List<ReplicationBatch> Batches { get; } = new();
        public List<WalRecord> Shipped { get; } = new();
        public Guid? Lineage { get; set; }
        public long? Echo { get; set; }
        public bool Accepting { get; set; } = true;

        public IReplicationTransport Build()
        {
            var transport = Substitute.For<IReplicationTransport>();
            transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var batch = call.Arg<ReplicationBatch>();
                    Batches.Add(batch);
                    var segments = batch.EncodedEnvelope?.EncodedEntries.ToArray() ?? [];
                    if (Accepting)
                    {
                        Shipped.AddRange(segments.Select(s => encoder.Decode(s.AsSpan())));
                    }

                    return Task.FromResult(new ReplicationAck
                    {
                        Accepted = Accepting,
                        HighestAppliedHlc = HybridLogicalClock.Zero,
                        BootstrapEpoch = Echo,
                        ReceiverLineage = Lineage,
                    });
                });
            return transport;
        }

        /// <summary>The frontier the latest batch carried.</summary>
        public ReplicationSourceFrontier? Latest => Batches.Count == 0 ? null : Batches[^1].SourceFrontier;
    }

    private static ReplicationSourceFrontierAggregateGrain FrontierAggregate(params string[] trees)
    {
        var membership = Substitute.For<IReplicatedTreeMembership>();
        membership.ReplicatedTrees.Returns(trees);
        return new ReplicationSourceFrontierAggregateGrain(
            membership, new FakePersistentState<ReplicationSourceFrontierAggregateState>());
    }

    private static WalRecord LocalSet(string tree, string key, HybridLogicalClock stamp) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = stamp,
        OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
    };

    private static (ReplicationShipperGrain Shipper, ReplicationShipperGrainTests.StubReplogShardGrain[] Feeds, FrontierTransport Transport, FakePersistentState<ReplicationShipperState> State)
        FrontierShipper(
            string tree,
            HybridLogicalClock floor,
            Action<IGrainFactory>? configureFactory = null,
            ILatticeMergeModeResolver? modeResolver = null,
            ReplicationSourceFrontierAggregateGrain? aggregate = null,
            Action<LatticeReplicationOptions>? configureOptions = null,
            ReplicationShipperGrainTests.StubWalRecordEncoder? walEncoder = null)
    {
        walEncoder ??= new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder) { ClockFloor = floor },
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder) { ClockFloor = floor },
        };
        var transport = new FrontierTransport(walEncoder);
        var state = new FakePersistentState<ReplicationShipperState>();
        aggregate ??= FrontierAggregate(tree);
        var shipper = CreateShipper(tree, feeds, walEncoder, transport.Build(), state: state, modeResolver: modeResolver,
            configureOptions: configureOptions,
            configureFactory: factory =>
            {
                factory.GetGrain<IReplicationSourceFrontierAggregateGrain>(TwoSiteClusterFixture.SiteBClusterId).Returns(aggregate);
                configureFactory?.Invoke(factory);
            });
        shipper.ClockFloorGateOpenForTesting = () => true;
        return (shipper, feeds, transport, state);
    }

    [Test]
    public async Task Shipper_vouches_for_the_published_floor_once_its_acknowledged_cursor_passes_it()
    {
        const string tree = "ccv-frontier-floor";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor);
        var lineage = Guid.NewGuid();
        transport.Lineage = lineage;

        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 11)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(transport.Batches.Select(b => b.SourceFrontier), Has.All.Null,
            "nothing is vouched for before the acknowledged cursor has passed a published floor");

        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(transport.Latest, Is.Not.Null);
        var frontier = transport.Latest!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(frontier.ReceiverLineage, Is.EqualTo(lineage), "tagged with the lineage the acknowledgements were taken under");
            Assert.That(frontier.TreeLowWatermark, Is.EqualTo(floor), "the minimum covered floor over the tree's partitions");
            Assert.That(frontier.OriginLowWatermark, Is.EqualTo(floor), "the only replicated tree is this one");
            Assert.That(frontier.OriginGeneration, Is.GreaterThan(0L));
            Assert.That(shipper.ReseedRequired, Is.False, "a first lineage seen before anything was acknowledged is not a gap");
        });
    }

    [Test]
    public async Task Shipper_vouches_for_nothing_the_peer_has_not_acknowledged()
    {
        const string tree = "ccv-frontier-unacked";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));
        transport.Lineage = Guid.NewGuid();
        transport.Accepting = false;
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 3);

        Assert.That(transport.Batches.Select(b => b.SourceFrontier), Has.All.Null,
            "a floor published past the acknowledged cursor covers a write the peer has not received");
    }

    [Test]
    public async Task An_acknowledgement_that_does_not_report_the_lineage_stops_the_watermark_until_one_does()
    {
        const string tree = "ccv-frontier-unreported";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));
        var lineage = Guid.NewGuid();
        transport.Lineage = lineage;
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.CurrentSourceFrontierForTesting, Is.Not.Null, "precondition: vouching");

        transport.Lineage = null;
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.CurrentSourceFrontierForTesting, Is.Null,
            "the batch the unreported acknowledgement covered may have landed under a lineage not yet seen");

        transport.Lineage = lineage;
        feeds[0].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.CurrentSourceFrontierForTesting?.ReceiverLineage, Is.EqualTo(lineage), "the same lineage reported again resumes it");
    }

    [Test]
    public async Task Shipper_vouches_for_nothing_while_the_peer_reports_no_lineage_or_an_empty_one()
    {
        const string tree = "ccv-frontier-nolineage";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));

        transport.Lineage = null;
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 2);
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(transport.Batches.Select(b => b.SourceFrontier), Has.All.Null, "an acknowledgement without a lineage vouches for nothing");

        transport.Lineage = Guid.Empty;
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 2);
        feeds[1].Append(LocalSet(tree, "d", Hlc(ticks, 62)));
        await PumpAsync(shipper, ticks: 1);

        Assert.Multiple(() =>
        {
            Assert.That(transport.Batches.Select(b => b.SourceFrontier), Has.All.Null, "a peer that tracks no lineage gets no watermark");
            Assert.That(shipper.ReseedRequired, Is.False, "an empty lineage is not a gap");
        });
    }

    [Test]
    public async Task A_new_receiver_lineage_after_acknowledgements_re_seeds_the_peer_and_vouches_again_only_after_the_replay()
    {
        const string tree = "ccv-frontier-lineage";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var (shipper, feeds, transport, state) = FrontierShipper(tree, floor);
        var first = Guid.NewGuid();
        transport.Lineage = first;
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(transport.Latest?.ReceiverLineage, Is.EqualTo(first), "precondition: vouching under the first lineage");

        // The peer's contents were replaced: a restore re-stamped its lineage.
        var second = Guid.NewGuid();
        transport.Lineage = second;
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.ReseedRequired, Is.True, "a new lineage after acknowledgements is a forced gap");

        var shippedBefore = transport.Batches.Count;
        feeds[1].Append(LocalSet(tree, "d", Hlc(ticks, 62)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(transport.Batches.Skip(shippedBefore).Select(b => b.SourceFrontier), Has.All.Null,
            "nothing is vouched for while the peer awaits its re-seed");

        // The peer re-seeds from a later export; the rewind replays the log.
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "e", Hlc(ticks, 63)));
        await PumpAsync(shipper, ticks: 6);
        feeds[0].Append(LocalSet(tree, "f", Hlc(ticks, 64)));
        await PumpAsync(shipper, ticks: 1);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.False, "precondition: the re-seed completed");
            Assert.That(state.State.ReplayFilterHorizon, Is.Null, "precondition: the replay passed its horizon");
            Assert.That(transport.Latest?.ReceiverLineage, Is.EqualTo(second), "vouching resumes, under the new lineage");
            Assert.That(transport.Batches.Where(b => b.SourceFrontier is { } f && f.ReceiverLineage == first).Count(),
                Is.EqualTo(transport.Batches.TakeWhile(b => b.SourceFrontier is not { } f || f.ReceiverLineage == first).Count(b => b.SourceFrontier is not null)),
                "no watermark tagged with the old lineage ships after the change");
        });
    }

    [Test]
    public async Task Lineage_re_seeds_of_one_peer_run_one_tree_at_a_time()
    {
        var ticks = DateTime.UtcNow.Ticks;
        var aggregate = FrontierAggregate("ccv-frontier-pace-1", "ccv-frontier-pace-2");
        var (a, aFeeds, aTransport, _) = FrontierShipper("ccv-frontier-pace-1", Hlc(ticks, 50), aggregate: aggregate);
        var (b, bFeeds, bTransport, _) = FrontierShipper("ccv-frontier-pace-2", Hlc(ticks, 50), aggregate: aggregate);
        aTransport.Lineage = bTransport.Lineage = Guid.NewGuid();
        aFeeds[0].Append(LocalSet("ccv-frontier-pace-1", "a", Hlc(ticks, 10)));
        bFeeds[0].Append(LocalSet("ccv-frontier-pace-2", "a", Hlc(ticks, 10)));
        await PumpAsync(a, ticks: 1);
        await PumpAsync(b, ticks: 1);

        // A rollout: the peer starts tracking a lineage for both trees at once.
        aTransport.Lineage = Guid.NewGuid();
        bTransport.Lineage = Guid.NewGuid();
        aFeeds[0].Append(LocalSet("ccv-frontier-pace-1", "b", Hlc(ticks, 60)));
        bFeeds[0].Append(LocalSet("ccv-frontier-pace-2", "b", Hlc(ticks, 60)));
        await PumpAsync(a, ticks: 1);
        await PumpAsync(b, ticks: 1);

        Assert.Multiple(() =>
        {
            Assert.That(a.ReseedRequired, Is.True, "the first tree takes the peer's re-seed slot");
            Assert.That(b.ReseedRequired, Is.False, "the second tree waits for the slot");
            Assert.That(b.CurrentSourceFrontierForTesting, Is.Null, "and vouches for nothing while it waits");
        });
    }

    [Test]
    public async Task A_rebind_discards_the_floors_of_the_retired_log()
    {
        const string tree = "ccv-frontier-rebind";
        const string rebound = tree + "-v2";
        var ticks = DateTime.UtcNow.Ticks;
        var oldFloor = Hlc(ticks, 90);
        var newFloor = Hlc(ticks, 40);
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var newFeeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder) { ClockFloor = newFloor },
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder) { ClockFloor = newFloor },
        };
        var (shipper, feeds, transport, state) = FrontierShipper(tree, oldFloor, factory =>
        {
            for (var p = 0; p < newFeeds.Length; p++)
            {
                factory.GetGrain<IWalShardGrain>($"{rebound}/{p}").Returns(newFeeds[p]);
            }
        }, walEncoder: walEncoder);
        state.State.BoundPhysicalTreeId = tree;
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 11)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.CurrentSourceFrontierForTesting?.TreeLowWatermark, Is.EqualTo(oldFloor), "precondition: vouching on the old log");

        // An online resize swaps the alias to a new log whose floor trails the old one.
        for (var i = 0; i < 3; i++)
        {
            newFeeds[0].Append(LocalSet(rebound, $"n{i}", Hlc(ticks, 20 + i)));
        }

        await shipper.NotifySourceIdentityChangedAsync(rebound, CancellationToken.None);
        await PumpAsync(shipper, ticks: 4);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReplayFilterHorizon, Is.Null, "precondition: the rebind's replay cleared");
            Assert.That(shipper.CurrentSourceFrontierForTesting?.TreeLowWatermark, Is.EqualTo(newFloor),
                "a floor the retired log published says nothing about the new log's offsets");
        });
    }

    [Test]
    public async Task A_key_filtered_tree_vouches_for_nothing()
    {
        const string tree = "ccv-frontier-filtered";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50), configureOptions: o => o.KeyPrefixes = ["a"]);
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a1", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        feeds[0].Append(LocalSet(tree, "a2", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(transport.Batches.Select(b => b.SourceFrontier), Has.All.Null,
            "writes the filter keeps from the peer are never applied there, so no watermark can cover them");
    }

    [Test]
    public async Task An_idle_link_carries_an_advanced_watermark_on_a_liveness_probe()
    {
        const string tree = "ccv-frontier-heartbeat";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.CurrentSourceFrontierForTesting, Is.Not.Null, "precondition: a watermark to deliver");

        await Task.Delay(ReplicationShipperGrain.SourceFrontierHeartbeatInterval + TimeSpan.FromMilliseconds(200));
        var before = transport.Batches.Count;
        await PumpAsync(shipper, ticks: 1);

        var probes = transport.Batches.Skip(before).Where(b => b.EncodedEnvelope?.EncodedEntries.Length is null or 0).ToList();
        Assert.That(probes.Select(b => b.SourceFrontier), Has.Some.Not.Null,
            "the idle link sends the watermark on a probe instead of waiting for the next write");
    }

    [Test]
    public async Task An_acknowledged_prepare_whose_terminal_is_not_acknowledged_caps_the_watermark()
    {
        const string tree = "ccv-frontier-prepare";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var txid = Guid.NewGuid();
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor, factory =>
            factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));
        transport.Lineage = Guid.NewGuid();
        var prepareStamp = Hlc(ticks, 20);
        feeds[0].Append(PreparedSet(tree, "k", 1, txid, prepareStamp, index: 0) with { AtomicBatchSize = 1 });
        await PumpAsync(shipper, ticks: 1);
        feeds[1].Append(LocalSet(tree, "x", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(transport.Latest?.TreeLowWatermark, Is.EqualTo(prepareStamp),
            "a prepared write is invisible at the peer until its terminal lands, so the watermark stays at it");

        feeds[1].Append(CommitTerminal(tree, txid, 0, Hlc(ticks, 61), atomicShardCount: 1));
        await PumpAsync(shipper, ticks: 2);
        feeds[1].Append(LocalSet(tree, "y", Hlc(ticks, 62)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(transport.Latest?.TreeLowWatermark, Is.EqualTo(floor),
            "once the terminal is acknowledged the saga no longer caps the watermark");
    }

    [Test]
    public async Task A_batch_the_cursor_passed_without_delivering_caps_the_watermark()
    {
        const string tree = "ccv-frontier-skip";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var calls = 0;
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns<LatticeMergeMode?>(_ =>
        {
            calls++;
            if (calls == 2)
            {
                throw new InvalidOperationException("schema-shaped encode failure");
            }

            return LatticeMergeMode.LwwRegister;
        });
        var (shipper, feeds, transport, state) = FrontierShipper(tree, floor, modeResolver: resolver);
        transport.Lineage = Guid.NewGuid();

        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        var skipped = Hlc(ticks, 30);
        feeds[0].Append(LocalSet(tree, "lost", skipped));
        await PumpAsync(shipper, ticks: 1);
        feeds[1].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);

        Assert.Multiple(() =>
        {
            Assert.That(transport.Shipped.Any(r => r.Key == "lost"), Is.False, "precondition: the batch failed to encode and was quarantined");
            Assert.That(state.State.Frontier.SkipClamp, Is.EqualTo(skipped));
            Assert.That(transport.Latest?.TreeLowWatermark, Is.EqualTo(skipped),
                "a write the cursor passed without delivering was never applied at the peer");
        });
    }
}
