using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4673, sender side: the shipper stamps every push with the source
/// lineage its binding was read under, and a move of the binding to a new
/// lineage - an alias move to a restored log, or a purge and recreate over the
/// same log - forces a gap and never ships a record the bound log held at the
/// change, the re-seed rewind included. Runs the real shipper over stub WAL
/// partitions and a stub registry row.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private sealed class LineageRegistry
    {
        public TreeRegistryEntry? Entry { get; set; } = new() { Lineage = Guid.NewGuid() };

        public ILatticeRegistry Build(string tree)
        {
            var registry = Substitute.For<ILatticeRegistry>();
            registry.GetEntryAsync(tree).Returns(_ => Task.FromResult<TreeRegistryEntry?>(Entry));
            registry.ResolveAsync(tree).Returns(_ => Task.FromResult(Entry?.PhysicalTreeId ?? tree));
            return registry;
        }
    }

    private static (ReplicationShipperGrain Shipper, ReplicationShipperGrainTests.StubReplogShardGrain[] Feeds, ReplicationShipperGrainTests.StubReplogShardGrain[] Restored, FrontierTransport Transport, LineageRegistry Registry)
        LineageShipper(string tree, string restoredPhysical)
    {
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        var restored = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        var transport = new FrontierTransport(walEncoder);
        var lineage = new LineageRegistry();
        var registry = lineage.Build(tree);
        var shipper = CreateShipper(tree, feeds, walEncoder, transport.Build(),
            configureFactory: factory =>
            {
                factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
                for (var p = 0; p < restored.Length; p++)
                {
                    factory.GetGrain<IWalShardGrain>($"{restoredPhysical}/{p}").Returns(restored[p]);
                }
            });
        return (shipper, feeds, restored, transport, lineage);
    }

    private static Guid? LastDataStamp(FrontierTransport transport) =>
        transport.Batches.Last(b => b.EncodedEnvelope?.EncodedEntries.Length > 0).SourceLineage;

    [Test]
    public async Task An_alias_move_to_a_new_lineage_stamps_the_new_lineage_and_never_ships_what_the_restored_log_held()
    {
        const string tree = "ccv-lineage-alias";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, restored, transport, registry) = LineageShipper(tree, "ccv-lineage-alias-p2");
        var original = registry.Entry!.Lineage;

        feeds[0].Append(LocalSet(tree, "before", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        var stampBefore = LastDataStamp(transport);

        // A restore wrote the restored contents into a new physical tree, then
        // moved the alias to it, re-stamping the lineage in the same row write.
        restored[0].Append(LocalSet(tree, "restored-content", Hlc(ticks, 5)));
        var restoredLineage = Guid.NewGuid();
        registry.Entry = registry.Entry! with { PhysicalTreeId = "ccv-lineage-alias-p2", Lineage = restoredLineage };
        await shipper.NotifySourceIdentityChangedAsync("ccv-lineage-alias-p2", CancellationToken.None);
        var gapForced = shipper.ReseedRequired;

        restored[1].Append(LocalSet(tree, "after", Hlc(ticks, 20)));
        await PumpAsync(shipper, ticks: 2);
        var stampAfter = LastDataStamp(transport);

        // The peer re-seeds from an export opened after the marker; the rewind
        // replays the restored log from its start.
        transport.Echo = 1;
        restored[1].Append(LocalSet(tree, "echo-carrier", Hlc(ticks, 30)));
        await PumpAsync(shipper, ticks: 6);

        Assert.Multiple(() =>
        {
            Assert.That(stampBefore, Is.EqualTo(original), "a push carries the lineage its binding was read under");
            Assert.That(gapForced, Is.True, "a binding that moved to a new lineage takes the peer off the log");
            Assert.That(stampAfter, Is.EqualTo(restoredLineage), "pushes read from the new log carry the new lineage");
            Assert.That(transport.Shipped.Any(r => r.Key == "after"), Is.True, "records written after the move ship");
            Assert.That(transport.Shipped.Any(r => r.Key == "restored-content"), Is.False,
                "a record the new log held at the move is carried by the re-seed export, never shipped, the rewind included");
            Assert.That(shipper.ReseedRequired, Is.False, "the re-seed settles: the skipped records do not loop the rewind");
        });
    }

    [Test]
    public async Task A_purge_and_recreate_over_the_same_log_never_ships_its_old_lineage_records()
    {
        const string tree = "ccv-lineage-purge";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, _, transport, registry) = LineageShipper(tree, "unused");

        feeds[0].Append(LocalSet(tree, "shipped-before", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);

        // A record of the old contents is still in the log, unshipped, when the
        // tree is purged and recreated under the same physical id and log.
        transport.Accepting = false;
        feeds[1].Append(LocalSet(tree, "old-lineage", Hlc(ticks, 11)));
        await PumpAsync(shipper, ticks: 1);
        transport.Accepting = true;
        var recreated = Guid.NewGuid();
        registry.Entry = registry.Entry! with { Lineage = recreated };
        await shipper.NotifySourceIdentityChangedAsync(tree, CancellationToken.None);
        var gapForced = shipper.ReseedRequired;

        feeds[1].Append(LocalSet(tree, "new-lineage", Hlc(ticks, 20)));
        await PumpAsync(shipper, ticks: 2);
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "echo-carrier", Hlc(ticks, 30)));
        await PumpAsync(shipper, ticks: 6);

        Assert.Multiple(() =>
        {
            Assert.That(gapForced, Is.True, "a lineage change over the same log takes the peer off the log");
            Assert.That(transport.Shipped.Any(r => r.Key == "new-lineage"), Is.True, "the recreated tree's writes ship");
            Assert.That(LastDataStamp(transport), Is.EqualTo(recreated));
            Assert.That(transport.Shipped.Any(r => r.Key == "old-lineage"), Is.False,
                "an old-lineage record the shared log still held is never shipped, the re-seed rewind included");
            Assert.That(shipper.ReseedRequired, Is.False, "the re-seed settles: the rewind does not loop on the skipped records");
        });
    }

    [Test]
    public async Task A_resize_keeps_the_lineage_and_replays_the_new_log_without_a_gap()
    {
        const string tree = "ccv-lineage-resize";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, restored, transport, registry) = LineageShipper(tree, "ccv-lineage-resize-p2");
        var lineage = registry.Entry!.Lineage;
        feeds[0].Append(LocalSet(tree, "before", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);

        restored[0].Append(LocalSet(tree, "copied", Hlc(ticks, 5)));
        registry.Entry = registry.Entry! with { PhysicalTreeId = "ccv-lineage-resize-p2" };
        await shipper.NotifySourceIdentityChangedAsync("ccv-lineage-resize-p2", CancellationToken.None);
        await PumpAsync(shipper, ticks: 2);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.False, "a physical move under an unchanged lineage is no gap");
            Assert.That(transport.Shipped.Any(r => r.Key == "copied"), Is.True, "the new log replays from its start (#4533)");
            Assert.That(LastDataStamp(transport), Is.EqualTo(lineage));
        });
    }

    [Test]
    public async Task A_refusal_for_the_source_lineage_on_a_current_binding_re_seeds_the_peer()
    {
        const string tree = "ccv-lineage-refused";
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        var refuse = true;
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new ReplicationAck
            {
                Accepted = !refuse,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SourceLineageRefused = refuse,
            }));
        var registry = new LineageRegistry().Build(tree);
        var shipper = CreateShipper(tree, feeds, walEncoder, transport,
            configureFactory: factory =>
                factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry));

        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);

        Assert.That(shipper.ReseedRequired, Is.True,
            "a current binding the peer did not drain is re-seeded, so the peer drains the current lineage");
    }

    [Test]
    public async Task A_refusal_while_the_binding_lineage_is_unknown_does_not_re_seed_the_peer()
    {
        const string tree = "ccv-lineage-unknown";
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new ReplicationAck
            {
                Accepted = false,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SourceLineageRefused = true,
            }));

        // No registry row: the registry tracks no lineage, so nothing is stamped
        // and a refusal cannot be answered by a re-seed.
        var shipper = CreateShipper(tree, feeds, walEncoder, transport);
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 2);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.SourceLineageStampForTesting, Is.Null);
            Assert.That(shipper.ReseedRequired, Is.False, "an unknown lineage only re-resolves and backs off");
        });
    }
}
