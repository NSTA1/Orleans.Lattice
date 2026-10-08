using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4673: a restart that re-reads the same lineage and a backstop
/// re-resolve of an unchanged row must never force a gap. Registering a tree
/// for the first time after its shipper bound it is not a lineage change. A
/// recreate that passes through an unregistered tree is still a change. Runs
/// the real shipper, re-activated over its own persisted state.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private static (ReplicationShipperGrain Shipper, FrontierTransport Transport) RestartableLineageShipper(
        string tree,
        ReplicationShipperGrainTests.StubReplogShardGrain[] feeds,
        ReplicationShipperGrainTests.StubWalRecordEncoder walEncoder,
        FakePersistentState<ReplicationShipperState> state,
        ILatticeRegistry registry)
    {
        var transport = new FrontierTransport(walEncoder);
        var shipper = CreateShipper(tree, feeds, walEncoder, transport.Build(), state: state,
            configureOptions: o => o.ShipSourceIdentityBackstopInterval = TimeSpan.FromTicks(1),
            configureFactory: factory =>
                factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry));
        return (shipper, transport);
    }

    private static ReplicationShipperGrainTests.StubReplogShardGrain[] LineageFeeds(ReplicationShipperGrainTests.StubWalRecordEncoder walEncoder) =>
    [
        new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
    ];

    [Test]
    public async Task A_restart_that_re_reads_an_unchanged_lineage_forces_no_gap_and_ships_every_record()
    {
        const string tree = "ccv-lineage-restart";
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = LineageFeeds(walEncoder);
        var state = new FakePersistentState<ReplicationShipperState>();
        var lineage = new LineageRegistry();
        var registry = lineage.Build(tree);

        var (before, _) = RestartableLineageShipper(tree, feeds, walEncoder, state, registry);
        feeds[0].Append(LocalSet(tree, "before-restart", Hlc(ticks, 10)));
        await PumpAsync(before, ticks: 2);

        // The peer is unreachable when the silo restarts with a record unshipped.
        feeds[1].Append(LocalSet(tree, "unshipped", Hlc(ticks, 11)));
        var (after, transport) = RestartableLineageShipper(tree, feeds, walEncoder, state, registry);
        await PumpAsync(after, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(after.ReseedRequired, Is.False, "a restart re-reads the same lineage: no gap");
            Assert.That(state.State.SourceLineageBoundary, Is.Empty, "and no record is withheld");
            Assert.That(transport.Shipped.Any(r => r.Key == "unshipped"), Is.True, "the record unshipped at the restart ships");
            Assert.That(after.SourceLineageStampForTesting, Is.EqualTo(lineage.Entry!.Lineage));
        });
    }

    [Test]
    public async Task A_tree_registered_after_its_shipper_bound_it_is_no_lineage_change()
    {
        const string tree = "ccv-lineage-late-register";
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = LineageFeeds(walEncoder);
        var state = new FakePersistentState<ReplicationShipperState>();
        var lineage = new LineageRegistry { Entry = null };
        var registry = lineage.Build(tree);

        // The shipper activates at silo start, before the tree's first write
        // registers it.
        var (before, _) = RestartableLineageShipper(tree, feeds, walEncoder, state, registry);
        await PumpAsync(before, ticks: 1);
        lineage.Entry = new() { Lineage = Guid.NewGuid() };
        feeds[0].Append(LocalSet(tree, "first-write", Hlc(ticks, 10)));
        await PumpAsync(before, ticks: 2);

        // Restart while a record is unshipped: it still ships under the
        // lineage the registration stamped because no prior lineage existed.
        feeds[1].Append(LocalSet(tree, "unshipped", Hlc(ticks, 11)));
        var (after, transport) = RestartableLineageShipper(tree, feeds, walEncoder, state, registry);
        await PumpAsync(after, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(before.ReseedRequired || after.ReseedRequired, Is.False,
                "the first registration of a tree replaced nothing, so it is no gap");
            Assert.That(state.State.SourceLineageBoundary, Is.Empty);
            Assert.That(transport.Shipped.Any(r => r.Key == "unshipped"), Is.True);
            Assert.That(after.SourceLineageStampForTesting, Is.EqualTo(lineage.Entry!.Lineage));
        });
    }

    [Test]
    public async Task A_recreate_that_passes_through_an_unregistered_tree_is_still_a_lineage_change()
    {
        const string tree = "ccv-lineage-unregistered";
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = LineageFeeds(walEncoder);
        var state = new FakePersistentState<ReplicationShipperState>();
        var lineage = new LineageRegistry();
        var registry = lineage.Build(tree);
        var (shipper, _) = RestartableLineageShipper(tree, feeds, walEncoder, state, registry);
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 2);

        lineage.Entry = null;
        await PumpAsync(shipper, ticks: 1);
        var whileUnregistered = shipper.ReseedRequired;
        lineage.Entry = new() { Lineage = Guid.NewGuid() };
        await PumpAsync(shipper, ticks: 1);

        Assert.Multiple(() =>
        {
            Assert.That(whileUnregistered, Is.False, "an unregistered tree is not yet a change");
            Assert.That(shipper.ReseedRequired, Is.True, "the recreate's new lineage is a change against the last one seen");
        });
    }
}
