using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the WAL reclamation read (#4195) on <see cref="LatticeTreeAdmin"/>:
/// it authorizes whole-tree read first, probes the floor holder at the tree's
/// physical alias target, echoes the caller's own tree name, and keys the wedge on
/// the holder's pin offset beside its state - so a pin at <c>-1</c> in the same
/// never-checkpointed state reads as the benign sentinel, not as a wedge.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminWalReclamationTests
{
    private const string Tree = "orders";
    private const string Physical = "orders-resized";

    private sealed class FixedGate(bool allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static ILatticeWalReclamation Create(IGrainFactory factory, bool allow = true)
        => new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver());

    private static ILatticeWalFloorHolderProbe Probe(IGrainFactory factory, WalFloorHolderProbeReport report)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Tree).Returns(Physical);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var probe = Substitute.For<ILatticeWalFloorHolderProbe>();
        probe.ProbeAsync(Arg.Any<CancellationToken>()).Returns(report);
        factory.GetGrain<ILatticeWalFloorHolderProbe>(Physical).Returns(probe);
        return probe;
    }

    private static WalFloorHolderProbeReport Holder(long pinOffset, WalGcBlockingPinState state, long? checkpoint) => new()
    {
        TreeId = Physical,
        PinStoreReadable = true,
        PinCount = 3,
        PinsWithoutOffset = pinOffset < 0 ? 3 : 1,
        ConsumerId = "_lattice_materialiser_orders-resized_bplusleaf/abc",
        LeafId = "bplusleaf/abc",
        Partition = 1,
        PinOffset = pinOffset,
        PersistedCheckpoint = checkpoint,
        State = state,
    };

    [Test]
    public async Task GetWalReclamationAsync_reports_a_wedge_for_a_usable_pin_above_a_never_persisted_checkpoint()
    {
        var factory = Substitute.For<IGrainFactory>();
        Probe(factory, Holder(42, WalGcBlockingPinState.NeverCheckpointed, -1));

        var report = await Create(factory).GetWalReclamationAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree), "the caller's own name, never the physical alias target");
            Assert.That(report.PinStoreReadable, Is.True);
            Assert.That(report.PinCount, Is.EqualTo(3));
            Assert.That(report.PinsWithoutOffset, Is.EqualTo(1));
            Assert.That(report.FloorHolder, Is.Not.Null);
            Assert.That(report.FloorHolder!.LeafId, Is.EqualTo("bplusleaf/abc"));
            Assert.That(report.FloorHolder.Partition, Is.EqualTo(1));
            Assert.That(report.FloorHolder.PinOffset, Is.EqualTo(42));
            Assert.That(report.FloorHolder.PersistedCheckpoint, Is.EqualTo(-1));
            Assert.That(report.FloorHolder.State, Is.EqualTo(TreeWalFloorHolderState.NeverCheckpointed));
            Assert.That(report.FloorHolder.HoldsOffsetFloor, Is.True);
            Assert.That(report.IsWedged, Is.True);
        });
    }

    [Test]
    public async Task GetWalReclamationAsync_does_not_report_a_wedge_for_the_benign_minus_one_sentinel()
    {
        var factory = Substitute.For<IGrainFactory>();
        Probe(factory, Holder(-1, WalGcBlockingPinState.NeverCheckpointed, -1));

        var report = await Create(factory).GetWalReclamationAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder!.State, Is.EqualTo(TreeWalFloorHolderState.NeverCheckpointed));
            Assert.That(report.FloorHolder.HoldsOffsetFloor, Is.False);
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task GetWalReclamationAsync_does_not_report_a_wedge_for_a_checkpointed_holder()
    {
        var factory = Substitute.For<IGrainFactory>();
        Probe(factory, Holder(42, WalGcBlockingPinState.CheckpointedCoverageUnknown, 40));

        var report = await Create(factory).GetWalReclamationAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder!.State, Is.EqualTo(TreeWalFloorHolderState.CheckpointedCoverageUnknown));
            Assert.That(report.FloorHolder.PersistedCheckpoint, Is.EqualTo(40));
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task GetWalReclamationAsync_reports_no_holder_when_the_tree_holds_no_pin()
    {
        var factory = Substitute.For<IGrainFactory>();
        Probe(factory, new WalFloorHolderProbeReport { TreeId = Physical, PinStoreReadable = true, PinOffset = -1 });

        var report = await Create(factory).GetWalReclamationAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder, Is.Null);
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [Test]
    public async Task GetWalReclamationAsync_carries_an_unreadable_pin_store_through()
    {
        var factory = Substitute.For<IGrainFactory>();
        Probe(factory, new WalFloorHolderProbeReport { TreeId = Physical, PinStoreReadable = false, PinOffset = -1 });

        var report = await Create(factory).GetWalReclamationAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.PinStoreReadable, Is.False);
            Assert.That(report.IsWedged, Is.False);
        });
    }

    [TestCase(WalGcBlockingPinState.CheckpointedUncovered, TreeWalFloorHolderState.CheckpointedUncovered)]
    [TestCase(WalGcBlockingPinState.NeverCheckpointed, TreeWalFloorHolderState.NeverCheckpointed)]
    [TestCase(WalGcBlockingPinState.NoDurableState, TreeWalFloorHolderState.NoDurableState)]
    [TestCase(WalGcBlockingPinState.Unreadable, TreeWalFloorHolderState.Unreadable)]
    [TestCase(WalGcBlockingPinState.Orphaned, TreeWalFloorHolderState.Orphaned)]
    [TestCase(WalGcBlockingPinState.CheckpointedCoverageUnknown, TreeWalFloorHolderState.CheckpointedCoverageUnknown)]
    public void ToWalReclamationReport_maps_every_core_state(WalGcBlockingPinState core, TreeWalFloorHolderState expected)
    {
        var report = LatticeTreeAdmin.ToWalReclamationReport(Holder(7, core, null), Tree);

        Assert.That(report.FloorHolder!.State, Is.EqualTo(expected));
    }

    [Test]
    public void Every_core_state_has_a_facade_mirror_of_the_same_name_and_value()
    {
        Assert.That(
            Enum.GetValues<TreeWalFloorHolderState>().Select(state => (state.ToString(), (int)state)),
            Is.EquivalentTo(Enum.GetValues<WalGcBlockingPinState>().Select(state => (state.ToString(), (int)state))));
    }

    [Test]
    public void GetWalReclamationAsync_denied_by_read_gate_throws_and_never_probes()
    {
        var factory = Substitute.For<IGrainFactory>();
        var probe = Probe(factory, Holder(42, WalGcBlockingPinState.NeverCheckpointed, -1));

        Assert.That(async () => await Create(factory, allow: false).GetWalReclamationAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        probe.DidNotReceive().ProbeAsync(Arg.Any<CancellationToken>());
    }

    [TestCase(null)]
    [TestCase("")]
    public void GetWalReclamationAsync_rejects_a_missing_tree_id(string? treeId)
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.That(async () => await Create(factory).GetWalReclamationAsync(treeId!),
            Throws.InstanceOf<ArgumentException>());
    }
}
