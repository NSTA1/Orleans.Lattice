using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the snapshot-capture trigger and status-read operations on
/// <see cref="LatticeTreeAdmin"/>. The mutating capture wraps the public
/// <see cref="ILattice.SnapshotAsync"/> verb (inheriting its system-tree guard,
/// destination-existence and in-progress validation, and internal-origin marker,
/// and letting the core re-enforce <c>Admin</c>) after authorizing the whole-tree
/// <c>Admin</c> capability fail-closed; the status read authorizes whole-tree
/// <c>Read</c> and projects the tree's observable snapshot signal - the
/// idle/in-flight state from <see cref="ILattice.IsSnapshotCompleteAsync"/>. Driven
/// purely with substitutes and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminSnapshotTests
{
    private const string Tree = "orders";
    private const string Dest = "orders-snap";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()));

    private static ILattice Wire(IGrainFactory factory)
    {
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Tree).Returns(lattice);
        return lattice;
    }

    // ----- SnapshotTree -----

    [Test]
    public async Task SnapshotTreeAsync_wraps_the_lattice_verb_and_projects_the_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.IsSnapshotCompleteAsync().Returns(false);
        var facade = Create(factory);

        var status = await facade.SnapshotTreeAsync(Tree, Dest, TreeSnapshotMode.Online, 128, 64);

        await lattice.Received(1).SnapshotAsync(Dest, SnapshotMode.Online, 128, 64, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.True);
            Assert.That(status.RequestedDestinationTreeId, Is.EqualTo(Dest));
            Assert.That(status.RequestedMode, Is.EqualTo(TreeSnapshotMode.Online));
        });
    }

    [Test]
    public async Task SnapshotTreeAsync_maps_offline_mode_and_null_sizing_to_the_core_verb()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        var facade = Create(factory);

        var status = await facade.SnapshotTreeAsync(Tree, Dest, TreeSnapshotMode.Offline);

        await lattice.Received(1).SnapshotAsync(Dest, SnapshotMode.Offline, null, null, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.RequestedMode, Is.EqualTo(TreeSnapshotMode.Offline));
        });
    }

    [Test]
    public void SnapshotTreeAsync_denied_by_admin_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.SnapshotTreeAsync(Tree, Dest, TreeSnapshotMode.Online),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().SnapshotAsync(
            Arg.Any<string>(), Arg.Any<SnapshotMode>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void SnapshotTreeAsync_reserved_source_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.SnapshotTreeAsync(LatticeConstants.SystemTreePrefix + "trees", Dest, TreeSnapshotMode.Online),
            Throws.ArgumentException);
        lattice.DidNotReceive().SnapshotAsync(
            Arg.Any<string>(), Arg.Any<SnapshotMode>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void SnapshotTreeAsync_reserved_destination_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.SnapshotTreeAsync(Tree, LatticeConstants.SystemTreePrefix + "snap", TreeSnapshotMode.Online),
            Throws.ArgumentException);
        lattice.DidNotReceive().SnapshotAsync(
            Arg.Any<string>(), Arg.Any<SnapshotMode>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void SnapshotTreeAsync_null_or_empty_tree_ids_throw()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.SnapshotTreeAsync(null!, Dest, TreeSnapshotMode.Online), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.SnapshotTreeAsync("", Dest, TreeSnapshotMode.Online), Throws.ArgumentException);
            Assert.That(async () => await facade.SnapshotTreeAsync(Tree, null!, TreeSnapshotMode.Online), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.SnapshotTreeAsync(Tree, "", TreeSnapshotMode.Online), Throws.ArgumentException);
        });
    }

    [Test]
    public void SnapshotTreeAsync_propagates_the_core_precondition_rejection()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.SnapshotAsync(Dest, SnapshotMode.Online, null, null, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("a snapshot with different parameters is already in progress")));
        var facade = Create(factory);

        Assert.That(async () => await facade.SnapshotTreeAsync(Tree, Dest, TreeSnapshotMode.Online),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- GetSnapshotStatus -----

    [Test]
    public async Task GetSnapshotStatusAsync_projects_the_idle_signal_with_null_requested()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        var facade = Create(factory);

        var status = await facade.GetSnapshotStatusAsync(Tree);

        await lattice.DidNotReceive().SnapshotAsync(
            Arg.Any<string>(), Arg.Any<SnapshotMode>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.RequestedDestinationTreeId, Is.Null);
            Assert.That(status.RequestedMode, Is.Null);
        });
    }

    [Test]
    public async Task GetSnapshotStatusAsync_reports_in_flight_while_a_snapshot_runs()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.IsSnapshotCompleteAsync().Returns(false);
        var facade = Create(factory);

        var status = await facade.GetSnapshotStatusAsync(Tree);

        Assert.That(status.InProgress, Is.True);
    }

    [Test]
    public void GetSnapshotStatusAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetSnapshotStatusAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void GetSnapshotStatusAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.GetSnapshotStatusAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.GetSnapshotStatusAsync(""), Throws.ArgumentException);
        });
    }
}
