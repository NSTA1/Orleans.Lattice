using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the online-resize trigger, undo, and status-read operations on
/// <see cref="LatticeTreeAdmin"/>. The mutating verbs wrap the public
/// <see cref="ILattice.ResizeAsync"/> / <see cref="ILattice.UndoResizeAsync"/>
/// verbs (inheriting their capacity-argument guards, system-tree guard, and
/// internal-origin marker, and letting the core re-enforce <c>TreeLifecycle</c>)
/// after authorizing the whole-tree <c>TreeLifecycle</c> capability fail-closed;
/// the status read authorizes whole-tree <c>Read</c> and projects the tree's
/// observable resize signal - the idle/in-flight state (from
/// <see cref="ILattice.IsResizeCompleteAsync"/>) and the current node capacity
/// (from <see cref="ILatticeRegistry"/>). Driven purely with substitutes and a
/// hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminResizeTests
{
    private const string Tree = "orders";

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

    private static (ILattice Lattice, ILatticeRegistry Registry) Wire(IGrainFactory factory)
    {
        var lattice = Substitute.For<ILattice>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILattice>(Tree).Returns(lattice);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (lattice, registry);
    }

    // ----- ResizeTree -----

    [Test]
    public async Task ResizeTreeAsync_wraps_the_lattice_verb_and_projects_the_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(false);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 });
        var facade = Create(factory);

        var status = await facade.ResizeTreeAsync(Tree, newMaxLeafKeys: 256, newMaxInternalChildren: 128);

        await lattice.Received(1).ResizeAsync(256, 128, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.True);
            Assert.That(status.CurrentMaxLeafKeys, Is.EqualTo(64));
            Assert.That(status.CurrentMaxInternalChildren, Is.EqualTo(32));
            Assert.That(status.RequestedMaxLeafKeys, Is.EqualTo(256));
            Assert.That(status.RequestedMaxInternalChildren, Is.EqualTo(128));
        });
    }

    [Test]
    public async Task ResizeTreeAsync_reports_idle_when_the_resize_has_completed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(true);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry { MaxLeafKeys = 256, MaxInternalChildren = 128 });
        var facade = Create(factory);

        var status = await facade.ResizeTreeAsync(Tree, 256, 128);

        Assert.Multiple(() =>
        {
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.CurrentMaxLeafKeys, Is.EqualTo(256));
            Assert.That(status.CurrentMaxInternalChildren, Is.EqualTo(128));
        });
    }

    [Test]
    public void ResizeTreeAsync_denied_by_lifecycle_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ResizeTreeAsync(Tree, 256, 128),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().ResizeAsync(Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ResizeTreeAsync_reserved_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.ResizeTreeAsync(LatticeConstants.SystemTreePrefix + "trees", 256, 128),
            Throws.ArgumentException);
        lattice.DidNotReceive().ResizeAsync(Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ResizeTreeAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.ResizeTreeAsync(null!, 256, 128), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ResizeTreeAsync("", 256, 128), Throws.ArgumentException);
        });
    }

    [Test]
    public void ResizeTreeAsync_propagates_the_core_capacity_argument_rejection()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        lattice.ResizeAsync(1, 128, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new ArgumentOutOfRangeException("newMaxLeafKeys")));
        var facade = Create(factory);

        Assert.That(async () => await facade.ResizeTreeAsync(Tree, 1, 128),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // ----- UndoTreeResize -----

    [Test]
    public async Task UndoTreeResizeAsync_wraps_the_lattice_verb_and_projects_null_requested()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(false);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 });
        var facade = Create(factory);

        var status = await facade.UndoTreeResizeAsync(Tree);

        await lattice.Received(1).UndoResizeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.True);
            Assert.That(status.CurrentMaxLeafKeys, Is.EqualTo(64));
            Assert.That(status.CurrentMaxInternalChildren, Is.EqualTo(32));
            Assert.That(status.RequestedMaxLeafKeys, Is.Null);
            Assert.That(status.RequestedMaxInternalChildren, Is.Null);
        });
    }

    [Test]
    public void UndoTreeResizeAsync_denied_by_lifecycle_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.UndoTreeResizeAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().UndoResizeAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void UndoTreeResizeAsync_reserved_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.UndoTreeResizeAsync(LatticeConstants.SystemTreePrefix + "trees"),
            Throws.ArgumentException);
        lattice.DidNotReceive().UndoResizeAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void UndoTreeResizeAsync_propagates_the_core_precondition_rejection()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        lattice.UndoResizeAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("no completed resize to undo")));
        var facade = Create(factory);

        Assert.That(async () => await facade.UndoTreeResizeAsync(Tree),
            Throws.TypeOf<InvalidOperationException>());
    }

    // ----- GetResizeStatus -----

    [Test]
    public async Task GetResizeStatusAsync_projects_the_idle_signal_and_node_capacity()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(true);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry { MaxLeafKeys = 200, MaxInternalChildren = 100 });
        var facade = Create(factory);

        var status = await facade.GetResizeStatusAsync(Tree);

        await lattice.DidNotReceive().ResizeAsync(Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.CurrentMaxLeafKeys, Is.EqualTo(200));
            Assert.That(status.CurrentMaxInternalChildren, Is.EqualTo(100));
            Assert.That(status.RequestedMaxLeafKeys, Is.Null);
            Assert.That(status.RequestedMaxInternalChildren, Is.Null);
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_reports_seeded_defaults_when_no_entry_exists()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(true);
        registry.GetEntryAsync(Tree).Returns((TreeRegistryEntry?)null);
        var facade = Create(factory);

        var status = await facade.GetResizeStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.CurrentMaxLeafKeys, Is.EqualTo(LatticeConstants.DefaultMaxLeafKeys));
            Assert.That(status.CurrentMaxInternalChildren, Is.EqualTo(LatticeConstants.DefaultMaxInternalChildren));
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_reports_seeded_defaults_when_entry_capacity_is_null()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(true);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry());
        var facade = Create(factory);

        var status = await facade.GetResizeStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.CurrentMaxLeafKeys, Is.EqualTo(LatticeConstants.DefaultMaxLeafKeys));
            Assert.That(status.CurrentMaxInternalChildren, Is.EqualTo(LatticeConstants.DefaultMaxInternalChildren));
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_reports_in_flight_while_a_resize_runs()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsResizeCompleteAsync().Returns(false);
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 });
        var facade = Create(factory);

        var status = await facade.GetResizeStatusAsync(Tree);

        Assert.That(status.InProgress, Is.True);
    }

    [Test]
    public void GetResizeStatusAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetResizeStatusAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void GetResizeStatusAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.GetResizeStatusAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.GetResizeStatusAsync(""), Throws.ArgumentException);
        });
    }
}
