using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the online-reshard trigger and status-read operations on
/// <see cref="LatticeTreeAdmin"/>. The trigger wraps the public
/// <see cref="ILattice.ReshardAsync"/> verb (inheriting its grow-only guards,
/// system-tree guard, and internal-origin marker, and letting the core re-enforce
/// <c>TreeLifecycle</c>) after authorizing the whole-tree <c>TreeLifecycle</c>
/// capability fail-closed; the status read authorizes whole-tree <c>Read</c> and
/// projects the tree's observable reshard signal - the idle/in-flight state (from
/// <see cref="ILattice.IsReshardCompleteAsync"/>) and the current
/// <see cref="ShardMap"/> fan-out (from <see cref="ILatticeRegistry"/>). Driven
/// purely with substitutes and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminReshardTests
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

    // ----- ReshardTree -----

    [Test]
    public async Task ReshardTreeAsync_wraps_the_lattice_verb_and_projects_the_status()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsReshardCompleteAsync().Returns(false);
        // A four-slot map routing to two distinct physical shards.
        registry.GetShardMapAsync(Tree).Returns(new ShardMap { Slots = new[] { 1, 0, 1, 0 }, Version = 7 });
        var facade = Create(factory);

        var status = await facade.ReshardTreeAsync(Tree, targetShardCount: 4);

        await lattice.Received(1).ReshardAsync(4, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.True);
            Assert.That(status.CurrentPhysicalShardCount, Is.EqualTo(2));
            Assert.That(status.VirtualShardCount, Is.EqualTo(4));
            Assert.That(status.MapVersion, Is.EqualTo(7));
            Assert.That(status.RequestedShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task ReshardTreeAsync_reports_idle_when_the_reshard_has_completed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsReshardCompleteAsync().Returns(true);
        registry.GetShardMapAsync(Tree).Returns(new ShardMap { Slots = new[] { 0, 1, 2, 3 }, Version = 9 });
        var facade = Create(factory);

        var status = await facade.ReshardTreeAsync(Tree, targetShardCount: 4);

        Assert.Multiple(() =>
        {
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.CurrentPhysicalShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void ReshardTreeAsync_denied_by_lifecycle_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.ReshardTreeAsync(Tree, 4),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().ReshardAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReshardTreeAsync_reserved_tree_id_is_rejected_before_any_dial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.ReshardTreeAsync(LatticeConstants.SystemTreePrefix + "trees", 4),
            Throws.ArgumentException);
        lattice.DidNotReceive().ReshardAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReshardTreeAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.ReshardTreeAsync(null!, 4), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ReshardTreeAsync("", 4), Throws.ArgumentException);
        });
    }

    [Test]
    public void ReshardTreeAsync_propagates_the_core_grow_only_argument_rejection()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, _) = Wire(factory);
        lattice.ReshardAsync(1, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new ArgumentOutOfRangeException("newShardCount")));
        var facade = Create(factory);

        Assert.That(async () => await facade.ReshardTreeAsync(Tree, 1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // ----- GetReshardStatus -----

    [Test]
    public async Task GetReshardStatusAsync_projects_the_idle_signal_and_map_fan_out()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsReshardCompleteAsync().Returns(true);
        registry.GetShardMapAsync(Tree).Returns(new ShardMap { Slots = new[] { 1, 0, 1, 0 }, Version = 3 });
        var facade = Create(factory);

        var status = await facade.GetReshardStatusAsync(Tree);

        await lattice.DidNotReceive().ReshardAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(status.TreeId, Is.EqualTo(Tree));
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.CurrentPhysicalShardCount, Is.EqualTo(2));
            Assert.That(status.VirtualShardCount, Is.EqualTo(4));
            Assert.That(status.MapVersion, Is.EqualTo(3));
            Assert.That(status.RequestedShardCount, Is.Null);
        });
    }

    [Test]
    public async Task GetReshardStatusAsync_reports_zeroed_counts_when_no_custom_map_exists()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsReshardCompleteAsync().Returns(true);
        registry.GetShardMapAsync(Tree).Returns((ShardMap?)null);
        var facade = Create(factory);

        var status = await facade.GetReshardStatusAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(status.InProgress, Is.False);
            Assert.That(status.CurrentPhysicalShardCount, Is.EqualTo(0));
            Assert.That(status.VirtualShardCount, Is.EqualTo(0));
            Assert.That(status.MapVersion, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task GetReshardStatusAsync_reports_in_flight_while_a_reshard_runs()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (lattice, registry) = Wire(factory);
        lattice.IsReshardCompleteAsync().Returns(false);
        registry.GetShardMapAsync(Tree).Returns(new ShardMap { Slots = new[] { 0, 1 }, Version = 1 });
        var facade = Create(factory);

        var status = await facade.GetReshardStatusAsync(Tree);

        Assert.That(status.InProgress, Is.True);
    }

    [Test]
    public void GetReshardStatusAsync_denied_by_read_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.GetReshardStatusAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void GetReshardStatusAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.GetReshardStatusAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.GetReshardStatusAsync(""), Throws.ArgumentException);
        });
    }
}
