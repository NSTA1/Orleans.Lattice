using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression tests for #5300: an OR-Map shape is registered under the logical tree
/// name, but a derived physical copy - an online resize's copy, a bootstrap hold's
/// held copy (#4791), a shadow-cutover restore's copy or a schema remediation's
/// destination - names its leaves with its own physical id
/// (<c>{tree}/resized/{op}</c> and so on). The leaf resolved the shape by that
/// physical id, so every CRDT fold, migration join and mirror join on a copy of an
/// OR-Map tree threw <see cref="LatticeCrdtShapeNotRegisteredException"/>.
/// <para>
/// The copy's registry entry records the logical tree it backs
/// (<c>DerivedFrom</c>), and the leaf already resolves that at activation for its
/// metrics. Shape resolution now goes through it, so every physical copy of a shaped
/// tree folds with the tree's registered shape.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtShapeDerivedCopyIntegrationTests
{
    private const string ResizedTree = "csd-resized";
    private const string HoldTree = "csd-hold";
    private const string HoldRoutedTree = "csd-hold-routed";
    private const string Key = "row";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Or_map_write_after_an_online_resize_folds_with_the_logical_tree_shape()
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(ResizedTree);
        await Map(lattice).SetAsync("a", "r1", Counter("r1", 1));

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(ResizedTree);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        Assert.That(await PhysicalTreeAsync(ResizedTree), Is.Not.EqualTo(ResizedTree),
            "precondition: the tree now resolves to the resize's derived copy");

        await Map(lattice).SetAsync("b", "r1", Counter("r1", 2));

        var map = await Map(lattice).GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(map.ContainsKey("a"), Is.True, "the row copied by the resize survives");
            Assert.That(map.ContainsKey("b"), Is.True, "an OR-Map write on the resized copy folds with the tree's shape");
        });
    }

    [Test]
    public async Task Or_map_rows_under_a_bootstrap_hold_are_copied_and_mirrored_with_the_logical_tree_shape()
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(HoldTree);
        await Map(lattice).SetAsync("a", "r1", Counter("r1", 1));
        var (resize, operationId, held) = await BeginHoldAsync(HoldTree);

        // The held copy gains a contribution of its own, as the bootstrap import
        // applies to it, on the row the copy pass brought over.
        using (LatticeBootstrapShadowRouteContext.BeginScope(HoldTree, held))
        {
            await Map(lattice).SetAsync("c", "r3", Counter("r3", 4));
        }

        // A write to the original during the hold is mirrored onto the held copy,
        // which must join it into that row (#4618) rather than overwrite it.
        await Map(lattice).SetAsync("b", "r1", Counter("r1", 2));

        await resize.CompleteBootstrapCopyAsync(operationId);
        var map = await Map(lattice).GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(map.ContainsKey("a"), Is.True, "the held copy holds the row the copy pass brought over");
            Assert.That(map.ContainsKey("b"), Is.True, "the held copy joined the mirrored write with the tree's shape");
            Assert.That(map.ContainsKey("c"), Is.True, "the join kept the held copy's own contribution");
        });
    }

    [Test]
    public async Task Or_map_write_routed_to_a_held_copy_folds_with_the_logical_tree_shape()
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(HoldRoutedTree);
        await Map(lattice).SetAsync("a", "r1", Counter("r1", 1));
        var (resize, operationId, held) = await BeginHoldAsync(HoldRoutedTree);

        // An apply the bootstrap routes to the held copy, as its import does.
        using (LatticeBootstrapShadowRouteContext.BeginScope(HoldRoutedTree, held))
        {
            await Map(lattice).SetAsync("b", "r2", Counter("r2", 3));
        }

        await resize.CompleteBootstrapCopyAsync(operationId);
        var map = await Map(lattice).GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(map.ContainsKey("a"), Is.True);
            Assert.That(map.ContainsKey("b"), Is.True, "an OR-Map apply on the held copy folds with the tree's shape");
        });
    }

    private async Task<(ITreeResizeGrain Resize, string OperationId, string Held)> BeginHoldAsync(string tree)
    {
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(tree);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        var held = await resize.GetBootstrapCopyTreeIdAsync();
        Assert.That(held, Is.Not.Null.And.Not.EqualTo(tree));
        return (resize, operationId, held!);
    }

    private async Task<string> PhysicalTreeAsync(string tree) =>
        (await _cluster.GrainFactory.GetLatticeRegistry().GetEntryAsync(tree))?.PhysicalTreeId ?? tree;

    private static OrMapAccessor<string, PnCounter> Map(ILattice lattice) => lattice.OrMap<string, PnCounter>(Key);

    private static PnCounter Counter(string replica, long amount)
    {
        var counter = new PnCounter();
        counter.Increment(replica, amount);
        return counter;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOrMapShape<string, PnCounter>(ResizedTree);
            siloBuilder.AddOrMapShape<string, PnCounter>(HoldTree);
            siloBuilder.AddOrMapShape<string, PnCounter>(HoldRoutedTree);
        }
    }
}
