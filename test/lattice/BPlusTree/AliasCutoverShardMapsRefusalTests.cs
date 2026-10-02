using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4270: the alias-cutover shard-map carries rewrite registry rows with
/// <see cref="ILatticeRegistry.UpdateAsync"/>, an unconditional upsert. They used
/// to default a missing row to an empty entry and write it, creating a row with
/// no structural pins. Each now refuses a missing row and writes nothing.
/// </summary>
[TestFixture]
public sealed class AliasCutoverShardMapsRefusalTests
{
    private const string Logical = "refusal-logical";
    private const string Destination = "refusal-destination";

    private IGrainFactory _grains = null!;
    private ILatticeRegistry _registry = null!;

    [SetUp]
    public void SetUp()
    {
        _grains = Substitute.For<IGrainFactory>();
        _registry = Substitute.For<ILatticeRegistry>();
        _grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(_registry);
        _registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        StubRouting(Logical);
        StubRouting(Destination);
    }

    [Test]
    public async Task PrepareCutoverAsync_refuses_an_unregistered_destination_without_writing()
    {
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Logical));
        StubEntry(Logical, new TreeRegistryEntry { ShardCount = 2 });

        var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(
            () => AliasCutoverShardMaps.PrepareCutoverAsync(_grains, Logical, Destination));

        Assert.That(ex!.TreeId, Is.EqualTo(Destination));
        await _registry.DidNotReceive().UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }

    [Test]
    public async Task PrepareCutoverAsync_refuses_an_unregistered_logical_tree_without_writing_it()
    {
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Logical));
        StubEntry(Destination, new TreeRegistryEntry { ShardCount = 3, ReplacedShardMap = Map(2) });

        var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(
            () => AliasCutoverShardMaps.PrepareCutoverAsync(_grains, Logical, Destination));

        Assert.That(ex!.TreeId, Is.EqualTo(Logical));
        await _registry.DidNotReceive().UpdateAsync(Logical, Arg.Any<TreeRegistryEntry>());
    }

    [Test]
    public async Task PrepareCutoverAsync_resumed_after_the_swap_with_no_destination_row_writes_nothing()
    {
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Destination));

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(_grains, Logical, Destination);

        Assert.That(replaced, Is.Null);
        await _registry.DidNotReceive().UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }

    [Test]
    public async Task PrepareRevertAsync_refuses_an_unregistered_logical_tree_without_writing()
    {
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Destination));
        StubEntry(Destination, new TreeRegistryEntry { ShardCount = 3, ReplacedShardMap = Map(2) });

        var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(
            () => AliasCutoverShardMaps.PrepareRevertAsync(_grains, Logical, Destination, previousPhysicalTreeId: Logical));

        Assert.That(ex!.TreeId, Is.EqualTo(Logical));
        await _registry.DidNotReceive().UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }

    private void StubEntry(string treeId, TreeRegistryEntry entry) =>
        _registry.GetEntryAsync(treeId).Returns(Task.FromResult<TreeRegistryEntry?>(entry));

    private void StubRouting(string treeId)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo(treeId, Map(2))));
        _grains.GetGrain<ILattice>(treeId, null).Returns(lattice);
    }

    private static ShardMap Map(int shards) =>
        ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, shards);
}
