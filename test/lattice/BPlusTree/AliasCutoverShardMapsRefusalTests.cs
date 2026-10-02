using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4270: the alias-cutover shard-map carries rewrite registry rows with
/// <see cref="ILatticeRegistry.UpdateAsync"/>, an unconditional upsert. They used
/// to default a missing row to an empty entry and write it, creating a row with
/// no structural pins. A missing destination or revert row now refuses and writes
/// nothing; a missing logical row on a cutover is the genuine create of a
/// restore into a fresh target and still proceeds.
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
    public async Task PrepareCutoverAsync_into_a_never_created_target_creates_its_row_with_the_destination_map()
    {
        // A shadow-cutover restore into a fresh target id: the cutover is the
        // genuine create of the logical row, so it must not be refused.
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Logical));
        StubEntry(Destination, new TreeRegistryEntry { ShardCount = 3, ReplacedShardMap = Map(2), NextShardIndex = 3 });

        await AliasCutoverShardMaps.PrepareCutoverAsync(_grains, Logical, Destination);

        await _registry.Received(1).UpdateAsync(Logical, Arg.Is<TreeRegistryEntry>(e =>
            e.ShardMap != null && e.ShardMap.Slots.SequenceEqual(Map(2).Slots) && e.NextShardIndex == 3));
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
