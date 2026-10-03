using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4270: the alias-cutover shard-map carries rewrite registry rows with
/// <see cref="ILatticeRegistry.UpdateAsync"/>, an unconditional upsert. They used
/// to default a missing row to an empty entry and write it. A revert whose logical
/// row is missing - impossible while the alias resolves to the shadow, so a sign
/// something already went wrong - now refuses and writes nothing. A cutover still
/// creates the rows it legitimately creates: a destination copy addressed before
/// its row is materialised, and the logical row of a restore into a fresh target.
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
    public async Task PrepareCutoverAsync_records_the_replaced_map_on_a_destination_with_no_row_yet()
    {
        // A restore or remediation copy is routinely addressed before its row is
        // materialised; the prepare is what first records it, so it must not refuse.
        StubEntry(Logical, new TreeRegistryEntry { ShardCount = 2 });

        await AliasCutoverShardMaps.PrepareCutoverAsync(_grains, Logical, Destination);

        await _registry.Received(1).UpdateAsync(Destination, Arg.Is<TreeRegistryEntry>(e => e.ReplacedShardMap != null));
        await _registry.DidNotReceive().UpdateAsync(Logical, Arg.Any<TreeRegistryEntry>());
    }

    [Test]
    public async Task SwapCutoverAsync_into_a_never_created_target_swaps_with_the_destination_map()
    {
        // A shadow-cutover restore into a fresh target id: the swap is the genuine
        // create of the logical row, and it carries the destination's map with it.
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Logical));
        StubEntry(Destination, new TreeRegistryEntry { ShardCount = 3, ReplacedShardMap = Map(2), NextShardIndex = 3 });

        await AliasCutoverShardMaps.SwapCutoverAsync(_grains, Logical, Destination);

        await _registry.Received(1).SwapAliasAsync(
            Logical,
            Destination,
            Arg.Is<ShardMap>(m => m.Slots.SequenceEqual(Map(2).Slots)),
            3,
            Logical);
        await _registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task PrepareCutoverAsync_resumed_after_the_swap_with_no_destination_row_writes_nothing()
    {
        StubEntry(Logical, new TreeRegistryEntry { ShardCount = 2, PhysicalTreeId = Destination });

        var replaced = await AliasCutoverShardMaps.PrepareCutoverAsync(_grains, Logical, Destination);

        Assert.That(replaced, Is.Null);
        await _registry.DidNotReceive().UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
    }

    [Test]
    public async Task RevertAsync_refuses_an_unregistered_logical_tree_without_writing()
    {
        _registry.ResolveAsync(Logical).Returns(Task.FromResult(Destination));
        StubEntry(Destination, new TreeRegistryEntry { ShardCount = 3, ReplacedShardMap = Map(2) });

        var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(
            () => AliasCutoverShardMaps.RevertAsync(_grains, Logical, Destination, previousPhysicalTreeId: Logical));

        Assert.That(ex!.TreeId, Is.EqualTo(Logical));
        await _registry.DidNotReceive().UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
        await _registry.DidNotReceive().SwapAliasAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
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
