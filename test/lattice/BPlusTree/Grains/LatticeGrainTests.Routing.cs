using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- GetRoutingAsync tests ---

    [Test]
    public async Task GetRoutingAsync_returns_default_map_when_registry_returns_null()
    {
        const string treeId = "routing-default";
        var options = new LatticeOptions { ShardCount = 4, VirtualShardCount = 16 };
        var (grain, factory, _) = CreateGrainWithRegistry(treeId, options);
        SetupShardRoot(factory);

        var routing = await grain.GetRoutingAsync();

        Assert.That(routing, Is.Not.Null);
        Assert.That(routing.PhysicalTreeId, Is.EqualTo(treeId));
        Assert.That(routing.Map, Is.Not.Null);
        Assert.That(routing.Map.Slots.Length, Is.EqualTo(16));
        for (int i = 0; i < 16; i++)
            Assert.That(routing.Map.Slots[i], Is.EqualTo(i % 4));
    }

    [Test]
    public async Task GetRoutingAsync_returns_custom_map_from_registry()
    {
        const string treeId = "routing-custom";
        var options = new LatticeOptions { ShardCount = 4, VirtualShardCount = 8 };
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId, options);
        var customMap = new ShardMap { Slots = [0, 1, 2, 3, 0, 1, 2, 3] };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(customMap));
        SetupShardRoot(factory);

        var routing = await grain.GetRoutingAsync();

        Assert.That(routing.Map, Is.SameAs(customMap));
        Assert.That(routing.PhysicalTreeId, Is.EqualTo(treeId));
    }

    [Test]
    public async Task GetRoutingAsync_resolves_alias_to_physical_tree_id()
    {
        const string aliasId = "alias-tree";
        const string physicalId = "physical-tree";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);
        registry.ResolveAsync(aliasId).Returns(Task.FromResult(physicalId));
        SetupShardRoot(factory);

        var routing = await grain.GetRoutingAsync();

        Assert.That(routing.PhysicalTreeId, Is.EqualTo(physicalId));
    }

    [Test]
    public async Task GetRoutingAsync_supports_tuple_deconstruction()
    {
        const string treeId = "routing-deconstruct";
        var (grain, factory, _) = CreateGrainWithRegistry(treeId);
        SetupShardRoot(factory);

        var (physicalId, map) = await grain.GetRoutingAsync();

        Assert.That(physicalId, Is.EqualTo(treeId));
        Assert.That(map, Is.Not.Null);
    }
}

