using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class RegistryEntryShardMapTests
{
    private const string TreeId = "tree";

    private static (IGrainFactory Factory, ILatticeRegistry Registry, LatticeOptionsResolver Resolver) CreateHarness()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 3 }));
        return (factory, registry, TestOptionsResolver.ForFactory(factory));
    }

    private static int RegistryCalls(ILatticeRegistry registry) => registry.ReceivedCalls().Count();

    [Test]
    public async Task ResolveAsync_returns_the_persisted_map_without_any_registry_call()
    {
        var (_, registry, resolver) = CreateHarness();
        var persisted = new ShardMap { Slots = [0, 0, 5, 5], Version = 4 };
        var entry = new TreeRegistryEntry { ShardCount = 2, ShardMap = persisted };

        var map = await RegistryEntryShardMap.ResolveAsync(registry, resolver, TreeId, entry);

        Assert.That(map, Is.SameAs(persisted));
        Assert.That(RegistryCalls(registry), Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_builds_the_default_map_from_a_pinned_shard_count_without_any_registry_call()
    {
        var (_, registry, resolver) = CreateHarness();
        var entry = new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 };

        var map = await RegistryEntryShardMap.ResolveAsync(registry, resolver, TreeId, entry);

        Assert.That(map.GetPhysicalShardIndices(), Is.EqualTo(new[] { 0, 1, 2, 3 }));
        Assert.That(RegistryCalls(registry), Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_falls_back_to_the_resolved_shard_count_for_an_unpinned_entry()
    {
        var (_, registry, resolver) = CreateHarness();
        var entry = new TreeRegistryEntry();

        var map = await RegistryEntryShardMap.ResolveAsync(registry, resolver, TreeId, entry);

        Assert.That(map.GetPhysicalShardIndices(), Is.EqualTo(new[] { 0, 1, 2 }));
        await registry.Received(1).GetEntryAsync(TreeId);
        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
    }

    [Test]
    public async Task ResolveAsync_re_reads_the_map_after_seeding_an_absent_entry()
    {
        var (_, registry, resolver) = CreateHarness();
        var seeded = new ShardMap { Slots = [0, 1, 1, 1], Version = 1 };
        registry.GetShardMapAsync(TreeId).Returns(Task.FromResult<ShardMap?>(seeded));

        var map = await RegistryEntryShardMap.ResolveAsync(registry, resolver, TreeId, entry: null);

        Assert.That(map, Is.SameAs(seeded));
        await registry.Received(1).GetShardMapAsync(TreeId);
    }

    [Test]
    public async Task ResolveAsync_uses_the_resolved_shard_count_for_a_system_tree_entry()
    {
        var (_, registry, resolver) = CreateHarness();
        var systemTree = LatticeConstants.SystemTreePrefix + "x";
        var entry = new TreeRegistryEntry { ShardCount = 7 };

        var map = await RegistryEntryShardMap.ResolveAsync(registry, resolver, systemTree, entry);

        Assert.That(map.GetPhysicalShardIndices(), Has.Count.EqualTo(LatticeConstants.DefaultShardCount));
        Assert.That(RegistryCalls(registry), Is.Zero);
    }
}
