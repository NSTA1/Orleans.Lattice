using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// <see cref="TreeBootstrapReadFence.ResolveAsync"/> derives the routed copy and
/// its shard map from a single registry entry read rather than paying a separate
/// <see cref="ILatticeRegistry.ResolveAsync"/> round trip for the alias.
/// </summary>
[TestFixture]
public sealed class TreeBootstrapReadFenceResolveTests
{
    private const string Logical = "fence-logical";
    private const string Physical = "fence-physical";

    [Test]
    public async Task ResolveAsync_reads_the_logical_entry_once_and_follows_its_alias()
    {
        var grains = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Logical).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            PhysicalTreeId = Physical,
            ShardCount = 3,
            ShardMap = ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, 3),
        }));
        registry.GetEntryAsync(Physical).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry { ShardCount = 3 }));

        var shards = await TreeBootstrapReadFence.ResolveAsync(grains, TestOptionsResolver.ForFactory(grains), Logical);

        Assert.Multiple(() =>
        {
            Assert.That(shards.PhysicalTreeId, Is.EqualTo(Physical));
            Assert.That(shards.ShardIndices, Is.EqualTo(new[] { 0, 1, 2 }));
        });
        await registry.Received(1).GetEntryAsync(Logical);
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }

    [Test]
    public async Task ResolveAsync_routes_an_unregistered_tree_to_itself()
    {
        var grains = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));

        var shards = await TreeBootstrapReadFence.ResolveAsync(grains, TestOptionsResolver.ForFactory(grains), Logical);

        Assert.That(shards.PhysicalTreeId, Is.EqualTo(Logical));
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }
}
