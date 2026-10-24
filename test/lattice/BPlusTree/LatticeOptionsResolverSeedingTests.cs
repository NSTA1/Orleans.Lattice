using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LatticeOptionsResolverSeedingTests
{
    private const string TreeId = "seed-tree";

    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry, ITreeDeletionGrain Deletion) Create(
        TreeRegistryEntry? entry = null)
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult(entry));
        var deletion = Substitute.For<ITreeDeletionGrain>();
        factory.GetGrain<ITreeDeletionGrain>(TreeId).Returns(deletion);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(TreeId).Returns(new LatticeOptions());
        return (new LatticeOptionsResolver(factory, monitor), registry, deletion);
    }

    [Test]
    public async Task ResolveAsync_missing_row_uses_registration_result_without_readback()
    {
        var (resolver, registry, deletion) = Create();
        var seeded = new TreeRegistryEntry
        {
            MaxLeafKeys = 256, MaxInternalChildren = 64, ShardCount = 4, WalPartitions = 8,
        };
        registry.RegisterAndGetEntryAsync(TreeId, null).Returns((true, seeded));

        var resolved = await resolver.ResolveAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.MaxLeafKeys, Is.EqualTo(256));
            Assert.That(resolved.MaxInternalChildren, Is.EqualTo(64));
            Assert.That(resolved.ShardCount, Is.EqualTo(4));
            Assert.That(resolved.WalPartitions, Is.EqualTo(8));
        });
        await deletion.Received(1).HoldsCompletedPurgeAsync();
        await registry.Received(1).GetEntryAsync(TreeId);
        await registry.Received(1).RegisterAndGetEntryAsync(TreeId, null);
        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!, default);
    }

    [Test]
    public async Task ResolveAsync_incomplete_row_uses_winning_registration_without_repinning()
    {
        var incomplete = new TreeRegistryEntry { MaxLeafKeys = 256 };
        var winner = incomplete with { ShardCount = 4 };
        var (resolver, registry, deletion) = Create(incomplete);
        registry.RegisterAndGetEntryAsync(TreeId, incomplete).Returns((false, winner));

        var resolved = await resolver.ResolveAsync(TreeId);

        Assert.That(resolved.MaxLeafKeys, Is.EqualTo(256));
        Assert.That(resolved.ShardCount, Is.EqualTo(4));
        Assert.That(resolved.MaxInternalChildren, Is.EqualTo(LatticeConstants.DefaultMaxInternalChildren));
        await registry.Received(1).GetEntryAsync(TreeId);
        await registry.Received(1).RegisterAndGetEntryAsync(TreeId, incomplete);
        await deletion.DidNotReceive().HoldsCompletedPurgeAsync();
    }

    [Test]
    public void ResolveAsync_purged_tree_is_not_registered()
    {
        var (resolver, registry, deletion) = Create();
        deletion.HoldsCompletedPurgeAsync().Returns(true);

        Assert.ThrowsAsync<InvalidOperationException>(() => resolver.ResolveAsync(TreeId));

        registry.DidNotReceiveWithAnyArgs().RegisterAndGetEntryAsync(default!, default);
    }

    [Test]
    public void ResolveAsync_registration_fault_propagates_without_readback()
    {
        var (resolver, registry, _) = Create(new TreeRegistryEntry());
        registry.RegisterAndGetEntryAsync(TreeId, Arg.Any<TreeRegistryEntry?>())
            .Returns(Task.FromException<(bool, TreeRegistryEntry?)>(new InvalidOperationException("unavailable")));

        Assert.ThrowsAsync<InvalidOperationException>(() => resolver.ResolveAsync(TreeId));

        registry.Received(1).GetEntryAsync(TreeId);
    }
}
