using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class RegistryEntryOptionsTests
{
    private const string TreeId = "tree";

    private static (ILatticeRegistry Registry, LatticeOptionsResolver Resolver, IGrainFactory Factory) CreateHarness()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (registry, TestOptionsResolver.ForFactory(factory), factory);
    }

    [Test]
    public async Task ReadAsync_complete_pins_reads_once_and_keeps_runtime_overrides()
    {
        var (registry, resolver, _) = CreateHarness();
        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = 256, MaxInternalChildren = 64, ShardCount = 3,
            WalPartitions = 4, MaxCacheValueBytes = 27, WalMaxRetainedBytes = 1024,
            PhysicalTreeId = "copy",
        };
        registry.GetEntryAsync(TreeId).Returns(entry);

        var result = await RegistryEntryOptions.ReadAsync(registry, resolver, TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entry, Is.SameAs(entry));
            Assert.That(result.Options.MaxLeafKeys, Is.EqualTo(256));
            Assert.That(result.Options.ShardCount, Is.EqualTo(3));
            Assert.That(result.Options.WalPartitions, Is.EqualTo(4));
            Assert.That(result.Options.MaxCacheValueBytes, Is.EqualTo(27));
            Assert.That(result.Options.WalMaxRetainedBytes, Is.EqualTo(1024));
        });
        await registry.Received(1).GetEntryAsync(TreeId);
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        await registry.DidNotReceiveWithAnyArgs().RegisterAndGetEntryAsync(default!);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ReadAsync_incomplete_pins_preserves_seeding_and_reads_topology_after_it(bool missing)
    {
        var (registry, resolver, _) = CreateHarness();
        var initial = missing ? null : new TreeRegistryEntry { ShardCount = 2 };
        var registered = false;
        var fresh = new TreeRegistryEntry
        {
            MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 3,
            PhysicalTreeId = "fresh-copy", ShardMap = new ShardMap { Slots = [0, 4], Version = 9 },
        };
        registry.GetEntryAsync(TreeId).Returns(_ => registered ? fresh : initial);
        registry.RegisterAndGetEntryAsync(TreeId, Arg.Any<TreeRegistryEntry?>()).Returns(_ =>
        {
            registered = true;
            return Task.FromResult<(bool, TreeRegistryEntry?)>(
                (missing, fresh with { PhysicalTreeId = "seeding-copy" }));
        });

        var result = await RegistryEntryOptions.ReadAsync(registry, resolver, TreeId);

        Assert.That(result.Entry, Is.SameAs(fresh));
        Assert.That(result.Options.ShardCount, Is.EqualTo(3));
        await registry.Received(1).RegisterAndGetEntryAsync(TreeId, initial);
        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!);
        await registry.Received(3).GetEntryAsync(TreeId);
    }

    [Test]
    public async Task ReadAsync_system_tree_keeps_canonical_options_without_registration()
    {
        var (registry, resolver, _) = CreateHarness();
        var systemId = LatticeConstants.SystemTreePrefix + "test";
        registry.GetEntryAsync(systemId).Returns(new TreeRegistryEntry { ShardCount = 9 });

        var result = await RegistryEntryOptions.ReadAsync(registry, resolver, systemId);

        Assert.That(result.Options.ShardCount, Is.EqualTo(LatticeConstants.DefaultShardCount));
        await registry.Received(1).GetEntryAsync(systemId);
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        await registry.DidNotReceiveWithAnyArgs().RegisterAndGetEntryAsync(default!);
    }

    [Test]
    public void ReadAsync_missing_purged_tree_refuses_registration()
    {
        var (registry, resolver, factory) = CreateHarness();
        factory.GetGrain<ITreeDeletionGrain>(TreeId).HoldsCompletedPurgeAsync().Returns(true);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => RegistryEntryOptions.ReadAsync(registry, resolver, TreeId));

        Assert.That(registry.ReceivedCalls().Any(c =>
            c.GetMethodInfo().Name == nameof(ILatticeRegistry.RegisterAsync)), Is.False);
        registry.DidNotReceiveWithAnyArgs().RegisterAndGetEntryAsync(default!);
    }

    [Test]
    public async Task ReadAsync_sequential_calls_observe_changed_pins_and_topology()
    {
        var (registry, resolver, _) = CreateHarness();
        var first = new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 2 };
        var second = first with { ShardCount = 4, PhysicalTreeId = "new-copy", NextShardIndex = 7 };
        registry.GetEntryAsync(TreeId).Returns(first, second);

        var before = await RegistryEntryOptions.ReadAsync(registry, resolver, TreeId);
        var after = await RegistryEntryOptions.ReadAsync(registry, resolver, TreeId);

        Assert.That(before.Options.ShardCount, Is.EqualTo(2));
        Assert.That(after.Options.ShardCount, Is.EqualTo(4));
        Assert.That(after.Entry, Is.SameAs(second));
        await registry.Received(2).GetEntryAsync(TreeId);
    }
}
