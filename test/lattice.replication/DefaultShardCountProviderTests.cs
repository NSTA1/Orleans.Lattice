using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="DefaultShardCountProvider"/>: the
/// thin wrapper that exposes the shard-count component of the core
/// <see cref="LatticeOptionsResolver"/> through the
/// <see cref="IShardCountProvider"/> seam.
/// </summary>
[TestFixture]
public class DefaultShardCountProviderTests
{
    private const string UserTree = "user-tree";

    private static (DefaultShardCountProvider Provider, ILatticeRegistry Registry)
        CreateProvider(int shardCount)
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = shardCount,
        };
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(entry));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var resolver = new LatticeOptionsResolver(factory, monitor);
        return (new DefaultShardCountProvider(resolver), registry);
    }

    [Test]
    public void Constructor_throws_when_resolver_is_null()
    {
        Assert.That(
            () => new DefaultShardCountProvider(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetShardCountAsync_throws_when_tree_id_is_null()
    {
        var (provider, _) = CreateProvider(4);
        Assert.That(
            async () => await provider.GetShardCountAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardCountAsync_throws_when_tree_id_is_empty()
    {
        var (provider, _) = CreateProvider(4);
        Assert.That(
            async () => await provider.GetShardCountAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardCountAsync_observes_cancellation_before_dispatch()
    {
        var (provider, _) = CreateProvider(4);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await provider.GetShardCountAsync(UserTree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetShardCountAsync_returns_resolved_shard_count_for_user_tree()
    {
        var (provider, _) = CreateProvider(7);

        var shardCount = await provider.GetShardCountAsync(UserTree);

        Assert.That(shardCount, Is.EqualTo(7));
    }

    [Test]
    public async Task GetShardCountAsync_returns_default_shard_count_for_system_tree()
    {
        // System trees bypass the registry and return canonical
        // defaults from LatticeConstants. Verify the wrapper
        // forwards the resolver''s system-tree behaviour verbatim.
        var (provider, registry) = CreateProvider(7);

        var shardCount = await provider.GetShardCountAsync($"{LatticeConstants.SystemTreePrefix}sys");

        Assert.That(shardCount, Is.EqualTo(LatticeConstants.DefaultShardCount));
        // Registry must not be consulted for system trees.
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    // ==================================================================
    // T-10 - Resolver failure propagation
    // ==================================================================

    [Test]
    public void GetShardCountAsync_propagates_registry_failure_for_user_tree()
    {
        // A transient registry RPC failure (e.g. the underlying
        // grain throws) must bubble out of the wrapper untouched -
        // the seam adds no swallow-and-default behaviour.
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>())
            .ThrowsAsync(new InvalidOperationException("registry unreachable"));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var resolver = new LatticeOptionsResolver(factory, monitor);
        var provider = new DefaultShardCountProvider(resolver);

        Assert.That(
            async () => await provider.GetShardCountAsync(UserTree),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("registry unreachable"));
    }
}