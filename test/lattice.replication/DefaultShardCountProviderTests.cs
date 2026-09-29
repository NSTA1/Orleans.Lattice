using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="DefaultShardCountProvider"/>: the
/// thin wrapper that exposes the shard-count component of the core
/// <see cref="LatticeOptionsResolver"/>, and the shard-root keys of the
/// tree's live routing, through the <see cref="IShardCountProvider"/> seam.
/// </summary>
[TestFixture]
public class DefaultShardCountProviderTests
{
    private const string UserTree = "user-tree";

    private static (DefaultShardCountProvider Provider, ILatticeRegistry Registry, IGrainFactory Factory)
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
        return (new DefaultShardCountProvider(resolver, factory), registry, factory);
    }

    [Test]
    public void Constructor_throws_when_resolver_is_null()
    {
        Assert.That(
            () => new DefaultShardCountProvider(null!, Substitute.For<IGrainFactory>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var resolver = new LatticeOptionsResolver(Substitute.For<IGrainFactory>(), monitor);
        Assert.That(
            () => new DefaultShardCountProvider(resolver, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetShardCountAsync_throws_when_tree_id_is_null()
    {
        var (provider, _, _) = CreateProvider(4);
        Assert.That(
            async () => await provider.GetShardCountAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardCountAsync_throws_when_tree_id_is_empty()
    {
        var (provider, _, _) = CreateProvider(4);
        Assert.That(
            async () => await provider.GetShardCountAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardCountAsync_observes_cancellation_before_dispatch()
    {
        var (provider, _, _) = CreateProvider(4);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await provider.GetShardCountAsync(UserTree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetShardCountAsync_returns_resolved_shard_count_for_user_tree()
    {
        var (provider, _, _) = CreateProvider(7);

        var shardCount = await provider.GetShardCountAsync(UserTree);

        Assert.That(shardCount, Is.EqualTo(7));
    }

    [Test]
    public async Task GetShardCountAsync_returns_default_shard_count_for_system_tree()
    {
        // System trees bypass the registry and return canonical
        // defaults from LatticeConstants. Verify the wrapper
        // forwards the resolver''s system-tree behaviour verbatim.
        var (provider, registry, _) = CreateProvider(7);

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
        var provider = new DefaultShardCountProvider(resolver, factory);

        Assert.That(
            async () => await provider.GetShardCountAsync(UserTree),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("registry unreachable"));
    }
    // ==================================================================
    // GetShardRootKeysAsync - the shards the live routing reaches
    // ==================================================================

    private static ILattice StubRouting(IGrainFactory factory, RoutingInfo routing)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(routing));
        factory.GetGrain<ILattice>(UserTree).Returns(lattice);
        return lattice;
    }

    [Test]
    public void GetShardRootKeysAsync_throws_when_tree_id_is_null()
    {
        var (provider, _, _) = CreateProvider(4);
        Assert.That(
            async () => await provider.GetShardRootKeysAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardRootKeysAsync_throws_when_tree_id_is_empty()
    {
        var (provider, _, _) = CreateProvider(4);
        Assert.That(
            async () => await provider.GetShardRootKeysAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardRootKeysAsync_observes_cancellation_before_dispatch()
    {
        var (provider, _, factory) = CreateProvider(4);
        var lattice = StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(8, 2)));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await provider.GetShardRootKeysAsync(UserTree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        _ = lattice.DidNotReceive().GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetShardRootKeysAsync_returns_every_physical_shard_of_an_unsplit_tree()
    {
        var (provider, _, factory) = CreateProvider(3);
        StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(12, 3)));

        var keys = await provider.GetShardRootKeysAsync(UserTree);

        Assert.That(keys, Is.EqualTo(new[] { $"{UserTree}/0", $"{UserTree}/1", $"{UserTree}/2" }));
    }

    [Test]
    public async Task GetShardRootKeysAsync_includes_a_split_target_above_the_pinned_shard_count()
    {
        // Regression: an adaptive split moves slots to a physical index above
        // the pinned ShardCount (2 here) without changing the pin. Walking
        // {tree}/0..ShardCount-1 skipped shard 5, which serves those slots.
        var (provider, _, factory) = CreateProvider(2);
        StubRouting(factory, new RoutingInfo(UserTree, new ShardMap { Slots = [0, 1, 0, 5, 0, 1, 5, 1], Version = 3 }));

        var pinned = await provider.GetShardCountAsync(UserTree);
        var keys = await provider.GetShardRootKeysAsync(UserTree);

        Assert.That(pinned, Is.EqualTo(2));
        Assert.That(keys, Is.EqualTo(new[] { $"{UserTree}/0", $"{UserTree}/1", $"{UserTree}/5" }));
    }

    [Test]
    public async Task GetShardRootKeysAsync_addresses_the_aliased_physical_tree_not_the_logical_id()
    {
        // Regression: after a resize or a shadow-cutover restore the logical id
        // is an alias, so {logicalId}/{i} names the retired physical copy.
        const string physical = "user-tree-restore-shadow-7";
        var (provider, _, factory) = CreateProvider(2);
        StubRouting(factory, new RoutingInfo(physical, ShardMap.CreateDefault(8, 2)));

        var keys = await provider.GetShardRootKeysAsync(UserTree);

        Assert.That(keys, Is.EqualTo(new[] { $"{physical}/0", $"{physical}/1" }));
    }

    [Test]
    public async Task GetShardRootKeysAsync_forces_a_routing_refresh()
    {
        // LatticeGrain caches routing per stateless-worker activation; a stale
        // cache would hide a split or alias swap that has already landed.
        var (provider, _, factory) = CreateProvider(2);
        var lattice = StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(8, 2)));

        await provider.GetShardRootKeysAsync(UserTree);

        _ = lattice.Received(1).GetRoutingAsync(true, Arg.Any<CancellationToken>());
        _ = lattice.DidNotReceive().GetRoutingAsync(false, Arg.Any<CancellationToken>());
    }
}
