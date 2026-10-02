using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="DefaultShardCountProvider"/>: the thin wrapper that
/// exposes the physical shard indices and shard-root keys of a tree's live routing
/// through the <see cref="IShardCountProvider"/> seam.
/// </summary>
[TestFixture]
public class DefaultShardCountProviderTests
{
    private const string UserTree = "user-tree";

    private static (DefaultShardCountProvider Provider, IGrainFactory Factory) CreateProvider()
    {
        var factory = Substitute.For<IGrainFactory>();
        return (new DefaultShardCountProvider(factory), factory);
    }

    private static ILattice StubRouting(IGrainFactory factory, RoutingInfo routing)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(routing));
        factory.GetGrain<ILattice>(UserTree).Returns(lattice);
        return lattice;
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new DefaultShardCountProvider(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ==================================================================
    // GetShardIndicesAsync - the physical shards the live routing reaches
    // ==================================================================

    [Test]
    public void GetShardIndicesAsync_throws_when_tree_id_is_null()
    {
        var (provider, _) = CreateProvider();
        Assert.That(
            async () => await provider.GetShardIndicesAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardIndicesAsync_throws_when_tree_id_is_empty()
    {
        var (provider, _) = CreateProvider();
        Assert.That(
            async () => await provider.GetShardIndicesAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardIndicesAsync_observes_cancellation_before_dispatch()
    {
        var (provider, factory) = CreateProvider();
        var lattice = StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(8, 2)));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await provider.GetShardIndicesAsync(UserTree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        _ = lattice.DidNotReceive().GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetShardIndicesAsync_returns_every_physical_shard_of_an_unsplit_tree()
    {
        var (provider, factory) = CreateProvider();
        StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(12, 3)));

        var indices = await provider.GetShardIndicesAsync(UserTree);

        Assert.That(indices, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public async Task GetShardIndicesAsync_includes_a_split_target_above_the_pinned_shard_count()
    {
        // Issue #4206 (the #3753 class): an adaptive split or a reshard moves slots
        // to a physical index above the pinned ShardCount without changing it, so
        // 0..ShardCount-1 skipped shard 5, which serves those slots.
        var (provider, factory) = CreateProvider();
        StubRouting(factory, new RoutingInfo(UserTree, new ShardMap { Slots = [0, 1, 0, 5, 0, 1, 5, 1], Version = 3 }));

        var indices = await provider.GetShardIndicesAsync(UserTree);

        Assert.That(indices, Is.EqualTo(new[] { 0, 1, 5 }));
    }

    [Test]
    public async Task GetShardIndicesAsync_forces_a_routing_refresh()
    {
        var (provider, factory) = CreateProvider();
        var lattice = StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(8, 2)));

        await provider.GetShardIndicesAsync(UserTree);

        _ = lattice.Received(1).GetRoutingAsync(true, Arg.Any<CancellationToken>());
        _ = lattice.DidNotReceive().GetRoutingAsync(false, Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetShardIndicesAsync_propagates_a_routing_failure()
    {
        var (provider, factory) = CreateProvider();
        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<RoutingInfo>>(_ => throw new InvalidOperationException("registry unreachable"));
        factory.GetGrain<ILattice>(UserTree).Returns(lattice);

        Assert.That(
            async () => await provider.GetShardIndicesAsync(UserTree),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("registry unreachable"));
    }

    // ==================================================================
    // GetShardRootKeysAsync - the shard roots the live routing reaches
    // ==================================================================

    [Test]
    public void GetShardRootKeysAsync_throws_when_tree_id_is_null()
    {
        var (provider, _) = CreateProvider();
        Assert.That(
            async () => await provider.GetShardRootKeysAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardRootKeysAsync_throws_when_tree_id_is_empty()
    {
        var (provider, _) = CreateProvider();
        Assert.That(
            async () => await provider.GetShardRootKeysAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetShardRootKeysAsync_observes_cancellation_before_dispatch()
    {
        var (provider, factory) = CreateProvider();
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
        var (provider, factory) = CreateProvider();
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
        var (provider, factory) = CreateProvider();
        StubRouting(factory, new RoutingInfo(UserTree, new ShardMap { Slots = [0, 1, 0, 5, 0, 1, 5, 1], Version = 3 }));

        var keys = await provider.GetShardRootKeysAsync(UserTree);

        Assert.That(keys, Is.EqualTo(new[] { $"{UserTree}/0", $"{UserTree}/1", $"{UserTree}/5" }));
    }

    [Test]
    public async Task GetShardRootKeysAsync_addresses_the_aliased_physical_tree_not_the_logical_id()
    {
        // Regression: after a resize or a shadow-cutover restore the logical id
        // is an alias, so {logicalId}/{i} names the retired physical copy.
        const string physical = "user-tree-restore-shadow-7";
        var (provider, factory) = CreateProvider();
        StubRouting(factory, new RoutingInfo(physical, ShardMap.CreateDefault(8, 2)));

        var keys = await provider.GetShardRootKeysAsync(UserTree);

        Assert.That(keys, Is.EqualTo(new[] { $"{physical}/0", $"{physical}/1" }));
    }

    [Test]
    public async Task GetShardRootKeysAsync_forces_a_routing_refresh()
    {
        // LatticeGrain caches routing per stateless-worker activation; a stale
        // cache would hide a split or alias swap that has already landed.
        var (provider, factory) = CreateProvider();
        var lattice = StubRouting(factory, new RoutingInfo(UserTree, ShardMap.CreateDefault(8, 2)));

        await provider.GetShardRootKeysAsync(UserTree);

        _ = lattice.Received(1).GetRoutingAsync(true, Arg.Any<CancellationToken>());
        _ = lattice.DidNotReceive().GetRoutingAsync(false, Arg.Any<CancellationToken>());
    }
}
