using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Grain-level coverage for the inlined stale-routing retry loops of
/// <see cref="LatticeGrain.GetAsync"/> and <see cref="LatticeGrain.GetWithVersionAsync"/>.
/// The grain is built in-process with a substituted <see cref="IShardRootGrain"/> whose
/// read call is programmed to throw a specific routing fault, so each catch arm
/// (stale-shard invalidate-and-retry, stale-tree alias rethrow, invalid-operation
/// alias rethrow) is exercised deterministically without a multi-silo cluster or any
/// timing dependency.
/// </summary>
[TestFixture]
public sealed class LatticeGrainShardRoutingRetryTests
{
    private const string TreeId = "shard-routing-retry";

    private static (LatticeGrain Grain, IShardRootGrain Shard) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));

        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 1 });
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, new LatticeOptions { WalPartitions = 1 });
        var services = Substitute.For<IServiceProvider>();

        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, shard);
    }

    [Test]
    public async Task GetWithVersionAsync_returns_the_shard_result_on_the_happy_path()
    {
        var (grain, shard) = CreateGrain();
        var expected = new VersionedValue { Value = new byte[] { 7 }, Version = new HybridLogicalClock { WallClockTicks = 9 } };
        shard.GetWithVersionAsync("k").Returns(Task.FromResult(expected));

        var result = await grain.GetWithVersionAsync("k");

        Assert.That(result.Value, Is.EqualTo(new byte[] { 7 }));
    }

    [Test]
    public async Task GetWithVersionAsync_retries_after_a_stale_shard_routing_fault()
    {
        var (grain, shard) = CreateGrain();
        var recovered = new VersionedValue { Value = new byte[] { 3 } };
        shard.GetWithVersionAsync("k").Returns(
            _ => throw new StaleShardRoutingException(),
            _ => Task.FromResult(recovered));

        var result = await grain.GetWithVersionAsync("k");

        Assert.That(result.Value, Is.EqualTo(new byte[] { 3 }));
        await shard.Received(2).GetWithVersionAsync("k");
    }

    [Test]
    public async Task GetWithVersionAsync_retries_after_a_stale_tree_routing_fault_once_an_alias_is_invalidated()
    {
        var (grain, shard) = CreateGrain();
        var recovered = new VersionedValue { Value = new byte[] { 1 } };
        shard.GetWithVersionAsync("k").Returns(
            _ => throw new StaleTreeRoutingException(),
            _ => Task.FromResult(recovered));

        var result = await grain.GetWithVersionAsync("k");

        Assert.That(result.Value, Is.EqualTo(new byte[] { 1 }));
        await shard.Received(2).GetWithVersionAsync("k");
    }

    [Test]
    public void GetWithVersionAsync_rethrows_an_invalid_operation_when_no_alias_to_invalidate()
    {
        var (grain, shard) = CreateGrain();
        shard.GetWithVersionAsync("k").Throws(new InvalidOperationException("boom"));

        Assert.That(
            async () => await grain.GetWithVersionAsync("k"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task GetAsync_returns_the_shard_result_on_the_happy_path()
    {
        var (grain, shard) = CreateGrain();
        shard.GetAsync("k").Returns(Task.FromResult<byte[]?>(new byte[] { 42 }));

        var result = await grain.GetAsync("k");

        Assert.That(result, Is.EqualTo(new byte[] { 42 }));
    }

    [Test]
    public async Task GetAsync_retries_after_a_stale_shard_routing_fault()
    {
        var (grain, shard) = CreateGrain();
        shard.GetAsync("k").Returns(
            _ => throw new StaleShardRoutingException(),
            _ => Task.FromResult<byte[]?>(new byte[] { 5 }));

        var result = await grain.GetAsync("k");

        Assert.That(result, Is.EqualTo(new byte[] { 5 }));
        await shard.Received(2).GetAsync("k");
    }

    [Test]
    public async Task GetAsync_retries_after_a_stale_tree_routing_fault_once_an_alias_is_invalidated()
    {
        var (grain, shard) = CreateGrain();
        shard.GetAsync("k").Returns(
            _ => throw new StaleTreeRoutingException(),
            _ => Task.FromResult<byte[]?>(new byte[] { 1 }));

        var result = await grain.GetAsync("k");

        Assert.That(result, Is.EqualTo(new byte[] { 1 }));
        await shard.Received(2).GetAsync("k");
    }

    [Test]
    public void GetAsync_rethrows_an_invalid_operation_when_no_alias_to_invalidate()
    {
        var (grain, shard) = CreateGrain();
        shard.GetAsync("k").Throws(new InvalidOperationException("boom"));

        Assert.That(
            async () => await grain.GetAsync("k"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task ExistsAsync_returns_the_shard_result_on_the_happy_path()
    {
        var (grain, shard) = CreateGrain();
        shard.ExistsAsync("k").Returns(Task.FromResult(true));

        Assert.That(await grain.ExistsAsync("k"), Is.True);
    }

    [Test]
    public async Task ExistsAsync_retries_after_a_stale_shard_routing_fault()
    {
        var (grain, shard) = CreateGrain();
        shard.ExistsAsync("k").Returns(
            _ => throw new StaleShardRoutingException(),
            _ => Task.FromResult(true));

        Assert.That(await grain.ExistsAsync("k"), Is.True);
        await shard.Received(2).ExistsAsync("k");
    }

    [Test]
    public async Task ExistsAsync_retries_after_a_stale_tree_routing_fault_once_an_alias_is_invalidated()
    {
        var (grain, shard) = CreateGrain();
        shard.ExistsAsync("k").Returns(
            _ => throw new StaleTreeRoutingException(),
            _ => Task.FromResult(true));

        Assert.That(await grain.ExistsAsync("k"), Is.True);
        await shard.Received(2).ExistsAsync("k");
    }

    [Test]
    public void ExistsAsync_rethrows_an_invalid_operation_when_no_alias_to_invalidate()
    {
        var (grain, shard) = CreateGrain();
        shard.ExistsAsync("k").Throws(new InvalidOperationException("boom"));

        Assert.That(
            async () => await grain.ExistsAsync("k"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ExistsAsync_rejects_a_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.That(async () => await grain.ExistsAsync(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetAsync_rejects_a_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.That(async () => await grain.GetAsync(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetWithVersionAsync_rejects_a_null_key()
    {
        var (grain, _) = CreateGrain();
        Assert.That(async () => await grain.GetWithVersionAsync(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetManyAsync_rejects_a_null_key_list()
    {
        var (grain, _) = CreateGrain();
        Assert.That(async () => await grain.GetManyAsync(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetManyAsync_honours_a_cancelled_token()
    {
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await grain.GetManyAsync(new List<string> { "k" }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
