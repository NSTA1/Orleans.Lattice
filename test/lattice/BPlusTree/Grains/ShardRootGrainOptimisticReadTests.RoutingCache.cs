using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    private static readonly GrainId InternalRootId = GrainId.Create("bplusinternal", "optimistic-root");
    private static readonly GrainId LeftLeafId = GrainId.Create("bplusleaf", "optimistic-left");
    private static readonly GrainId RightLeafId = GrainId.Create("bplusleaf", "optimistic-right");

    private static RoutingTableSnapshot PreSplitRouting => new()
    {
        SeparatorKeys = [null],
        ChildIds = [LeftLeafId],
        ChildrenAreLeaves = true,
    };

    private static RoutingTableSnapshot PostSplitRouting => new()
    {
        SeparatorKeys = [null, "m"],
        ChildIds = [LeftLeafId, RightLeafId],
        ChildrenAreLeaves = true,
    };

    private static (ShardRootGrain Grain, IBPlusInternalGrain Root, ILeafCacheGrain Left, ILeafCacheGrain Right, IBPlusLeafGrain RightLeaf)
        CreateGrainWithInternalRoot()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = InternalRootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var root = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(InternalRootId).Returns(root);

        var left = Substitute.For<ILeafCacheGrain>();
        var right = Substitute.For<ILeafCacheGrain>();
        var leftKey = LeftLeafId.ToString();
        var rightKey = RightLeafId.ToString();
        factory.GetGrain<ILeafCacheGrain>(Arg.Is<string>(k => k == leftKey), Arg.Any<string>()).Returns(left);
        factory.GetGrain<ILeafCacheGrain>(Arg.Is<string>(k => k == rightKey), Arg.Any<string>()).Returns(right);
        var rightLeaf = Substitute.For<IBPlusLeafGrain>();
        rightLeaf.GetAsync(Arg.Any<string>()).Returns((byte[]?)null);
        factory.GetGrain<IBPlusLeafGrain>(RightLeafId).Returns(rightLeaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { OptimisticShardRootPointReads = true },
            factory: factory);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, root, left, right, rightLeaf);
    }

    [Test]
    public async Task TryGetOptimisticAsync_routing_cache_miss_defers_to_serial_without_fetching_or_caching()
    {
        var (grain, root, left, right, rightLeaf) = CreateGrainWithInternalRoot();
        root.GetRoutingTableAsync().Returns(Task.FromResult(PostSplitRouting));
        rightLeaf.GetWithVersionAsync("z").Returns(Stamped(Encoding.UTF8.GetBytes("vz")));

        var first = await grain.TryGetOptimisticAsync("z");
        var second = await grain.TryGetOptimisticAsync("z");

        // An optimistic read runs interleaved with serial writes, so it must never
        // fetch (and so never publish) a routing table the write path would then
        // route through: a cache miss is a serial retry, every time.
        Assert.That(first.IsValidated, Is.False);
        Assert.That(second.IsValidated, Is.False);
        await root.DidNotReceive().GetRoutingTableAsync();
        await left.DidNotReceive().GetAsync(Arg.Any<string>());
        await right.DidNotReceive().GetAsync(Arg.Any<string>());
        await rightLeaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task TryGetOptimisticAsync_routes_through_a_routing_table_the_serial_path_cached()
    {
        var (grain, root, _, right, rightLeaf) = CreateGrainWithInternalRoot();
        root.GetRoutingTableAsync().Returns(Task.FromResult(PostSplitRouting));
        right.GetAsync("z").Returns(Encoding.UTF8.GetBytes("vz"));
        rightLeaf.GetWithVersionAsync("z").Returns(Stamped(Encoding.UTF8.GetBytes("vz")));

        await grain.TryGetOptimisticAsync("z");
        await grain.GetAsync("z");
        var optimistic = await grain.TryGetOptimisticAsync("z");

        Assert.That(optimistic.IsValidated, Is.True);
        Assert.That(Encoding.UTF8.GetString(optimistic.Value!), Is.EqualTo("vz"));
        await root.Received(1).GetRoutingTableAsync();
    }

    [Test]
    public async Task TryGetOptimisticAsync_invalidated_routing_table_defers_to_serial()
    {
        var (grain, root, _, right, _) = CreateGrainWithInternalRoot();
        root.GetRoutingTableAsync().Returns(Task.FromResult(PostSplitRouting));
        right.GetAsync("z").Returns(Encoding.UTF8.GetBytes("vz"));
        await grain.GetAsync("z");

        grain.InvalidateRoutingTable(InternalRootId);
        var optimistic = await grain.TryGetOptimisticAsync("z");

        Assert.That(optimistic.IsValidated, Is.False);
        await root.Received(1).GetRoutingTableAsync();
    }

    [Test]
    public async Task GetAsync_routing_table_fetched_across_an_invalidation_is_not_cached()
    {
        var (grain, root, left, right, _) = CreateGrainWithInternalRoot();

        // The first fetch is served by the internal root BEFORE a leaf split is
        // accepted into it, but its reply only reaches the shard root AFTER the
        // split's publish invalidated the routing cache (reachable through the
        // interleaved SetManyAsync).
        var staleFetch = new TaskCompletionSource<RoutingTableSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        root.GetRoutingTableAsync().Returns(staleFetch.Task, Task.FromResult(PostSplitRouting));
        left.GetAsync("z").Returns((byte[]?)null);
        right.GetAsync("z").Returns(Encoding.UTF8.GetBytes("vz"));

        var firstRead = grain.GetAsync("z");
        Assert.That(firstRead.IsCompleted, Is.False, "The read must be parked on the routing-table fetch.");

        grain.InvalidateRoutingTable(InternalRootId);
        staleFetch.SetResult(PreSplitRouting);
        await firstRead;

        // A later read must not route through the pre-split children list: that
        // snapshot names only the left leaf, which no longer holds "z".
        var second = await grain.GetAsync("z");

        Assert.That(second, Is.Not.Null, "A stale cached routing table lost a present key.");
        Assert.That(Encoding.UTF8.GetString(second!), Is.EqualTo("vz"));
        await root.Received(2).GetRoutingTableAsync();
    }
}
