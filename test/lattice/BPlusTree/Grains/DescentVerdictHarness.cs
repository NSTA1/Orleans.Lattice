using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A real <see cref="ShardRootGrain"/> over a one-level tree - the root routes
/// <c>[.., m)</c> to the left leaf and <c>[m, ..)</c> to the right - whose link batch a
/// test can hold open, so a descent overlaps it deterministically (#5876).
/// </summary>
internal sealed class DescentVerdictHarness
{
    /// <summary>The root internal node.</summary>
    public static readonly GrainId RootId = GrainId.Create("internal", "descent-root");

    /// <summary>The leaf owning <c>[.., m)</c>.</summary>
    public static readonly GrainId LeftLeafId = GrainId.Create("leaf", "descent-left");

    /// <summary>The default leaf owning <c>[m, ..)</c>.</summary>
    public static readonly GrainId RightLeafId = GrainId.Create("leaf", "descent-right");

    /// <summary>The sibling the held link batch links.</summary>
    public static readonly GrainId SiblingId = GrainId.Create("leaf", "descent-sibling");

    private DescentVerdictHarness(ShardRootGrain grain, IBPlusInternalGrain root, IBPlusLeafGrain left)
    {
        Grain = grain;
        Root = root;
        Left = left;
    }

    /// <summary>The shard root under test.</summary>
    public ShardRootGrain Grain { get; }

    /// <summary>The root internal node's substitute.</summary>
    public IBPlusInternalGrain Root { get; }

    /// <summary>The left leaf's substitute.</summary>
    public IBPlusLeafGrain Left { get; }

    /// <summary>The left internal node of the two-level shapes.</summary>
    public static readonly GrainId LeftParentId = GrainId.Create("internal", "descent-left-parent");

    /// <summary>The right internal node of the two-level shapes.</summary>
    public static readonly GrainId RightParentId = GrainId.Create("internal", "descent-right-parent");

    /// <summary>Builds the one-level harness; <paramref name="rightLeafId"/> names the leaf descent reaches for <c>m</c> and above.</summary>
    public static DescentVerdictHarness Create(GrainId? rightLeafId = null)
    {
        var right = rightLeafId ?? RightLeafId;
        return Build(
            new Dictionary<GrainId, RoutingTableSnapshot>
            {
                [RootId] = Table(true, null, (null, LeftLeafId), ("m", right)),
            },
            [LeftLeafId, right, SiblingId],
            acceptAt: RootId);
    }

    /// <summary>
    /// A two-level tree whose left parent divided at <c>m</c> - its bound narrowed and its
    /// right child moved to a new right half - while the root does not yet route to that
    /// half (a5e2e2ef's V1 case): the root sends every key to the left parent.
    /// </summary>
    public static DescentVerdictHarness CreateLaggingInternalSplit(GrainId? leftLeafId = null)
    {
        var left = leftLeafId ?? LeftLeafId;
        return Build(
            new Dictionary<GrainId, RoutingTableSnapshot>
            {
                [RootId] = Table(false, null, (null, LeftParentId)),
                [LeftParentId] = Table(true, "m", (null, left)),
                [RightParentId] = Table(true, null, (null, RightLeafId)),
            },
            [left, RightLeafId, SiblingId],
            acceptAt: LeftParentId);
    }

    /// <summary>
    /// A settled two-level tree - the root routes <c>m</c> and above to the right parent -
    /// whose held link batch, if any, accepts at the left parent: another subtree than the
    /// right leaf's.
    /// </summary>
    public static DescentVerdictHarness CreateTwoLevel() => Build(
        new Dictionary<GrainId, RoutingTableSnapshot>
        {
            [RootId] = Table(false, null, (null, LeftParentId), ("m", RightParentId)),
            [LeftParentId] = Table(true, null, (null, LeftLeafId)),
            [RightParentId] = Table(true, null, (null, RightLeafId)),
        },
        [LeftLeafId, RightLeafId, SiblingId],
        acceptAt: LeftParentId);

    private static RoutingTableSnapshot Table(bool childrenAreLeaves, string? high, params (string? Separator, GrainId Child)[] entries) => new()
    {
        SeparatorKeys = entries.Select(e => e.Separator).ToArray(),
        ChildIds = entries.Select(e => e.Child).ToArray(),
        ChildrenAreLeaves = childrenAreLeaves,
        HighKeyExclusive = high,
    };

    private static DescentVerdictHarness Build(Dictionary<GrainId, RoutingTableSnapshot> tables, GrainId[] leafIds, GrainId acceptAt)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", "descent-tree/0"));
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = RootId;
        state.State.RootIsLeaf = false;

        var internals = new Dictionary<GrainId, IBPlusInternalGrain>();
        foreach (var (id, table) in tables)
        {
            var node = Substitute.For<IBPlusInternalGrain, IGrainBase>();
            var nodeContext = Substitute.For<IGrainContext>();
            nodeContext.GrainId.Returns(id);
            ((IGrainBase)node).GrainContext.Returns(nodeContext);
            node.GetRoutingTableAsync().Returns(_ => Task.FromResult(table));
            node.GetLeftmostChildWithMetadataAsync().Returns(_ => Task.FromResult((table.ChildIds[0], table.ChildrenAreLeaves)));
            node.RouteWithMetadataAsync(Arg.Any<string>()).Returns(call => Task.FromResult(table.Route(call.ArgAt<string>(0))));
            node.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>()).Returns(Task.FromResult<SplitResult?>(null));
            internals[id] = node;
        }

        var leaves = new Dictionary<GrainId, IBPlusLeafGrain>();
        foreach (var id in leafIds)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
            leaves[id] = leaf;
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(call => internals[call.ArgAt<GrainId>(0)]);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(call => leaves[call.ArgAt<GrainId>(0)]);
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Substitute.For<IShardRootGrain>());

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
        grain.UseHandOffStorageForTesting(new EnumerableMemoryGrainStorage());
        return new DescentVerdictHarness(grain, internals[acceptAt], leaves[leafIds[0]]);
    }

    /// <summary>
    /// Starts a write whose leaf split is being linked and holds the link batch inside the
    /// root's accept; dispose the returned handle to complete it.
    /// </summary>
    public async Task<HeldLinkBatch> HoldLinkBatchAsync()
    {
        var accepting = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Root.AcceptSplitAsync("c", SiblingId).Returns(_ =>
        {
            entered.TrySetResult();
            return accepting.Task;
        });
        Left.SetAsync("b", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "c",
            NewSiblingId = SiblingId,
            ChildIsLeaf = true,
        }));

        var write = Grain.SetAsync("b", [1]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        return new HeldLinkBatch(accepting, write);
    }

    /// <summary>A link batch held open inside the root's accept.</summary>
    internal sealed class HeldLinkBatch(TaskCompletionSource<SplitResult?> accepting, Task write)
    {
        /// <summary>Completes the batch and waits for its write.</summary>
        public async Task ReleaseAsync()
        {
            accepting.TrySetResult(null);
            await write.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }
}
