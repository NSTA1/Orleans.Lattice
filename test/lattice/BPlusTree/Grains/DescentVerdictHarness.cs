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

    /// <summary>Builds the harness; <paramref name="rightLeafId"/> names the leaf descent reaches for <c>m</c> and above.</summary>
    public static DescentVerdictHarness Create(GrainId? rightLeafId = null)
    {
        var right = rightLeafId ?? RightLeafId;
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", "descent-tree/0"));
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = RootId;
        state.State.RootIsLeaf = false;

        var table = new RoutingTableSnapshot
        {
            SeparatorKeys = [null, "m"],
            ChildIds = [LeftLeafId, right],
            ChildrenAreLeaves = true,
        };
        var root = Substitute.For<IBPlusInternalGrain, IGrainBase>();
        var rootContext = Substitute.For<IGrainContext>();
        rootContext.GrainId.Returns(RootId);
        ((IGrainBase)root).GrainContext.Returns(rootContext);
        root.GetRoutingTableAsync().Returns(_ => Task.FromResult(table));
        root.GetLeftmostChildWithMetadataAsync().Returns(_ => Task.FromResult((LeftLeafId, true)));
        root.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>()).Returns(Task.FromResult<SplitResult?>(null));

        var left = Substitute.For<IBPlusLeafGrain>();
        left.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        var leaves = new Dictionary<GrainId, IBPlusLeafGrain>
        {
            [LeftLeafId] = left,
            [right] = Substitute.For<IBPlusLeafGrain>(),
            [SiblingId] = Substitute.For<IBPlusLeafGrain>(),
        };

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(_ => root);
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
        return new DescentVerdictHarness(grain, root, left);
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
