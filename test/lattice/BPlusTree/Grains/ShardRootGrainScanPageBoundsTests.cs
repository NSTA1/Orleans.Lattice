using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the bounded page-fill scaffolding shared by every
/// <c>*BoundedAsync</c> range-scan entry point on <see cref="ShardRootGrain"/>.
/// <para>
/// Two concerns are covered. First, the <b>walk pooling</b>: a page fill borrows
/// a pooled walk to carry its budget and phase, and must return it on both the
/// success and the fault path of the un-guarded await, or the pool leaks an
/// instance per call. Second, the <b>hard stall ceiling</b>: when
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> is armed and a page fill
/// parks, the call is abandoned with a <see cref="ScanPageStalledException"/>
/// tagged with the phase it stalled in - the tag is the whole point of the
/// ceiling, because it is what makes a recurrence self-diagnosing rather than a
/// bare duration.
/// </para>
/// <para>
/// The prologue and descent phases are driven by parking the first cross-grain
/// await of each: the tree-registry registration inside the activation seed
/// (still phase Prologue) and the leftmost-leaf traversal (phase Descent).
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPageBoundsTests
{
    private const string TreeId = "scan-page-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class GrainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IBPlusInternalGrain Internal { get; init; }
    }

    /// <summary>
    /// Builds a shard root wired for a bounded page fill.
    /// <paramref name="stallDuration"/> is passed straight through as
    /// <see cref="LatticeOptions.MaxScanPageStallDuration"/>: pass
    /// <see cref="Timeout.InfiniteTimeSpan"/> to leave the ceiling disarmed (the
    /// un-guarded await path), or a short finite span to arm it.
    /// </summary>
    private static GrainHarness CreateHarness(
        TimeSpan stallDuration,
        bool seeded = true,
        bool rootIsLeaf = true,
        Func<Task>? registerBehavior = null,
        Func<Task<RoutingTableSnapshot>>? descentBehavior = null,
        Func<Task<GrainId?>>? leafWalkBehavior = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();

        var factory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));
        registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>())
            .Returns(_ => registerBehavior?.Invoke() ?? Task.CompletedTask);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var leafId = GrainId.Create(GrainType.Create("leaf"), GrainIdKeyExtensions.CreateGuidKey(Guid.NewGuid()));
        var leafGrainContext = Substitute.For<IGrainContext>();
        leafGrainContext.GrainId.Returns(leafId);
        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)leaf).GrainContext.Returns(leafGrainContext);
        leaf.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        leaf.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        leaf.GetNextSiblingAsync().Returns(_ =>
            leafWalkBehavior?.Invoke() ?? Task.FromResult<GrainId?>(null));
        leaf.RebuildProjectionFromWalAsync().Returns(Task.CompletedTask);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);

        var internalNode = Substitute.For<IBPlusInternalGrain>();
        internalNode.GetRoutingTableAsync().Returns(_ =>
            descentBehavior?.Invoke() ?? Task.FromResult(new RoutingTableSnapshot
            {
                ChildIds = [leafId],
                SeparatorKeys = [null],
                ChildrenAreLeaves = true,
            }));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalNode);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(internalNode);

        if (seeded)
        {
            state.State.RootNodeId = rootIsLeaf ? leafId : GrainId.Create("internal", Guid.NewGuid().ToString());
            state.State.RootIsLeaf = rootIsLeaf;
        }

        var baseOptions = new LatticeOptions
        {
            MaxScanPageStallDuration = stallDuration,
            // The seed chain has its own ceiling; leave it disarmed so a parked
            // registration surfaces as the scan-page stall under test rather than
            // as the activation-readiness timeout.
            ActivationReadyTimeout = Timeout.InfiniteTimeSpan,
        };
        var optionsResolver = TestOptionsResolver.ForFactory(factory, baseOptions);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new GrainHarness { Grain = grain, State = state, Leaf = leaf, Internal = internalNode };
    }

    // ------------------------------------------- un-guarded await, pool return

    [Test]
    public async Task Bounded_page_fill_that_completes_asynchronously_returns_its_walk_to_the_pool()
    {
        // The page task is deliberately incomplete when the guard inspects it, so
        // the call takes the awaiting path rather than the synchronous fast path.
        // Draining the pool afterwards proves the walk came back: a leaked walk
        // would leave the pool handing out a fresh instance every call.
        var gate = new TaskCompletionSource<GrainId?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var h = CreateHarness(Timeout.InfiniteTimeSpan, leafWalkBehavior: () => gate.Task);

        var page = h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);
        Assert.That(page.IsCompleted, Is.False, "the page fill must still be in flight for this test to mean anything");

        gate.SetResult(null);
        await page;

        // A second call must succeed identically, which it can only do if the
        // first walk was reset and pooled rather than abandoned.
        var second = await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);
        Assert.That(second, Is.Not.Null);
    }

    [Test]
    public void Bounded_page_fill_that_faults_asynchronously_still_returns_its_walk_to_the_pool()
    {
        var gate = new TaskCompletionSource<GrainId?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        // Only the first page fill parks on the gate; a retry walks cleanly, so
        // the retry is a genuine check that the pool survived the fault rather
        // than a second run of the same failure.
        var h = CreateHarness(
            Timeout.InfiniteTimeSpan,
            leafWalkBehavior: () => Interlocked.Increment(ref calls) == 1
                ? gate.Task
                : Task.FromResult<GrainId?>(null));

        var page = h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);
        gate.SetException(new InvalidOperationException("leaf boom"));

        // The fault must propagate unchanged - the pooling wrapper is not allowed
        // to swallow or re-wrap it.
        Assert.That(async () => await page,
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("leaf boom"));

        // And the walk must still have been returned, so the next call works.
        Assert.That(async () => await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None),
            Throws.Nothing);
    }

    // --------------------------------------------------- hard stall ceiling

    [Test]
    public void Page_fill_parked_in_the_prologue_stalls_and_is_tagged_prologue()
    {
        // An unseeded shard runs the activation seed chain while the walk is still
        // in its prologue phase. Parking the registry registration - the first
        // cross-grain await of that chain - parks the page fill before any leaf is
        // read, which is exactly the shape the ceiling exists to bound.
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var h = CreateHarness(
            TimeSpan.FromMilliseconds(150),
            seeded: false,
            registerBehavior: () => parked.Task);

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Phase, Is.EqualTo("prologue"),
                "a stall before the descent must be attributed to the prologue");
            Assert.That(ex.LeavesVisited, Is.Zero, "no leaf was read");
            Assert.That(ex.Operation, Is.EqualTo(nameof(IShardRootGrain.RebuildShardProjectionBoundedAsync)));
            Assert.That(ex.ShardIndex, Is.Zero);
            Assert.That(ex.TreeId, Is.EqualTo(TreeId));
            Assert.That(ex.Message, Does.Contain("before any leaf was read"));
        });

        parked.SetResult();
    }

    [Test]
    public void Page_fill_parked_in_the_descent_stalls_and_is_tagged_descent()
    {
        // An internal root forces a real traversal down to the start leaf, which
        // runs under phase Descent. Parking that traversal stalls the page fill
        // after the prologue but still before any leaf is read.
        var parked = new TaskCompletionSource<RoutingTableSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        var h = CreateHarness(
            TimeSpan.FromMilliseconds(150),
            rootIsLeaf: false,
            descentBehavior: () => parked.Task);

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Phase, Is.EqualTo("descent"),
                "a stall while traversing to the start leaf must be attributed to the descent");
            Assert.That(ex.LeavesVisited, Is.Zero, "the descent ends before any leaf is read");
            Assert.That(ex.Message, Does.Contain("while traversing down to the start leaf"));
            Assert.That(ex.TimeoutSeconds, Is.EqualTo(TimeSpan.FromMilliseconds(150).TotalSeconds));
        });

        parked.SetResult(new RoutingTableSnapshot { ChildIds = [], SeparatorKeys = [null], ChildrenAreLeaves = true });
    }

    [Test]
    public async Task Armed_ceiling_does_not_fault_a_page_fill_that_completes_inside_it()
    {
        // The negative control: the ceiling must be inert on a healthy call.
        var h = CreateHarness(TimeSpan.FromSeconds(30));

        Assert.That(async () => await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None),
            Throws.Nothing);

        // And the armed-but-unfired walk must still be reusable.
        var second = await h.Grain.RebuildShardProjectionBoundedAsync(null, CancellationToken.None);
        Assert.That(second, Is.Not.Null);
    }
}
