using System.Text;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the per-leaf fan-out in <c>ShardRootGrain.TraverseForBatchReadAsync</c>.
///
/// <para>
/// A batch read that spans several leaves used to await one leaf at a time, so
/// the call's latency was the SUM of every bucket's round trip rather than the
/// MAX. That is not a throughput nicety: a scattered point-probe over a large
/// tree buckets into many leaves, and the summed latency walks off the end of
/// the Orleans response deadline, surfacing as a <c>TimeoutException</c> on
/// <c>ILattice.GetManyAsync</c> rather than as slowness. The write path
/// (<c>SetManyLocalOnlyAsync</c>) already fans out per leaf; only the read path
/// was sequential.
/// </para>
///
/// <para>
/// The test is a barrier rather than a timing measurement, so it cannot pass by
/// luck or flake under a loaded CI runner: every leaf cache blocks on arrival
/// until ALL of them have been entered. Under the fan-out every bucket is
/// dispatched before the first await, so the barrier releases; under the
/// sequential shape the first leaf waits for siblings that were never
/// dispatched, and the test fails deterministically on the barrier timeout.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainBatchReadFanOutTests
{
    private const string TreeId = "test-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// How long a leaf waits at the barrier before declaring the fan-out
    /// regressed. Generous enough that a loaded runner never trips it (the
    /// passing path releases as soon as the last leaf arrives, typically in
    /// microseconds), short enough that a regression fails the run promptly
    /// instead of hanging it.
    /// </summary>
    private static readonly TimeSpan BarrierTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Blocks each arriving leaf until <paramref name="expected"/> of them are
    /// in flight simultaneously, which is only possible when the caller
    /// dispatched them all before awaiting any.
    /// </summary>
    private sealed class LeafArrivalBarrier(int expected)
    {
        private readonly TaskCompletionSource _allArrived =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _arrived;
        private int _peakConcurrency;

        public int PeakConcurrency => Volatile.Read(ref _peakConcurrency);

        public async Task<Dictionary<string, byte[]>> ArriveAndReadAsync(List<string> keys)
        {
            var inFlight = Interlocked.Increment(ref _arrived);

            int observed;
            while (inFlight > (observed = Volatile.Read(ref _peakConcurrency))
                   && Interlocked.CompareExchange(ref _peakConcurrency, inFlight, observed) != observed)
            {
                // Another leaf raised the peak first; re-read and retry.
            }

            if (inFlight >= expected)
            {
                _allArrived.TrySetResult();
            }

            try
            {
                await _allArrived.Task.WaitAsync(BarrierTimeout);
            }
            catch (TimeoutException)
            {
                throw new InvalidOperationException(
                    $"Only {Volatile.Read(ref _arrived)} of {expected} leaf reads were ever in flight at "
                    + "once, so the batch read is awaiting one leaf at a time. Its latency is the sum of "
                    + "every bucket's round trip, which is what pushes a wide batch past the Orleans "
                    + "response deadline.");
            }

            var values = new Dictionary<string, byte[]>(keys.Count);
            foreach (var key in keys)
            {
                values[key] = Encoding.UTF8.GetBytes("v:" + key);
            }

            return values;
        }
    }

    /// <summary>
    /// Builds a shard whose root is an INTERNAL node routing three keys to three
    /// distinct leaves, so a single batch read buckets into three separate leaf
    /// calls. Every leaf cache shares one arrival barrier.
    /// </summary>
    private static (ShardRootGrain Grain, LeafArrivalBarrier Barrier) CreateThreeLeafShard(
        int expectedArrivals = 3)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var internalRootId = GrainId.Create("internal", "root");
        var leafIds = new[]
        {
            GrainId.Create("leaf", "leaf-a"),
            GrainId.Create("leaf", "leaf-b"),
            GrainId.Create("leaf", "leaf-c"),
        };

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = internalRootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

        // Route() walks separators backwards and takes the first that is null or
        // <= key, so "a" -> leaf-a, "b" -> leaf-b, "c" -> leaf-c.
        var internalRoot = Substitute.For<IBPlusInternalGrain>();
        internalRoot.GetRoutingTableAsync()
            .Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[] { null, "b", "c" },
                ChildIds = leafIds,
                ChildrenAreLeaves = true,
            }));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalRoot);

        var barrier = new LeafArrivalBarrier(expectedArrivals);
        var cachesByKey = new Dictionary<string, ILeafCacheGrain>(StringComparer.Ordinal);
        foreach (var leafId in leafIds)
        {
            var cache = Substitute.For<ILeafCacheGrain>();
            cache.GetManyAsync(Arg.Any<List<string>>())
                .Returns(call => barrier.ArriveAndReadAsync(call.Arg<List<string>>()));
            cachesByKey[leafId.ToString()] = cache;
        }

        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>())
            .Returns(call => cachesByKey[call.ArgAt<string>(0)]);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, barrier);
    }

    [Test]
    public async Task Batch_read_dispatches_every_leaf_before_awaiting_any()
    {
        var (grain, barrier) = CreateThreeLeafShard();

        var result = await grain.GetManyAsync(["a", "b", "c"]);

        Assert.Multiple(() =>
        {
            // The fan-out itself: all three leaf reads were in flight at once,
            // which is only reachable when every bucket is dispatched before the
            // first await.
            Assert.That(barrier.PeakConcurrency, Is.EqualTo(3),
                "All three per-leaf reads should be in flight simultaneously.");

            // Paired positive control: the merged result is still correct, so the
            // assertion above cannot pass on a fan-out that lost or mismatched a
            // bucket's rows.
            Assert.That(result, Has.Count.EqualTo(3));
            Assert.That(Encoding.UTF8.GetString(result["a"]), Is.EqualTo("v:a"));
            Assert.That(Encoding.UTF8.GetString(result["b"]), Is.EqualTo("v:b"));
            Assert.That(Encoding.UTF8.GetString(result["c"]), Is.EqualTo("v:c"));
        });
    }

    /// <summary>
    /// The single-bucket shortcut must still return the leaf's rows verbatim: it
    /// bypasses the dispatch array, the <c>Task.WhenAll</c> and the merge target,
    /// so it is a genuinely separate path from the fan-out above.
    /// </summary>
    [Test]
    public async Task Batch_read_confined_to_one_leaf_still_returns_its_rows()
    {
        // One expected arrival: this batch buckets into a single leaf, so the
        // shortcut issues exactly one leaf call and must not wait on siblings.
        var (grain, barrier) = CreateThreeLeafShard(expectedArrivals: 1);

        // Both keys route to leaf-a, so the batch buckets into exactly one leaf.
        var result = await grain.GetManyAsync(["a", "a"]);

        Assert.Multiple(() =>
        {
            Assert.That(barrier.PeakConcurrency, Is.EqualTo(1),
                "A single-bucket batch issues exactly one leaf read.");
            Assert.That(result, Has.Count.EqualTo(1));
            Assert.That(Encoding.UTF8.GetString(result["a"]), Is.EqualTo("v:a"));
        });
    }
}
