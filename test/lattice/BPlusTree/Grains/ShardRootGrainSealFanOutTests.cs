using System.Diagnostics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for issue 1960: installing the moved-away seal across a shard's leaf
/// chain used to apply each leaf's write serially, so the shard root - which is
/// non-reentrant, and through which every read path reaches a leaf - was held
/// for the sum of every per-leaf write.
/// <para>
/// The walk deliberately still happens inside one grain turn. That atomicity is
/// load-bearing: a half-installed seal is wrong in the dangerous direction,
/// because an already-sealed leaf returns null for a key whose slot the source
/// shard still owns, which is the U9h-C "key missing mid-chaos" failure. What
/// changed is that the per-leaf writes fan out with bounded concurrency instead
/// of running one after another.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainSealFanOutTests
{
    private const string TreeId = "seal-fanout-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required Func<List<int>> MarkedLeafIndices { get; init; }
        public required Func<List<int>> UnmarkedLeafIndices { get; init; }
        public required Func<int> PeakConcurrentMarks { get; init; }
    }

    /// <summary>
    /// Builds a leaf chain whose <c>MarkSlotsMovedAwayAsync</c> takes
    /// <paramref name="perLeafDelay"/>, so a serial walk costs
    /// <c>leafCount x perLeafDelay</c> and a fanned-out one costs far less.
    /// Records which leaves were marked and the peak concurrency observed.
    /// </summary>
    private static Harness CreateChain(int leafCount, TimeSpan perLeafDelay)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var marked = new List<int>();
        var unmarked = new List<int>();
        var markedLock = new object();
        var inFlight = 0;
        var peak = 0;

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.MarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(async _ =>
                {
                    var now = Interlocked.Increment(ref inFlight);
                    // Track the high-water mark of concurrent applies.
                    int observed;
                    do
                    {
                        observed = Volatile.Read(ref peak);
                        if (now <= observed) break;
                    } while (Interlocked.CompareExchange(ref peak, now, observed) != observed);

                    if (perLeafDelay > TimeSpan.Zero)
                        await Task.Delay(perLeafDelay);

                    lock (markedLock) marked.Add(index);
                    Interlocked.Decrement(ref inFlight);
                });

            leaf.UnmarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>())
                .Returns(_ =>
                {
                    lock (markedLock) unmarked.Add(index);
                    return Task.CompletedTask;
                });

            var next = index + 1 < leafCount ? (GrainId?)ids[index + 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 1, factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            State = state,
            MarkedLeafIndices = () => { lock (markedLock) return marked.ToList(); },
            UnmarkedLeafIndices = () => { lock (markedLock) return unmarked.ToList(); },
            PeakConcurrentMarks = () => Volatile.Read(ref peak),
        };
    }

    // --- correctness ---

    [Test]
    public async Task Every_leaf_in_the_chain_is_sealed()
    {
        var h = CreateChain(leafCount: 100, perLeafDelay: TimeSpan.Zero);

        var count = await h.Grain.MarkLeavesMovedAwayAsync([1, 2, 3], virtualShardCount: 16);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(100), "every leaf must be reported");
            Assert.That(h.MarkedLeafIndices().OrderBy(i => i), Is.EqualTo(Enumerable.Range(0, 100)),
                "batching must not skip a leaf - a missed leaf would serve a stale " +
                "orphan value for a moved key after the swap");
        });
    }

    [Test]
    public async Task A_chain_shorter_than_one_batch_is_sealed_completely()
    {
        var h = CreateChain(leafCount: 3, perLeafDelay: TimeSpan.Zero);

        var count = await h.Grain.MarkLeavesMovedAwayAsync([7], virtualShardCount: 16);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(3));
            Assert.That(h.MarkedLeafIndices(), Has.Count.EqualTo(3));
        });
    }

    [Test]
    public async Task A_chain_that_is_not_a_whole_number_of_batches_is_sealed_completely()
    {
        // 70 leaves against a batch size of 32: two full batches plus a partial.
        var h = CreateChain(leafCount: 70, perLeafDelay: TimeSpan.Zero);

        var count = await h.Grain.MarkLeavesMovedAwayAsync([1], virtualShardCount: 8);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(70));
            Assert.That(h.MarkedLeafIndices().OrderBy(i => i), Is.EqualTo(Enumerable.Range(0, 70)),
                "the trailing partial batch must not be dropped");
        });
    }

    [Test]
    public async Task An_empty_slot_set_seals_nothing_and_does_not_walk()
    {
        var h = CreateChain(leafCount: 10, perLeafDelay: TimeSpan.Zero);

        var count = await h.Grain.MarkLeavesMovedAwayAsync([], virtualShardCount: 16);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.Zero);
            Assert.That(h.MarkedLeafIndices(), Is.Empty);
        });
    }

    /// <summary>
    /// The installation must be complete when the call returns. The split
    /// coordinator relies on this: it calls this immediately before
    /// <c>EnterRejectPhaseAsync</c> so no read crosses the Swap boundary
    /// observing an unmarked leaf.
    /// </summary>
    [Test]
    public async Task The_seal_is_fully_installed_before_the_call_returns()
    {
        var h = CreateChain(leafCount: 64, perLeafDelay: TimeSpan.FromMilliseconds(5));

        await h.Grain.MarkLeavesMovedAwayAsync([1, 2], virtualShardCount: 16);

        Assert.That(h.MarkedLeafIndices(), Has.Count.EqualTo(64),
            "no per-leaf write may still be in flight when the turn ends");
    }

    // --- impact ---

    /// <summary>
    /// The measurement this change exists for. With a per-leaf write cost, a
    /// serial walk costs leafCount x delay; the fan-out should cost roughly
    /// ceil(leafCount / 32) x delay. Asserted with a wide margin so the test
    /// pins the order of magnitude rather than a timing.
    /// </summary>
    [Test]
    public async Task The_per_leaf_writes_run_concurrently_rather_than_serially()
    {
        const int leaves = 128;
        var delay = TimeSpan.FromMilliseconds(10);
        var h = CreateChain(leaves, delay);

        var sw = Stopwatch.StartNew();
        await h.Grain.MarkLeavesMovedAwayAsync([1], virtualShardCount: 16);
        sw.Stop();

        var serialCost = TimeSpan.FromMilliseconds(leaves * delay.TotalMilliseconds);
        Assert.Multiple(() =>
        {
            Assert.That(h.PeakConcurrentMarks(), Is.GreaterThan(1),
                "the per-leaf writes must overlap");
            Assert.That(sw.Elapsed, Is.LessThan(serialCost / 2),
                $"fan-out should cost far less than the serial {serialCost.TotalMilliseconds} ms");
        });
    }

    [Test]
    public async Task Concurrency_is_bounded_so_a_long_chain_cannot_swamp_the_scheduler()
    {
        var h = CreateChain(leafCount: 200, perLeafDelay: TimeSpan.FromMilliseconds(5));

        await h.Grain.MarkLeavesMovedAwayAsync([1], virtualShardCount: 16);

        Assert.That(h.PeakConcurrentMarks(), Is.LessThanOrEqualTo(32),
            "an unbounded fan-out over a whole shard would burst proportionally " +
            "to the chain length against the storage provider");
    }

    // --- the consolidation inverse shares the helper ---

    [Test]
    public async Task Unsealing_covers_every_leaf_too()
    {
        var h = CreateChain(leafCount: 50, perLeafDelay: TimeSpan.Zero);
        // ReclaimSlotsAsync lifts a seal, so the shard record has to already
        // carry one under the same virtual shard count - otherwise it
        // short-circuits before reaching the leaf layer at all. The map is
        // slot -> owning shard index.
        h.State.State.MovedAwaySlots[1] = 1;
        h.State.State.MovedAwaySlots[2] = 1;
        h.State.State.MovedAwayVirtualShardCount = 16;

        var reclaimed = await h.Grain.ReclaimSlotsAsync([1, 2], virtualShardCount: 16);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.EqualTo(2));
            Assert.That(h.UnmarkedLeafIndices().OrderBy(i => i), Is.EqualTo(Enumerable.Range(0, 50)),
                "the consolidation inverse shares the fan-out helper and must " +
                "reach every leaf too");
        });
    }
}
