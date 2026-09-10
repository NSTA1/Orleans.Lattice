using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests that <see cref="ShardRootGrain.PurgeAsync"/> clears the internal-node
/// set with bounded concurrency instead of one node after another.
/// <para>
/// The leaf chain deliberately stays serial - a leaf's sibling pointer has to be
/// read before its state is cleared, so the walk cannot be reordered. The
/// internal-node set is different: <c>CollectInternalNodeIds</c> materialises it
/// completely before the first clear is issued, so the clears are independent of
/// one another and of the traversal that produced them. Issued serially, purging
/// a tree with I internal nodes cost I sequential round trips against a tree that
/// is already offline by contract and has nothing waiting on it.
/// </para>
/// <para>
/// The correctness properties (every collected node cleared exactly once, the
/// shard row cleared last) are pinned by <see cref="ShardRootGrainPurgeTests"/>;
/// this fixture pins the concurrency shape - and re-asserts completeness at a
/// width that crosses several waves, because a batching bug shows up as a
/// dropped trailing wave rather than as a wrong answer on a small tree.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainPurgeFanOutTests
{
    private const string ShardKey = "purge-fanout-tree/0";
    private const string LeafGrainType = "leaf";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }

        public required IReadOnlyList<IBPlusInternalGrain> InternalNodes { get; init; }

        public required Func<int> PeakConcurrentClears { get; init; }

        public required Func<int> ClearedCount { get; init; }
    }

    /// <summary>
    /// Builds a two-level topology - one internal root over
    /// <paramref name="internalChildren"/> internal children, each owning one
    /// leaf - so the collected internal-node set is
    /// <c>internalChildren + 1</c>. Each internal clear takes
    /// <paramref name="perNodeDelay"/>, so a serial sweep costs
    /// <c>nodes x delay</c> and a fanned-out one costs far less.
    /// </summary>
    private static Harness CreateTree(int internalChildren, TimeSpan perNodeDelay)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();

        var inFlight = 0;
        var peak = 0;
        var cleared = 0;

        var leafIds = new GrainId[internalChildren];
        for (var i = 0; i < internalChildren; i++)
        {
            leafIds[i] = GrainId.Create(LeafGrainType, $"{ShardKey}:L{i}");
        }

        for (var i = 0; i < internalChildren; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var next = i + 1 < internalChildren ? (GrainId?)leafIds[i + 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.ClearGrainStateAsync().Returns(Task.CompletedTask);
            factory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
        }

        var internals = new List<IBPlusInternalGrain>(internalChildren + 1);
        var childIds = new List<GrainId>(internalChildren);

        IBPlusInternalGrain Register(GrainId id, bool childrenAreLeaves, IReadOnlyList<GrainId> children)
        {
            var node = Substitute.For<IBPlusInternalGrain>();
            node.AreChildrenLeavesAsync().Returns(Task.FromResult(childrenAreLeaves));
            node.GetChildIdsAsync().Returns(_ => Task.FromResult(new List<GrainId>(children)));
            node.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[children.Count],
                ChildIds = [.. children],
                ChildrenAreLeaves = childrenAreLeaves,
            }));
            node.ClearGrainStateAsync().Returns(async _ =>
            {
                RecordPeak(ref peak, Interlocked.Increment(ref inFlight));
                if (perNodeDelay > TimeSpan.Zero)
                {
                    await Task.Delay(perNodeDelay);
                }
                else
                {
                    await Task.Yield();
                }

                Interlocked.Increment(ref cleared);
                Interlocked.Decrement(ref inFlight);
            });
            factory.GetGrain<IBPlusInternalGrain>(id).Returns(node);
            internals.Add(node);
            return node;
        }

        for (var i = 0; i < internalChildren; i++)
        {
            var id = GrainId.Create("internal", $"{ShardKey}:I{i}");
            childIds.Add(id);
            Register(id, childrenAreLeaves: true, children: [leafIds[i]]);
        }

        var rootId = GrainId.Create("internal", $"{ShardKey}:root");
        Register(rootId, childrenAreLeaves: false, children: childIds);

        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        return new Harness
        {
            Grain = new ShardRootGrain(
                context,
                state,
                factory,
                TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory),
                NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            InternalNodes = internals,
            PeakConcurrentClears = () => Volatile.Read(ref peak),
            ClearedCount = () => Volatile.Read(ref cleared),
        };
    }

    private static void RecordPeak(ref int peak, int current)
    {
        int observed;
        do
        {
            observed = Volatile.Read(ref peak);
            if (current <= observed)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(ref peak, current, observed) != observed);
    }

    [Test]
    public async Task Every_internal_node_is_cleared_across_several_waves()
    {
        // 100 children plus the root is 101 nodes against a width of 32: three
        // full waves plus a partial. A dropped trailing wave would leak the last
        // five nodes' grain state after the tree is gone.
        var h = CreateTree(internalChildren: 100, perNodeDelay: TimeSpan.Zero);

        await h.Grain.PurgeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.ClearedCount(), Is.EqualTo(101));
            Assert.That(h.InternalNodes, Has.Count.EqualTo(101));
        });

        foreach (var node in h.InternalNodes)
        {
            await node.Received(1).ClearGrainStateAsync();
        }
    }

    [Test]
    public async Task The_internal_node_clears_overlap_rather_than_running_serially()
    {
        const int Children = 128;
        var delay = TimeSpan.FromMilliseconds(10);
        var h = CreateTree(Children, delay);

        var sw = Stopwatch.StartNew();
        await h.Grain.PurgeAsync();
        sw.Stop();

        var serialCost = TimeSpan.FromMilliseconds((Children + 1) * delay.TotalMilliseconds);
        Assert.Multiple(() =>
        {
            Assert.That(h.PeakConcurrentClears(), Is.GreaterThan(1),
                "a serial sweep never has more than one clear in flight");
            Assert.That(sw.Elapsed, Is.LessThan(serialCost / 2),
                $"the fan-out should cost far less than the serial {serialCost.TotalMilliseconds} ms");
        });
    }

    [Test]
    public async Task Concurrency_is_bounded_so_a_wide_tree_cannot_swamp_the_scheduler()
    {
        var h = CreateTree(internalChildren: 200, perNodeDelay: TimeSpan.FromMilliseconds(5));

        await h.Grain.PurgeAsync();

        Assert.That(h.PeakConcurrentClears(), Is.LessThanOrEqualTo(BoundedFanOut.DefaultWidth),
            "an unbounded sweep would burst in proportion to the size of the tree");
    }

    [Test]
    public async Task The_sweep_is_complete_before_the_shard_row_is_cleared()
    {
        // PurgeAsync clears the shard row last. If the fan-out returned before
        // its waves settled, a crash between the two would leave orphaned
        // internal-node state with no root left to find it from.
        var h = CreateTree(internalChildren: 64, perNodeDelay: TimeSpan.FromMilliseconds(2));

        await h.Grain.PurgeAsync();

        Assert.That(h.ClearedCount(), Is.EqualTo(65),
            "no internal-node clear may still be in flight when the turn ends");
    }
}
