using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1971: <c>ShardRootGrain.CountAsync</c> walked its
/// whole leaf chain inside one non-reentrant call, so counting held the shard -
/// and every other request queued behind it - for the duration. It also had no
/// past-range exit at all, so a narrow range still cost a walk to the end of
/// the chain.
/// <para>
/// Counting differs from the range delete bounded by #1956 in one way that
/// dominates the design: a repeated tombstone is idempotent, but a repeated
/// count is not. A resume position that lands on an already-counted leaf
/// silently inflates the answer, so these tests assert the total is exactly
/// preserved, not merely that the walk terminates.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainCountWorkBoundTests
{
    private const string TreeId = "count-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required Func<int> LeafCountCalls { get; init; }
        public required int TotalKeys { get; init; }
    }

    /// <summary>
    /// Builds a forward leaf chain where leaf <c>i</c> owns keys
    /// <c>k{i:D3}-*</c> and declares its high bound as the next leaf's first
    /// key, so a bounded walk has a real resume point to return.
    /// <para>
    /// The root is modelled as an <b>internal</b> node so a resume key is
    /// re-descended to the leaf that owns it. A leaf root would collapse every
    /// descent onto the single root leaf, which is the real shape for a
    /// one-leaf shard but would make a multi-leaf resume test meaningless.
    /// </para>
    /// </summary>
    private static Harness CreateChain(int leafCount, int keysPerLeaf, int maxLeavesPerBatch)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            ids[i] = GrainId.Create("bplusleaf", $"leaf{i}");

        var rootId = GrainId.Create("bplusinternal", "root");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var countCalls = 0;

        // Internal root: children are the leaves, and separator i is the first
        // key of leaf i (index 0 is always null), which is exactly what each
        // leaf declares as its exclusive high bound.
        var root = Substitute.For<IBPlusInternalGrain>();
        var separators = new string?[leafCount];
        separators[0] = null;
        for (var i = 1; i < leafCount; i++) separators[i] = $"k{i:D3}-000";
        root.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
        {
            SeparatorKeys = separators,
            ChildIds = ids,
            ChildrenAreLeaves = true,
        }));
        root.GetLeftmostChildAsync().Returns(Task.FromResult(ids[0]));
        root.GetLeftmostChildWithMetadataAsync().Returns(Task.FromResult((ids[0], true)));
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var keys = new List<string>();
            for (var j = 0; j < keysPerLeaf; j++)
                keys.Add($"k{index:D3}-{j:D3}");

            leaf.CountAsync(Arg.Any<string?>(), Arg.Any<string?>())
                .Returns(call =>
                {
                    Interlocked.Increment(ref countCalls);
                    var lo = call.ArgAt<string?>(0);
                    var hi = call.ArgAt<string?>(1);
                    return Task.FromResult(keys.Count(k =>
                        (lo is null || string.CompareOrdinal(k, lo) >= 0)
                        && (hi is null || string.CompareOrdinal(k, hi) < 0)));
                });

            var high = index + 1 < leafCount ? $"k{index + 1:D3}-000" : null;
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = $"k{index:D3}-000",
                HighKeyExclusive = high,
            }));

            var next = index + 1 < leafCount ? (GrainId?)ids[index + 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = maxLeavesPerBatch,
                MaxScanPageDuration = TimeSpan.Zero,
            },
            shardCount: 1,
            factory: factory);

        return new Harness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            LeafCountCalls = () => Volatile.Read(ref countCalls),
            TotalKeys = leafCount * keysPerLeaf,
        };
    }

    [Test]
    public async Task A_bounded_batch_stops_after_its_leaf_budget_and_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 4);

        var page = await h.Grain.CountBoundedAsync(null, null);

        Assert.Multiple(() =>
        {
            Assert.That(h.LeafCountCalls(), Is.LessThanOrEqualTo(5),
                "the walk must stop once the leaf budget is spent, instead of "
                + "holding the non-reentrant shard for the whole chain");
            Assert.That(page.ResumeFromInclusive, Is.Not.Null,
                "a batch that stopped early must hand back a resume position, "
                + "otherwise the caller would treat a partial count as the answer");
            Assert.That(page.Count, Is.LessThan(h.TotalKeys),
                "precondition: the batch really did stop short of the whole chain");
        });
    }

    /// <summary>
    /// The property that matters most. Driving the bounded protocol to
    /// completion must produce exactly the same total as the unbounded walk -
    /// no leaf counted twice at a resume boundary, and none skipped.
    /// </summary>
    [Test]
    public async Task Driving_the_bounded_walk_to_completion_counts_every_key_exactly_once()
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 4);

        var total = 0;
        string? cursor = null;
        var batches = 0;
        while (true)
        {
            var page = await h.Grain.CountBoundedAsync(cursor, null);
            total += page.Count;
            batches++;
            Assert.That(batches, Is.LessThan(100), "the resumed walk must terminate");
            if (page.ResumeFromInclusive is not { } next) break;
            cursor = next;
        }

        Assert.Multiple(() =>
        {
            Assert.That(total, Is.EqualTo(h.TotalKeys),
                "a resumed count must equal the unbounded count exactly - a resume "
                + "position landing on an already-counted leaf would inflate it");
            Assert.That(batches, Is.GreaterThan(1),
                "precondition: the walk really was split into multiple batches");
        });
    }

    /// <summary>
    /// The wire-compatible wrapper must behave exactly as it always did for a
    /// caller that has not adopted the bounded protocol.
    /// </summary>
    [Test]
    public async Task The_unbounded_wrapper_still_returns_the_whole_count()
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 4);

        Assert.That(await h.Grain.CountAsync(null, null), Is.EqualTo(h.TotalKeys));
    }

    /// <summary>
    /// A single-leaf shard must never hand back a resume key. It has no next
    /// sibling to resume into, so a resume position would re-descend to the
    /// same leaf and count it again forever - the failure mode that makes
    /// counting stricter than the idempotent range delete.
    /// </summary>
    [Test]
    public async Task A_single_leaf_shard_never_reports_a_resume_key()
    {
        var h = CreateChain(leafCount: 1, keysPerLeaf: 2, maxLeavesPerBatch: 1);

        var page = await h.Grain.CountBoundedAsync(null, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.Null);
            Assert.That(page.Count, Is.EqualTo(h.TotalKeys));
        });
    }

    /// <summary>
    /// Issue 1971's cheap win: a narrow range must stop once the walk is past
    /// its upper bound, rather than walking to the end of the shard's chain.
    /// </summary>
    [Test]
    public async Task A_narrow_range_stops_once_the_walk_is_past_it()
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 1000);

        var count = await h.Grain.CountAsync("k000-000", "k002-000");

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(4), "leaves 0 and 1, two keys each");
            Assert.That(h.LeafCountCalls(), Is.LessThan(10),
                "a narrow range must not cost a walk to the end of a 40-leaf "
                + "chain; before the past-range exit this visited all 40");
        });
    }
}
