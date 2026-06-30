using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1046: the paged range-scan sibling walks in
/// <see cref="ShardRootGrain"/> must terminate as soon as the walk provably
/// leaves the <c>[startInclusive, endExclusive)</c> range, rather than reading
/// every remaining leaf to the end of the tree. The walk consults each leaf's
/// persisted <see cref="LeafKeyRange"/> bounds (a range-based, predicate-
/// independent signal) to decide when to stop.
/// </summary>
[TestFixture]
public class ShardRootGrainRangeScanTerminationTests
{
    private const string TreeId = "range-scan-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class ChainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IReadOnlyList<IBPlusLeafGrain> Leaves { get; init; }
    }

    /// <summary>
    /// Builds a forward sibling chain leaf0 -&gt; leaf1 -&gt; ... -&gt; leaf(N-1)
    /// rooted at leaf0 (root-is-leaf). Each leaf's owned-key-range high bound is
    /// taken from <paramref name="highBounds"/> (entry <c>i</c> is leaf <c>i</c>'s
    /// <see cref="LeafKeyRange.HighKeyExclusive"/>; a <see langword="null"/>
    /// entry leaves that leaf's high bound unset). Only leaf0 returns rows for a
    /// scan; every later leaf returns empty (it is out of range), so the test
    /// counts how many leaves the coordinator actually reads.
    /// </summary>
    private static ChainHarness CreateForwardChain(
        string?[] highBounds,
        IReadOnlyList<KeyValuePair<string, byte[]>>? leaf0Entries = null,
        string?[]? lowBounds = null)
    {
        var n = highBounds.Length;
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[n];
        for (var i = 0; i < n; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var entries0 = leaf0Entries ?? new[] { new KeyValuePair<string, byte[]>("a1", new byte[] { 1 }) };
        var keys0 = entries0.Select(e => e.Key).ToList();

        var factory = Substitute.For<IGrainFactory>();
        var leaves = new IBPlusLeafGrain[n];
        for (var i = 0; i < n; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var entries = i == 0 ? entries0.ToList() : new List<KeyValuePair<string, byte[]>>();
            var keys = i == 0 ? keys0.ToList() : new List<string>();

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(entries.ToList()));
            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(keys.ToList()));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = lowBounds?[i],
                HighKeyExclusive = highBounds[i],
            }));
            var next = i + 1 < n ? (GrainId?)ids[i + 1] : null;
            var prev = i - 1 >= 0 ? (GrainId?)ids[i - 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult(prev));

            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            leaves[i] = leaf;
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 1, factory: factory);

        return new ChainHarness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            Leaves = leaves,
        };
    }

    /// <summary>
    /// Builds a reverse sibling chain rooted at the rightmost leaf (root-is-leaf
    /// addressing leaf(N-1)), walked backward via prev-sibling pointers. Only the
    /// rightmost leaf returns rows; each earlier leaf is out of range. Bounds are
    /// supplied per leaf so the test can drive early termination on the low bound.
    /// </summary>
    private static ChainHarness CreateReverseChain(string?[] lowBounds)
    {
        var n = lowBounds.Length;
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[n];
        for (var i = 0; i < n; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        // Root-is-leaf addressing the rightmost leaf so the reverse walk starts there.
        state.State.RootNodeId = ids[n - 1];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaves = new IBPlusLeafGrain[n];
        for (var i = 0; i < n; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var entries = i == n - 1
                ? new List<KeyValuePair<string, byte[]>> { new("z1", new byte[] { 1 }) }
                : new List<KeyValuePair<string, byte[]>>();

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(entries.ToList()));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = lowBounds[i],
                HighKeyExclusive = null,
            }));
            var next = i + 1 < n ? (GrainId?)ids[i + 1] : null;
            var prev = i - 1 >= 0 ? (GrainId?)ids[i - 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult(prev));

            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            leaves[i] = leaf;
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 1, factory: factory);

        return new ChainHarness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            Leaves = leaves,
        };
    }

    private static async Task AssertReadCountAsync(IBPlusLeafGrain leaf, int expected)
    {
        await leaf.Received(expected).GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(),
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>());
    }

    // ========================================================================
    // Forward sorted entries
    // ========================================================================

    [Test]
    public async Task GetSortedEntriesBatchAsync_stops_at_first_leaf_whose_high_bound_reaches_endExclusive()
    {
        // 50 leaves; only leaf0 is in range, and its high bound == endExclusive.
        var highs = new string?[50];
        highs[0] = "b"; // leaf0 owns [.., "b"); next siblings all start >= "b" >= endExclusive.
        for (var i = 1; i < highs.Length; i++) highs[i] = "z";
        var h = CreateForwardChain(highs);

        var page = await h.Grain.GetSortedEntriesBatchAsync("a", "b", pageSize: 100);

        Assert.That(page.Entries, Has.Count.EqualTo(1));
        Assert.That(page.HasMore, Is.False);
        await AssertReadCountAsync(h.Leaves[0], 1);
        // The whole point of issue 1046: out-of-range siblings are never read,
        // independent of how many follow the last in-range leaf.
        for (var i = 1; i < h.Leaves.Count; i++)
            await AssertReadCountAsync(h.Leaves[i], 0);
        // We must not even fetch the next-sibling pointer once range-exhausted.
        await h.Leaves[0].DidNotReceive().GetNextSiblingAsync();
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_without_endExclusive_still_walks_to_end_of_tree()
    {
        // Control: an unbounded scan has no range to leave, so the prior
        // end-of-tree walk behaviour is preserved (every leaf is read).
        var highs = new string?[4];
        highs[0] = "b";
        for (var i = 1; i < highs.Length; i++) highs[i] = "z";
        var h = CreateForwardChain(highs);

        var page = await h.Grain.GetSortedEntriesBatchAsync(startInclusive: null, endExclusive: null, pageSize: 100);

        Assert.That(page.HasMore, Is.False);
        for (var i = 0; i < h.Leaves.Count; i++)
            await AssertReadCountAsync(h.Leaves[i], 1);
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_with_null_high_bound_falls_back_to_walking_siblings()
    {
        // Safety fallback: a leaf whose high bound is unset (legacy state /
        // outermost leaf) must NOT terminate the walk early - the coordinator
        // keeps walking exactly as before.
        var highs = new string?[3] { null, null, null };
        var h = CreateForwardChain(highs);

        var page = await h.Grain.GetSortedEntriesBatchAsync("a", "b", pageSize: 100);

        Assert.That(page.HasMore, Is.False);
        for (var i = 0; i < h.Leaves.Count; i++)
            await AssertReadCountAsync(h.Leaves[i], 1);
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_terminates_even_when_predicate_filters_every_in_range_row()
    {
        // The termination signal is range-based, not emptiness-based: even if a
        // predicate filtered every row on the in-range leaf (leaf0 returns no
        // entries), the high-bound check still stops the walk.
        var highs = new string?[40];
        highs[0] = "b";
        for (var i = 1; i < highs.Length; i++) highs[i] = "z";
        var h = CreateForwardChain(highs, leaf0Entries: new List<KeyValuePair<string, byte[]>>());

        var page = await h.Grain.GetSortedEntriesBatchAsync("a", "b", pageSize: 100);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.HasMore, Is.False);
        await AssertReadCountAsync(h.Leaves[0], 1);
        for (var i = 1; i < h.Leaves.Count; i++)
            await AssertReadCountAsync(h.Leaves[i], 0);
    }

    // ========================================================================
    // Forward sorted keys
    // ========================================================================

    [Test]
    public async Task GetSortedKeysBatchAsync_stops_at_first_leaf_whose_high_bound_reaches_endExclusive()
    {
        var highs = new string?[30];
        highs[0] = "b";
        for (var i = 1; i < highs.Length; i++) highs[i] = "z";
        var h = CreateForwardChain(highs);

        var page = await h.Grain.GetSortedKeysBatchAsync("a", "b", pageSize: 100);

        Assert.That(page.Keys, Has.Count.EqualTo(1));
        Assert.That(page.HasMore, Is.False);
        await h.Leaves[0].Received(1).GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>());
        for (var i = 1; i < h.Leaves.Count; i++)
            await h.Leaves[i].DidNotReceive().GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>());
    }

    // ========================================================================
    // Reverse sorted entries
    // ========================================================================

    [Test]
    public async Task GetSortedEntriesBatchReverseAsync_stops_at_first_leaf_whose_low_bound_reaches_startInclusive()
    {
        // Rightmost leaf owns [low, ..) with low <= startInclusive, so every
        // previous sibling holds only keys strictly below startInclusive.
        var n = 50;
        var lows = new string?[n];
        for (var i = 0; i < n; i++) lows[i] = "a"; // earlier leaves below range
        lows[n - 1] = "y"; // rightmost leaf low bound == startInclusive
        var h = CreateReverseChain(lows);

        var page = await h.Grain.GetSortedEntriesBatchReverseAsync("y", "z", pageSize: 100);

        Assert.That(page.Entries, Has.Count.EqualTo(1));
        Assert.That(page.HasMore, Is.False);
        await AssertReadCountAsync(h.Leaves[n - 1], 1);
        for (var i = 0; i < n - 1; i++)
            await AssertReadCountAsync(h.Leaves[i], 0);
        await h.Leaves[n - 1].DidNotReceive().GetPrevSiblingAsync();
    }
}
