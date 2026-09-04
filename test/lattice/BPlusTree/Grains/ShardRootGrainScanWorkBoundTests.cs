using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1955: the paged range-scan loops in
/// <see cref="ShardRootGrain"/> were bounded by <em>output</em> (results
/// collected) rather than by <em>work</em> (leaves visited), so a page fill
/// over leaves that yield few kept results walked an unbounded stretch of the
/// sibling chain inside one call. Because shard reads are deliberately
/// non-reentrant that call head-of-line-blocks every other request to the
/// shard for its whole duration.
/// <para>
/// The leaves this fixture builds declare <em>no</em> key bounds, so the walk
/// has no leaf boundary to resume from. That is the case in which the page
/// fill must still never return an empty page claiming more is available -
/// a caller with neither a resume boundary nor a returned key has nothing to
/// advance its next request with, and would be stranded. Where leaves do
/// declare bounds, an empty page is resumable and is the correct answer; see
/// <see cref="ShardRootGrainSterileScanTests"/> (issue 1992).
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanWorkBoundTests
{
    private const string TreeId = "work-bound-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class ChainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IReadOnlyList<IBPlusLeafGrain> Leaves { get; init; }
        public required Func<int> EntryReadCount { get; init; }
        public required Func<int> KeyReadCount { get; init; }
    }

    /// <summary>
    /// Builds a forward sibling chain of <paramref name="leafCount"/> leaves
    /// where each leaf yields <paramref name="entriesPerLeaf"/> entries with
    /// globally ascending keys, and no leaf declares a high bound (so range
    /// termination never fires and only the work bound can stop the walk).
    /// </summary>
    private static ChainHarness CreateChain(
        int leafCount,
        int entriesPerLeaf,
        int maxLeavesPerScanPage)
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
        var leaves = new IBPlusLeafGrain[leafCount];
        var entryReads = 0;
        var keyReads = 0;

        for (var i = 0; i < leafCount; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var entries = new List<KeyValuePair<string, byte[]>>();
            for (var j = 0; j < entriesPerLeaf; j++)
            {
                // Zero-padded so ordinal key order matches chain order.
                entries.Add(new KeyValuePair<string, byte[]>(
                    $"k{i:D4}-{j:D4}", new byte[] { 1 }));
            }
            var keys = entries.Select(e => e.Key).ToList();

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call =>
                {
                    Interlocked.Increment(ref entryReads);
                    // Honour afterExclusive exactly as a real leaf does, so
                    // pagination actually advances across pages.
                    var after = call.ArgAt<string?>(2);
                    var filtered = after is null
                        ? entries.ToList()
                        : entries.Where(e => string.CompareOrdinal(e.Key, after) > 0).ToList();
                    return Task.FromResult(filtered);
                });
            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call =>
                {
                    Interlocked.Increment(ref keyReads);
                    var after = call.ArgAt<string?>(2);
                    var filtered = after is null
                        ? keys.ToList()
                        : keys.Where(k => string.CompareOrdinal(k, after) > 0).ToList();
                    return Task.FromResult(filtered);
                });
            // No bounds declared, so ForwardWalkLeftRangeAsync never terminates
            // the walk and the work bound is the only thing that can.
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = null,
                HighKeyExclusive = null,
            }));
            var next = i + 1 < leafCount ? (GrainId?)ids[i + 1] : null;
            var prev = i - 1 >= 0 ? (GrainId?)ids[i - 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult(prev));

            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            leaves[i] = leaf;
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = maxLeavesPerScanPage,
                // Deterministic: the leaf count is the only active bound.
                MaxScanPageDuration = TimeSpan.Zero,
            },
            shardCount: 1,
            factory: factory);

        return new ChainHarness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            Leaves = leaves,
            EntryReadCount = () => Volatile.Read(ref entryReads),
            KeyReadCount = () => Volatile.Read(ref keyReads),
        };
    }

    [Test]
    public async Task Entries_page_stops_reading_leaves_once_the_work_budget_is_spent()
    {
        // 200 leaves, 1 entry each, page size 100: without a work bound the
        // call would read 100 leaves to fill one page.
        var harness = CreateChain(leafCount: 200, entriesPerLeaf: 1, maxLeavesPerScanPage: 8);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.EntryReadCount(), Is.LessThanOrEqualTo(8),
                "the walk must stop once the leaf budget is spent");
            Assert.That(page.Entries, Has.Count.EqualTo(8));
            Assert.That(page.HasMore, Is.True,
                "a work-bounded page must report that more is available");
        });
    }

    [Test]
    public async Task Keys_page_stops_reading_leaves_once_the_work_budget_is_spent()
    {
        var harness = CreateChain(leafCount: 200, entriesPerLeaf: 1, maxLeavesPerScanPage: 5);

        var page = await harness.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.KeyReadCount(), Is.LessThanOrEqualTo(5));
            Assert.That(page.Keys, Has.Count.EqualTo(5));
            Assert.That(page.HasMore, Is.True);
        });
    }

    /// <summary>
    /// The forward-progress invariant at the grain seam, in the case where the
    /// walk cannot name a resume position. These leaves declare no bounds, so
    /// the only thing a caller could resume from is the last key in the page -
    /// which means a work-bounded page must never come back empty while
    /// claiming more is available.
    /// </summary>
    [Test]
    public async Task A_work_bounded_page_is_never_empty_while_claiming_more()
    {
        // A budget of 1 leaf against leaves that yield nothing until the very
        // last one: the bound must not fire while the page is still empty.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        const int leafCount = 12;
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        for (var i = 0; i < leafCount; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            // Only the final leaf yields anything; every earlier leaf is a
            // sterile visit (the shape a tombstoned or moved-away run creates).
            var entries = i == leafCount - 1
                ? new List<KeyValuePair<string, byte[]>> { new("z", new byte[] { 1 }) }
                : new List<KeyValuePair<string, byte[]>>();

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(entries.ToList()));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = null,
                HighKeyExclusive = null,
            }));
            var next = i + 1 < leafCount ? (GrainId?)ids[i + 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 1,
                MaxScanPageDuration = TimeSpan.Zero,
            },
            shardCount: 1,
            factory: factory);

        var grain = new ShardRootGrain(context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        var page = await grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null);

        Assert.That(page.Entries, Is.Not.Empty,
            "an empty page claiming HasMore would strand a caller that resumes " +
            "from the last returned key");
    }

    /// <summary>
    /// Bounding the work must not lose data: paging all the way through a
    /// work-bounded scan yields exactly the same keys, in the same order, as
    /// the chain holds.
    /// </summary>
    [Test]
    public async Task Paging_through_a_work_bounded_scan_loses_no_entries_and_preserves_order()
    {
        const int leafCount = 40;
        const int entriesPerLeaf = 3;
        var harness = CreateChain(leafCount, entriesPerLeaf, maxLeavesPerScanPage: 3);

        var seen = new List<string>();
        string? continuation = null;
        var guard = 0;

        while (guard++ < 1000)
        {
            var page = await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 7,
                continuationToken: continuation);

            seen.AddRange(page.Entries.Select(e => e.Key));
            if (!page.HasMore)
                break;

            Assert.That(page.Entries, Is.Not.Empty,
                "a page claiming more must carry at least one entry so the " +
                "continuation can advance");
            continuation = page.Entries[^1].Key;
        }

        var expected = new List<string>();
        for (var i = 0; i < leafCount; i++)
            for (var j = 0; j < entriesPerLeaf; j++)
                expected.Add($"k{i:D4}-{j:D4}");

        Assert.Multiple(() =>
        {
            Assert.That(guard, Is.LessThan(1000), "the scan must terminate");
            Assert.That(seen, Is.Unique, "no key may be yielded twice");
            Assert.That(seen, Is.EqualTo(expected),
                "every key must be yielded exactly once, in ascending order");
        });
    }

    [Test]
    public async Task A_disabled_budget_restores_the_unbounded_walk()
    {
        var harness = CreateChain(leafCount: 30, entriesPerLeaf: 1, maxLeavesPerScanPage: 0);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 25, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(25),
                "with the bound disabled the page fills as it always did");
            Assert.That(harness.EntryReadCount(), Is.EqualTo(25));
        });
    }
}
