using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 1992: the scan work bound introduced by issue
/// 1955 was disarmed on a <em>sterile run</em> - a stretch of leaves whose rows
/// are all filtered out inside the leaf (tombstoned, TTL-expired, predicate- or
/// slot-rejected) or by the shard's moved-away filter. Because the bound could
/// only fire once the page held at least one result, a sterile run walked the
/// sibling chain without limit inside a single non-reentrant shard call, and
/// head-of-line-blocked the whole partition for as long as it took.
/// <para>
/// The fix separates the two questions the old signature conflated: the budget
/// answers only "is the work spent?", and the call site decides whether it may
/// act on that by asking whether it can name a resume position. The visited
/// leaf's boundary key is that position, so a bounded page can now come back
/// empty and still be resumable - which is what these tests pin.
/// </para>
/// <para>
/// The sibling fixture <see cref="ShardRootGrainScanWorkBoundTests"/> covers the
/// complementary case: leaves that declare no bounds, where no resume position
/// exists and the walk must still never emit an unresumable empty page.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainSterileScanTests
{
    private const string TreeId = "sterile-scan-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>Virtual shard count used by the slot-filtered variants.</summary>
    private const int VirtualShardCount = 4;

    private static readonly int[] AllSlots = [0, 1, 2, 3];

    private sealed class ChainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required Func<int> LeafReadCount { get; init; }

        /// <summary>Every key the chain holds, in ascending ordinal order.</summary>
        public required IReadOnlyList<string> AllKeys { get; init; }

        /// <summary>The keys a scan can actually observe (the live tail).</summary>
        public required IReadOnlyList<string> LiveKeys { get; init; }
    }

    private static string LeafBound(int leafIndex) => $"k{leafIndex:D4}";

    private static string KeyIn(int leafIndex, int slot) => $"k{leafIndex:D4}-{slot:D4}";

    /// <summary>
    /// Builds a two-level tree (one internal root over <paramref name="leafCount"/>
    /// leaves) where every leaf declares real key bounds, so the walk has a leaf
    /// boundary to resume from.
    /// <para>
    /// Leaves outside [<paramref name="liveFrom"/>, <paramref name="liveTo"/>) are
    /// <em>sterile</em>: they hold keys and declare bounds, but return nothing to
    /// the shard - the shape a run of tombstoned, expired, or filtered rows
    /// creates. A real leaf applies those filters internally, so the shard cannot
    /// tell a sterile leaf from an empty range and has no returned key to resume
    /// from.
    /// </para>
    /// </summary>
    private static ChainHarness CreateChain(
        int leafCount,
        int keysPerLeaf,
        int maxLeavesPerScanPage,
        int liveFrom,
        int liveTo)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var internalId = GrainId.Create("internal", "root");
        var leafIds = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            leafIds[i] = GrainId.Create("leaf", $"leaf{i}");

        state.State.RootNodeId = internalId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var leafReads = 0;
        var allKeys = new List<string>();
        var liveKeys = new List<string>();

        for (var i = 0; i < leafCount; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var keys = new List<string>();
            for (var j = 0; j < keysPerLeaf; j++)
                keys.Add(KeyIn(i, j));
            allKeys.AddRange(keys);

            var sterile = i < liveFrom || i >= liveTo;
            if (!sterile)
                liveKeys.AddRange(keys);

            // A sterile leaf answers every read with nothing, exactly as a leaf
            // whose rows are all tombstoned or predicate-rejected does.
            var visible = sterile ? [] : keys;

            List<string> Filter(NSubstitute.Core.CallInfo call)
            {
                Interlocked.Increment(ref leafReads);
                var startInclusive = call.ArgAt<string?>(0);
                var endExclusive = call.ArgAt<string?>(1);
                var afterExclusive = call.ArgAt<string?>(2);
                var beforeExclusive = call.ArgAt<string?>(3);
                return visible.Where(k =>
                    (startInclusive is null || string.CompareOrdinal(k, startInclusive) >= 0) &&
                    (endExclusive is null || string.CompareOrdinal(k, endExclusive) < 0) &&
                    (afterExclusive is null || string.CompareOrdinal(k, afterExclusive) > 0) &&
                    (beforeExclusive is null || string.CompareOrdinal(k, beforeExclusive) < 0))
                    .ToList();
            }

            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call => Task.FromResult(Filter(call)));
            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call => Task.FromResult(
                    Filter(call)
                        .Select(k => new KeyValuePair<string, byte[]>(k, [1]))
                        .ToList()));

            // Real bounds: the boundary the fix resumes from. The outermost
            // edges are open, as they are in a real tree.
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = i == 0 ? null : LeafBound(i),
                HighKeyExclusive = i == leafCount - 1 ? null : LeafBound(i + 1),
            }));

            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                i + 1 < leafCount ? (GrainId?)leafIds[i + 1] : null));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult(
                i - 1 >= 0 ? (GrainId?)leafIds[i - 1] : null));

            factory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
        }

        var separators = new string?[leafCount];
        for (var i = 0; i < leafCount; i++)
            separators[i] = i == 0 ? null : LeafBound(i);

        var internalGrain = Substitute.For<IBPlusInternalGrain>();
        internalGrain.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
        {
            SeparatorKeys = separators,
            ChildIds = leafIds,
            ChildrenAreLeaves = true,
        }));
        factory.GetGrain<IBPlusInternalGrain>(internalId).Returns(internalGrain);

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
            LeafReadCount = () => Volatile.Read(ref leafReads),
            AllKeys = allKeys,
            LiveKeys = liveKeys,
        };
    }

    /// <summary>A chain that is sterile everywhere except its final leaf.</summary>
    private static ChainHarness CreateSterileChain(int leafCount, int maxLeavesPerScanPage)
        => CreateChain(leafCount, keysPerLeaf: 3, maxLeavesPerScanPage,
            liveFrom: leafCount - 1, liveTo: leafCount);

    // ======================================================================
    //  The defect itself: the bound must fire even with nothing collected.
    // ======================================================================

    [Test]
    public async Task A_sterile_forward_entry_run_stops_at_the_work_bound()
    {
        var harness = CreateSterileChain(leafCount: 200, maxLeavesPerScanPage: 8);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.LeafReadCount(), Is.LessThanOrEqualTo(8),
                "before the fix a sterile run walked the chain without limit " +
                "inside one non-reentrant shard call (issue 1992)");
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.HasMore, Is.True);
            Assert.That(page.ResumeFromKey, Is.EqualTo(LeafBound(8)),
                "the page resumes from the boundary past the last leaf it walked");
        });
    }

    [Test]
    public async Task A_sterile_forward_key_run_stops_at_the_work_bound()
    {
        var harness = CreateSterileChain(leafCount: 200, maxLeavesPerScanPage: 5);

        var page = await harness.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.LeafReadCount(), Is.LessThanOrEqualTo(5));
            Assert.That(page.Keys, Is.Empty);
            Assert.That(page.HasMore, Is.True);
            Assert.That(page.ResumeFromKey, Is.EqualTo(LeafBound(5)));
        });
    }

    [Test]
    public async Task A_sterile_reverse_entry_run_stops_at_the_work_bound()
    {
        // Sterile from the right: only the first leaf is live, so a reverse
        // walk crosses the whole sterile tail before it can collect anything.
        var harness = CreateChain(leafCount: 200, keysPerLeaf: 3,
            maxLeavesPerScanPage: 6, liveFrom: 0, liveTo: 1);

        var page = await harness.Grain.GetSortedEntriesBatchReverseAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.LeafReadCount(), Is.LessThanOrEqualTo(6));
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.HasMore, Is.True);
            // Reverse starts at leaf 199 and walks left; after 6 leaves the
            // last visited is leaf 194, whose inclusive low bound is the
            // exclusive ceiling the next page resumes below.
            Assert.That(page.ResumeFromKey, Is.EqualTo(LeafBound(194)));
        });
    }

    [Test]
    public async Task A_sterile_reverse_key_run_stops_at_the_work_bound()
    {
        var harness = CreateChain(leafCount: 200, keysPerLeaf: 3,
            maxLeavesPerScanPage: 4, liveFrom: 0, liveTo: 1);

        var page = await harness.Grain.GetSortedKeysBatchReverseAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.LeafReadCount(), Is.LessThanOrEqualTo(4));
            Assert.That(page.Keys, Is.Empty);
            Assert.That(page.HasMore, Is.True);
            Assert.That(page.ResumeFromKey, Is.EqualTo(LeafBound(196)));
        });
    }

    [Test]
    public async Task A_sterile_slot_filtered_key_run_stops_at_the_work_bound()
    {
        var harness = CreateSterileChain(leafCount: 200, maxLeavesPerScanPage: 7);

        var page = await harness.Grain.GetSortedKeysBatchForSlotsAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null,
            sortedSlots: AllSlots, virtualShardCount: VirtualShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(harness.LeafReadCount(), Is.LessThanOrEqualTo(7));
            Assert.That(page.Keys, Is.Empty);
            Assert.That(page.HasMore, Is.True);
            Assert.That(page.ResumeFromKey, Is.EqualTo(LeafBound(7)));
        });
    }

    [Test]
    public async Task A_sterile_slot_filtered_entry_run_stops_at_the_work_bound()
    {
        var harness = CreateSterileChain(leafCount: 200, maxLeavesPerScanPage: 3);

        var page = await harness.Grain.GetSortedEntriesBatchForSlotsAsync(
            startInclusive: null, endExclusive: null, pageSize: 100, continuationToken: null,
            sortedSlots: AllSlots, virtualShardCount: VirtualShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(harness.LeafReadCount(), Is.LessThanOrEqualTo(3));
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.HasMore, Is.True);
            Assert.That(page.ResumeFromKey, Is.EqualTo(LeafBound(3)));
        });
    }

    // ======================================================================
    //  The resume key must actually be usable: no loss, no loop, no re-walk.
    // ======================================================================

    [Test]
    public async Task Paging_a_sterile_forward_scan_terminates_and_yields_the_live_tail()
    {
        const int leafCount = 60;
        var harness = CreateChain(leafCount, keysPerLeaf: 3, maxLeavesPerScanPage: 4,
            liveFrom: leafCount - 2, liveTo: leafCount);

        var seen = new List<string>();
        string? continuation = null;
        string? resumeFrom = null;
        var pages = 0;

        while (pages++ < 500)
        {
            var page = await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 4,
                continuationToken: continuation, resumeFromKey: resumeFrom);

            seen.AddRange(page.Entries.Select(e => e.Key));
            if (!page.HasMore)
                break;

            // The caller contract: prefer the leaf boundary, fall back to the
            // last row. One of the two must be present or the scan is stranded.
            if (page.ResumeFromKey is not null)
            {
                resumeFrom = page.ResumeFromKey;
            }
            else
            {
                Assert.That(page.Entries, Is.Not.Empty,
                    "a page claiming more with no resume boundary must carry a row");
                continuation = page.Entries[^1].Key;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(pages, Is.LessThan(500), "the scan must terminate");
            Assert.That(seen, Is.Unique, "no key may be yielded twice");
            Assert.That(seen, Is.EqualTo(harness.LiveKeys),
                "every live key must be yielded exactly once, in ascending order");
        });
    }

    [Test]
    public async Task Paging_a_sterile_reverse_scan_terminates_and_yields_the_live_head()
    {
        const int leafCount = 60;
        var harness = CreateChain(leafCount, keysPerLeaf: 3, maxLeavesPerScanPage: 4,
            liveFrom: 0, liveTo: 2);

        var seen = new List<string>();
        string? continuation = null;
        string? resumeFrom = null;
        var pages = 0;

        while (pages++ < 500)
        {
            var page = await harness.Grain.GetSortedKeysBatchReverseAsync(
                startInclusive: null, endExclusive: null, pageSize: 4,
                continuationToken: continuation, resumeFromKey: resumeFrom);

            seen.AddRange(page.Keys);
            if (!page.HasMore)
                break;

            if (page.ResumeFromKey is not null)
            {
                resumeFrom = page.ResumeFromKey;
            }
            else
            {
                Assert.That(page.Keys, Is.Not.Empty,
                    "a page claiming more with no resume boundary must carry a row");
                continuation = page.Keys[^1];
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(pages, Is.LessThan(500), "the scan must terminate");
            Assert.That(seen, Is.Unique, "no key may be yielded twice");
            Assert.That(seen, Is.EqualTo(harness.LiveKeys.Reverse()),
                "every live key must be yielded exactly once, in descending order");
        });
    }

    /// <summary>
    /// The resume key is a leaf boundary strictly past the leaves the bounded
    /// call already walked, so a resumed page starts where the previous one
    /// stopped instead of re-reading the sterile prefix. Without that, paging a
    /// long sterile run would be quadratic in the number of leaves.
    /// </summary>
    [Test]
    public async Task Each_forward_page_resumes_strictly_beyond_the_leaves_it_walked()
    {
        const int leafCount = 40;
        const int budgetLeaves = 4;
        var harness = CreateChain(leafCount, keysPerLeaf: 1, maxLeavesPerScanPage: budgetLeaves,
            liveFrom: leafCount, liveTo: leafCount);

        string? resumeFrom = null;
        var boundaries = new List<string>();
        var pages = 0;

        while (pages++ < 100)
        {
            var page = await harness.Grain.GetSortedKeysBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10,
                continuationToken: null, predicate: null, resumeFromKey: resumeFrom);

            if (!page.HasMore)
                break;

            Assert.That(page.ResumeFromKey, Is.Not.Null,
                "a fully sterile bounded page must carry a resume boundary");
            if (resumeFrom is not null)
            {
                Assert.That(string.CompareOrdinal(page.ResumeFromKey, resumeFrom),
                    Is.GreaterThan(0),
                    "each page must advance strictly past the previous boundary");
            }
            boundaries.Add(page.ResumeFromKey!);
            resumeFrom = page.ResumeFromKey;
        }

        Assert.Multiple(() =>
        {
            Assert.That(pages, Is.LessThan(100), "the scan must terminate");
            Assert.That(boundaries, Is.Ordered.Using<string>(StringComparer.Ordinal));
            // A whole-chain sterile sweep at N leaves per page costs one read
            // per leaf, not one per leaf per page: the resume boundary skips
            // what the previous page walked.
            Assert.That(harness.LeafReadCount(),
                Is.LessThanOrEqualTo(leafCount + (leafCount / budgetLeaves) + 2),
                "a resumed page must not re-walk the sterile prefix");
        });
    }

    [Test]
    public async Task Each_reverse_page_resumes_strictly_below_the_leaves_it_walked()
    {
        const int leafCount = 40;
        var harness = CreateChain(leafCount, keysPerLeaf: 1, maxLeavesPerScanPage: 4,
            liveFrom: leafCount, liveTo: leafCount);

        string? resumeFrom = null;
        var pages = 0;

        while (pages++ < 100)
        {
            var page = await harness.Grain.GetSortedKeysBatchReverseAsync(
                startInclusive: null, endExclusive: null, pageSize: 10,
                continuationToken: null, predicate: null, resumeFromKey: resumeFrom);

            if (!page.HasMore)
                break;

            Assert.That(page.ResumeFromKey, Is.Not.Null,
                "a fully sterile bounded reverse page must carry a resume boundary");
            if (resumeFrom is not null)
            {
                Assert.That(string.CompareOrdinal(page.ResumeFromKey, resumeFrom),
                    Is.LessThan(0),
                    "each reverse page must advance strictly below the previous boundary");
            }
            resumeFrom = page.ResumeFromKey;
        }

        Assert.That(pages, Is.LessThan(100), "the reverse scan must terminate");
    }

    /// <summary>
    /// A resume boundary must not skip live rows that sit exactly on it. The
    /// forward boundary is a leaf's exclusive high key, which is the next
    /// leaf's <em>inclusive</em> low key - so it is applied inclusively, not as
    /// a continuation token (which the leaf would treat as exclusive).
    /// </summary>
    [Test]
    public async Task A_forward_resume_boundary_keeps_a_key_that_sits_on_it()
    {
        // Leaf 3 is live and its first key is exactly at the boundary that
        // leaf 2 hands back.
        var harness = CreateChain(leafCount: 8, keysPerLeaf: 3, maxLeavesPerScanPage: 3,
            liveFrom: 3, liveTo: 8);
        var boundary = LeafBound(3);

        var page = await harness.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100,
            continuationToken: null, predicate: null, resumeFromKey: boundary);

        Assert.Multiple(() =>
        {
            Assert.That(page.Keys, Is.Not.Empty);
            Assert.That(page.Keys[0], Is.EqualTo(KeyIn(3, 0)),
                "the boundary is an inclusive lower bound, so the first key of " +
                "the leaf it names must survive it");
        });
    }

    /// <summary>
    /// A sterile run inside a bounded range still terminates on the range, not
    /// on the budget, once the walk leaves the range.
    /// </summary>
    [Test]
    public async Task A_bounded_range_over_a_sterile_run_still_terminates_on_the_range()
    {
        var harness = CreateChain(leafCount: 40, keysPerLeaf: 3, maxLeavesPerScanPage: 2,
            liveFrom: 40, liveTo: 40);

        string? resumeFrom = null;
        var pages = 0;
        var sawEnd = false;

        while (pages++ < 100)
        {
            var page = await harness.Grain.GetSortedKeysBatchAsync(
                startInclusive: null, endExclusive: LeafBound(6), pageSize: 10,
                continuationToken: null, predicate: null, resumeFromKey: resumeFrom);

            Assert.That(page.Keys, Is.Empty, "every leaf in range is sterile");
            if (!page.HasMore)
            {
                sawEnd = true;
                break;
            }
            resumeFrom = page.ResumeFromKey;
            Assert.That(resumeFrom, Is.Not.Null);
            Assert.That(string.CompareOrdinal(resumeFrom, LeafBound(6)), Is.LessThanOrEqualTo(0),
                "the walk must not resume past the end of the requested range");
        }

        Assert.That(sawEnd, Is.True, "the range must terminate the scan");
    }
}
