using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Attribution for a forfeited leaf-division fast path (issue #2787).
/// <para>
/// A leaf divides cheaply only while its lazily hydrated snapshot frame is
/// still attached. Detaching is irreversible for the life of the activation, so
/// the first whole-cache operation to run forfeits the fast path for every
/// later one - including a division, which then has to materialise the whole
/// leaf to place a cut. The split seam cannot see any of this: it observes a
/// bare <see langword="false"/> from the bisect, identical whether the leaf
/// never had a frame or an unrelated operation consumed it.
/// </para>
/// <para>
/// Those two cases have opposite costs. A leaf replayed from the write-ahead log
/// never attaches a frame and its rows are already resident, so the fallback is
/// free; a leaf whose frame was consumed pays for the whole leaf, resident and
/// unsheddable. The fixtures below pin that the pair
/// (refusal reason, detaching seam) separates them, because neither value does
/// so alone.
/// </para>
/// </summary>
public sealed class LeafSnapshotDetachAttributionTests
{
    private static LeafEntryCache NewCache()
        => new(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));

    private static string Key(int i) => $"k{i:D5}";

    private static LeafSnapshotRow[] Corpus(int rowCount)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = new LeafSnapshotRow(
                Key(i),
                new LwwValue<byte[]>
                {
                    Value = new byte[64],
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                });
        }

        return rows;
    }

    private static LeafEntryCache CacheWithFrame(int rowCount, long budgetBytes = 0L)
    {
        var cache = NewCache();
        Assert.That(
            cache.TryAttachSnapshot(LeafSnapshotCodec.Encode(Corpus(rowCount)), budgetBytes),
            Is.True,
            "the frame must attach, or the fixture measures a cache that never had a fast path");
        return cache;
    }

    // ---------------------------------------------------------------
    // The benign case: no frame was ever attached.
    // ---------------------------------------------------------------

    [Test]
    public void A_cache_that_never_attached_a_frame_reports_no_detaching_seam()
    {
        var cache = NewCache();

        // Runs the accessor that detaches on a frame-backed cache. Nothing is
        // forfeited here because there was nothing to forfeit.
        _ = cache.EnumerateRows().ToList();

        Assert.That(
            cache.LastDetachSeam,
            Is.EqualTo(LeafSnapshotDetachSeam.None),
            "a cache with no frame must not report a detach - otherwise a leaf replayed from the "
            + "write-ahead log is indistinguishable from one whose frame was consumed, and only "
            + "the second is a defect");
    }

    [Test]
    public void A_refusal_with_no_frame_ever_attached_pairs_the_reason_with_no_seam()
    {
        var cache = NewCache();

        Assert.Multiple(() =>
        {
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.False);
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached));
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "reason alone cannot classify this refusal; the seam is what makes it benign");
        });
    }

    // ---------------------------------------------------------------
    // The harmful case: a frame existed and something else consumed it.
    // ---------------------------------------------------------------

    [Test]
    public void A_refusal_after_a_whole_cache_accessor_pairs_the_same_reason_with_the_guilty_seam()
    {
        var cache = CacheWithFrame(rowCount: 64);

        Assert.That(
            cache.TryGetBisectingKeyWithoutHydrating(out _, out var beforeReason),
            Is.True,
            "the frame is attached, so the fast path must be available before anything consumes it");
        Assert.That(beforeReason, Is.EqualTo(LeafBisectRefusalReason.None));

        _ = cache.Keys.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var afterReason),
                Is.False);
            Assert.That(
                afterReason,
                Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached),
                "the refusal reason is identical to the never-attached case, which is exactly why "
                + "the reason on its own is not a usable instrument");
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor),
                "the seam is the only signal that separates this expensive refusal from the free one");
        });
    }

    [Test]
    public void Each_whole_cache_accessor_records_itself_as_the_detaching_seam()
    {
        var viaKeys = CacheWithFrame(rowCount: 16);
        _ = viaKeys.Keys.ToList();

        var viaRows = CacheWithFrame(rowCount: 16);
        _ = viaRows.EnumerateRows().ToList();

        var viaUnderlying = CacheWithFrame(rowCount: 16);
        _ = viaUnderlying.UnderlyingRows;

        Assert.Multiple(() =>
        {
            Assert.That(viaKeys.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor));
            Assert.That(viaRows.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor));
            Assert.That(viaUnderlying.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor));
        });
    }

    [Test]
    public void Clearing_a_frame_backed_cache_records_the_clear_seam()
    {
        var cache = CacheWithFrame(rowCount: 16);

        cache.Clear();

        Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.Clear));
    }

    // ---------------------------------------------------------------
    // The bounded route must NOT detach, which is the property the fix relies on.
    // ---------------------------------------------------------------

    [Test]
    public void A_bounded_window_fold_leaves_the_frame_attached_and_the_fast_path_available()
    {
        var cache = CacheWithFrame(rowCount: 512, budgetBytes: 4L * 1024);

        var visited = 0;
        foreach (var (startInclusive, endExclusive) in cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var _ in cache.EnumerateRange(startInclusive, endExclusive))
            {
                visited++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(512), "the fold must still visit every row exactly once");
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "a bounded fold must leave the frame attached; if this reddens, every caller "
                + "migrated to the fold is silently still forfeiting the division fast path");
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.True,
                "the leaf must still be able to place a cut after a full bounded scan");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
        });
    }

    [Test]
    public void A_whole_cache_scan_of_the_same_corpus_forfeits_what_the_fold_preserves()
    {
        // The mirror of the arm above, on an identical corpus. Its value is not
        // that it reddens - it is that it stays green and shows the difference
        // is the accessor, not the corpus.
        var cache = CacheWithFrame(rowCount: 512, budgetBytes: 4L * 1024);

        var visited = cache.EnumerateRows().Count();

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(512), "both routes must visit the same rows");
            Assert.That(
                cache.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor));
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out _),
                Is.False,
                "the whole-cache route forfeits the fast path - this is the behaviour the "
                + "migrated callers exist to stop");
        });
    }

    // ---------------------------------------------------------------
    // Frame-shape refusals, which are not size related.
    // ---------------------------------------------------------------

    [Test]
    public void A_single_row_frame_refuses_because_it_has_no_interior_pivot()
    {
        var cache = CacheWithFrame(rowCount: 1);

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason), Is.False);
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.TooFewRows));
        });
    }

    [Test]
    public void A_successful_bisect_reports_no_refusal_reason_and_leaves_the_frame_attached()
    {
        var cache = CacheWithFrame(rowCount: 64);

        Assert.Multiple(() =>
        {
            Assert.That(cache.TryGetBisectingKeyWithoutHydrating(out var key, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
            Assert.That(key, Is.EqualTo(Key(32)));
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
        });
    }

    [Test]
    public void The_reason_reporting_overload_agrees_with_the_bare_one()
    {
        // Why this still matters, restated for issue #2865. The original
        // justification said production called the bare overload and that
        // divergence would make "the instrument measure a different decision
        // from the one the split path actually takes". That second clause is
        // false: since #2837 the split path calls only the reason-reporting
        // overload, so no divergence can reach the instrument through the bare
        // one. The clause that IS still true is the fixture one. Eight
        // assertions across four fixtures pin leaf-division behaviour through
        // the bare form, so if it stopped being a pure delegation those
        // fixtures would silently assert about a different decision from the
        // one production takes - green, and measuring the wrong thing. The
        // overload is therefore convenience for tests, not dead code, and this
        // arm is what keeps the convenience honest.
        var attached = CacheWithFrame(rowCount: 64);
        var detached = NewCache();

        Assert.Multiple(() =>
        {
            Assert.That(
                attached.TryGetBisectingKeyWithoutHydrating(out var bareKey),
                Is.EqualTo(attached.TryGetBisectingKeyWithoutHydrating(out var reasonKey, out _)));
            Assert.That(bareKey, Is.EqualTo(reasonKey));
            Assert.That(
                detached.TryGetBisectingKeyWithoutHydrating(out _),
                Is.EqualTo(detached.TryGetBisectingKeyWithoutHydrating(out _, out _)));
        });
    }

    // ---------------------------------------------------------------
    // Two-sided proof at the real split seam.
    //
    // Everything above runs against the cache in isolation. These two arms run
    // against an actual grain, and they exist because an instrument that can
    // only ever report one answer is exactly as vacuous as the tests it was
    // built to replace - and would not look it. A constant proves nothing
    // unless the assertion references something other than that constant, so
    // the same instrument is asserted to report BOTH answers on two leaves that
    // differ only in whether their frame survived. In each case the frame's
    // state is established WITHOUT the instrument, from the resident row count,
    // so neither arm is satisfiable by an instrument stuck on one value.
    // ---------------------------------------------------------------

    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(int rowCount, long residentBudgetBytes = 4L * 1024)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 25L,
                EncodedRows = LeafSnapshotCodec.Encode(Corpus(rowCount)),
                SnapshotOffsetsByPartition = [25L],
            }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-attribution";
        state.State.ShardIndex = 0;

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions
                {
                    WalPartitions = 1,
                    LeafPartialHydrationEnabled = true,
                    LeafHydrationResidentBytes = residentBudgetBytes,
                },
                maxLeafKeys: 1_000_000,
                shardCount: 1,
                factory: grainFactory),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        Assert.That(
            await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "the leaf must come online from its snapshot without decoding it");
        return grain;
    }

    [Test]
    public async Task A_leaf_whose_frame_survived_reports_the_fast_path_was_available()
    {
        var grain = await RehydratedLeafAsync(512);

        // The positive side. The frame is provably attached, established
        // without the instrument: no row has been materialised, which only an
        // attached frame permits.
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "precondition: nothing may be resident, or the frame has already gone");

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out var key, out var reason),
                Is.True,
                "a leaf with an attached frame must place its cut from the frame alone");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
            Assert.That(key, Is.Not.Null.And.Not.Empty);
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None));
        });
    }

    [Test]
    public async Task A_leaf_whose_frame_was_consumed_reports_the_fast_path_was_forfeited()
    {
        var grain = await RehydratedLeafAsync(512);

        // The negative side, seeded deliberately through EntriesForTest.
        //
        // That accessor is the trap this instrument exists to close: it routes
        // through UnderlyingRows -> HydrateAll -> DetachSnapshot, so a test that
        // touches it has forfeited the frame before asserting anything. Which is
        // precisely what makes it the most reliable way to construct a
        // known-detached leaf - the detach is guaranteed by source, not by the
        // instrument under test. Do not "clean this up" to a gentler accessor:
        // the guarantee is the entire point of using it here.
        Assert.That(grain.EntriesForTest, Is.Not.Empty, "the accessor must materialise the leaf");

        // Again established independently of the instrument: every row is now
        // resident, which only a full hydration can achieve.
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.EqualTo(512),
            "precondition: the whole leaf must be resident, proving the frame was released");

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.False,
                "with the frame gone the leaf cannot pivot without materialising itself");
            Assert.That(
                reason,
                Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached),
                "this is refusal reason 1 - the one an unconverted seam produces");
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor),
                "and the seam must name EntriesForTest's route, not the division itself");
        });
    }

    // ---------------------------------------------------------------
    // Issue #2843: completing a ranged hydration must NOT forfeit the bisect.
    //
    // This is the third distinct outcome, and the one the epic had been
    // conflating with the first two. The bounded conversions merged upstream
    // stopped the frame being consumed through a whole-cache accessor, but
    // completing a ranged hydration - which happens legitimately whenever the
    // budget covers the leaf - still released the frame, so every division that
    // followed forfeited its pivot.
    //
    // The two arms below are the two-sided proof. They run on an IDENTICAL
    // corpus and an identical generous budget, so the only thing that differs
    // between them is the accessor that completes the leaf: the positive arm
    // completes it through the ranged read seam and the frame is RETAINED
    // (seam None); the contrast arm completes the same leaf through a
    // whole-cache accessor and the frame is still DETACHED (seam
    // UnderlyingRowsAccessor). Pinning both proves the retention is specific to
    // the ranged seam and that the fix is surgical - it does not over-retain on
    // the whole-cache accessors, whose transient whole-leaf materialisation is
    // the separately-tracked #2842 memory harm. The positive arm is the
    // non-vacuous guard (the negative control reverts the source fix and it
    // reddens); the contrast arm is a control whose job is to stay green and
    // show the difference is the accessor, not the corpus. Each arm establishes
    // its frame state WITHOUT the instrument under test, from the resident row
    // count, so neither is satisfiable by an instrument stuck on one value.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_completed_ranged_hydration_retains_the_frame_so_a_division_can_still_bisect()
    {
        // Budget generous enough that a single unbounded window covers the whole
        // leaf, so the ranged walk completes the source. Before #2843 that
        // detached the frame (LastDetachSeam == RangeHydrationCompleted), and
        // the bisect below refused with NoSnapshotAttached.
        var grain = await RehydratedLeafAsync(512, residentBudgetBytes: 64L * 1024 * 1024);

        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "precondition: nothing may be resident before the walk, or the frame has already gone");

        // Drive a completing ranged hydration through the real read seam.
        // CountAsync resolves to an unbounded EnumerateRange, which within this
        // budget is a single protected window covering every block.
        _ = await grain.CountAsync();

        // Non-vacuity, established WITHOUT the property under test: the ranged
        // walk actually ran to completion. Every row is resident, which only a
        // completed hydration achieves; a seam that short-circuited would leave
        // this below the row count and fail here first. This holds identically
        // whether or not the frame was retained, so it cannot mask the property.
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.EqualTo(512),
            "the ranged walk must have completed the source, or this arm proves nothing");

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.HasPendingHydration,
                Is.True,
                "a completed ranged hydration must retain the frame (issue #2843)");
            // Resident-bytes attribution (review point #1). ResidentFootprintBytes
            // is StateBytes plus the retained frame's length, so it strictly
            // exceeds StateBytes exactly when the frame is retained. Pre-#2843 the
            // completion detached the frame, so the footprint would have collapsed
            // to StateBytes with the decoded rows PINNED unsheddable (TrimToBudget
            // no-ops on a null source - that is the #2842 harm in miniature). The
            // retained frame is the compressed source the rows decode from, so it
            // is both the sheddability precondition (TrimToBudget can now evict the
            // decoded blocks back to the residual) and strictly smaller than a
            // second whole-leaf ordered-view buffer the forfeited fast path would
            // have built. This asserts the frame is genuinely resident-accounted,
            // not merely flagged.
            Assert.That(
                grain.CacheForTest.ResidentFootprintBytes,
                Is.GreaterThan(grain.CacheForTest.StateBytes),
                "the retained frame must be resident-accounted; pre-#2843 the footprint "
                + "would equal StateBytes with the frame detached and the rows pinned");
            // Constraint from the epic: None and RangeHydrationCompleted are
            // BOTH legitimate outcomes of a correct conversion in general - the
            // budget-to-leaf ratio decides which - so the ratio-independent
            // invariant is that the seam is none of the three whole-cache
            // accessors. This arm additionally controls the ratio (the budget
            // covers the whole leaf, so the ranged walk completes in one
            // protected window with no eviction), which is exactly what lets it
            // pin the stronger equality: post-#2843 the completion no longer
            // detaches, so RangeHydrationCompleted is never produced and the
            // seam is precisely None.
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.Not.EqualTo(LeafSnapshotDetachSeam.KeysAccessor)
                    .And.Not.EqualTo(LeafSnapshotDetachSeam.EnumerateRowsAccessor)
                    .And.Not.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor),
                "the ranged completion must not detach through any whole-cache accessor");
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "nothing detached; RangeHydrationCompleted is no longer produced, so the "
                + "seam that once marked this forfeiture is never recorded");
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out var key, out var reason),
                Is.True,
                "with the frame retained, the split path takes its pivot from the frame rather "
                + "than materialising the whole leaf through the fallback");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
            Assert.That(key, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task A_whole_cache_completion_of_the_same_leaf_still_detaches_and_forfeits_the_bisect()
    {
        // The contrast arm: the SAME corpus and the SAME generous budget as the
        // positive arm, so the only variable is the accessor. Completing the
        // leaf through a whole-cache accessor (EntriesForTest routes through
        // UnderlyingRows -> HydrateAll -> DetachSnapshot) must STILL detach and
        // forfeit the bisect. This is what makes the positive arm's "seam is
        // None" meaningful - it proves the instrument can report a non-None
        // seam on this exact corpus and budget - and it pins that the #2843 fix
        // did not leak retention onto the whole-cache accessors that carry the
        // separately-tracked #2842 memory harm.
        var grain = await RehydratedLeafAsync(512, residentBudgetBytes: 64L * 1024 * 1024);

        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "precondition: nothing may be resident before the accessor runs");

        // Complete the leaf through the whole-cache route.
        Assert.That(grain.EntriesForTest, Is.Not.Empty, "the accessor must materialise the leaf");

        // Non-vacuity, established WITHOUT the detach property: the whole-cache
        // walk read every row. Independent of whether the frame detached.
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.EqualTo(512),
            "the whole-cache accessor must have materialised the whole leaf");

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.CacheForTest.HasPendingHydration,
                Is.False,
                "a whole-cache completion still releases the frame - the #2843 fix is ranged-only");
            Assert.That(
                grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.UnderlyingRowsAccessor),
                "the seam names the whole-cache accessor, not a ranged completion");
            Assert.That(
                grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.False,
                "with the frame released the division forfeits its pivot, exactly as before the fix");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.NoSnapshotAttached));
        });
    }

    // ---------------------------------------------------------------
    // Issue #2843, acceptance concern: the fix must reach the OVERSIZED leaf,
    // not only the small ones. Production over the wedged corpus recorded 16
    // divisions of which exactly one completed and 15 threw; the one that
    // completed was small enough that the forfeited fallback (Cache.Keys ->
    // whole-leaf ordered view, the #2842 transient) was affordable, while the
    // single leaf holding 99.04% of the retained WAL was not. So a remedy is
    // only a remedy if the FIXED path's cost does not scale with leaf size -
    // otherwise it passes on the easy members and leaves the one leaf the epic
    // exists to divide untouched.
    //
    // This is the inverse of the whole-cache contrast arm above. There the SEAM
    // is the variable and size is held fixed; here SIZE is the variable and the
    // seam is held fixed (the frame is attached at both sizes). It pins that the
    // pivot the retained frame yields is read straight from the frame's ordinal
    // index - a single key decode - and materialises ZERO blocks at either size,
    // so it is exactly as affordable on the oversized leaf as on the small one.
    // Like the contrast arm it is green with or without the source fix by
    // construction (a freshly-rehydrated leaf never reaches the completion
    // detach line): its job is to make executable WHY retaining the frame - which
    // the positive arm proves the fix does after a ranged completion - is what
    // keeps the oversized leaf divisible. The positive arm is the guard; this is
    // its rationale.
    // ---------------------------------------------------------------

    [TestCase(512)]
    [TestCase(4096)]
    public async Task The_retained_frames_pivot_is_read_without_materialising_any_block_so_it_reaches_the_oversized_leaf(
        int rowCount)
    {
        var grain = await RehydratedLeafAsync(rowCount);

        // Established WITHOUT the property under test: the frame is attached and
        // the leaf is entirely unmaterialised, so any block materialised across
        // the bisect below is the bisect's own cost and nothing else.
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True, "precondition: the frame is attached");
        Assert.That(
            grain.CacheForTest.HydratedRowCount,
            Is.Zero,
            "precondition: nothing resident, so the post-bisect count is purely the bisect's cost");

        var placed = grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out var key, out var reason);

        Assert.Multiple(() =>
        {
            Assert.That(placed, Is.True, "an attached frame must place its cut at any leaf size");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
            // The pivot is the frame's MEDIAN key, read by ordinal at index
            // rowCount/2 - not the median of the resident set, which is empty.
            // Pinning the exact key proves the read went through the frame's
            // order rather than any materialised structure, and it is the same
            // derivation at both sizes.
            Assert.That(
                key,
                Is.EqualTo(Key(rowCount / 2)),
                "the pivot is the frame's median row, decoded by ordinal, not computed from resident rows");
            // The size-independence itself, and the whole point of this arm:
            // obtaining the pivot materialised NO block and trimmed none, at
            // BOTH 512 and 4096 rows. The forfeited fallback would instead have
            // materialised the whole leaf, whose cost scales with rowCount - so
            // this flat cost is exactly what lets the fix divide the oversized
            // leaf the small-leaf fallback cannot afford.
            Assert.That(
                grain.CacheForTest.HydratedRowCount,
                Is.Zero,
                "the pivot decoded a single frame key and materialised no block - its cost is flat "
                + "in leaf size, unlike the whole-leaf ordered-view fallback the frame's absence forces");
            Assert.That(
                grain.CacheForTest.EvictedBlockCount,
                Is.Zero,
                "and nothing was hydrated only to be trimmed");
            Assert.That(
                grain.CacheForTest.HasPendingHydration,
                Is.True,
                "the frame is still attached after the bisect - reading the pivot did not consume it");
        });
    }

    // ---------------------------------------------------------------
    // Issue #2865: the seam-to-tag projection must be TOTAL.
    //
    // RecordBisectRefusal projects LastDetachSeam onto the closed vocabulary
    // carried on detach_seam via BPlusLeafGrain.DetachSeamTag, whose switch
    // ends in `_ => "none"`. That default is not a harmless fallback: `none` is
    // the tag value meaning "no frame was ever attached", which is the BENIGN
    // reading this counter exists to separate from the harmful one. A seam
    // member with no arm is therefore not merely untagged - it is actively
    // mislabelled as the opposite case, on a counter whose documented reading
    // is "reason=no_snapshot_attached with detach_seam=none is benign". The
    // failure is silent: no series goes missing, one simply reports the wrong
    // thing, and an operator reads a forfeiture as a leaf that never had a
    // frame.
    //
    // This is exactly the shape #2865 removed a dead arm for, taken from the
    // other side. Removing a member that nothing can emit costs nothing;
    // ADDING one without a tag costs an operator a wrong verdict. The arms are
    // otherwise unguarded, because DetachSeamTag is private and every existing
    // fixture asserts on LeafSnapshotDetachSeam rather than on the projection.
    // ---------------------------------------------------------------

    [Test]
    public void Every_detach_seam_has_its_own_metric_tag()
    {
        var project = typeof(BPlusLeafGrain).GetMethod(
            "DetachSeamTag",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        // Loud on rename: a reflection probe that silently finds nothing would
        // pass this whole fixture vacuously, which is the defect it exists to
        // catch elsewhere.
        Assert.That(
            project,
            Is.Not.Null,
            "BPlusLeafGrain.DetachSeamTag was renamed or removed - this guard is now measuring "
            + "nothing and must be repointed rather than deleted");

        var seams = Enum.GetValues<LeafSnapshotDetachSeam>();
        Assert.That(
            seams, Has.Length.GreaterThan(1),
            "precondition: the enum must carry more than the None member, or the loop below is empty");

        var tags = seams.ToDictionary(
            seam => seam,
            seam => (string)project!.Invoke(null, [seam])!);

        Assert.Multiple(() =>
        {
            Assert.That(
                tags[LeafSnapshotDetachSeam.None],
                Is.EqualTo("none"),
                "the benign reading must keep its tag, or the documented pairing "
                + "'no_snapshot_attached with none is benign' names a value nothing emits");

            foreach (var (seam, tag) in tags.Where(kv => kv.Key != LeafSnapshotDetachSeam.None))
            {
                Assert.That(
                    tag,
                    Is.Not.EqualTo("none"),
                    $"{seam} falls through DetachSeamTag's default arm, so a real forfeiture through "
                    + "it would be reported as the benign 'no frame was ever attached' case");
            }

            Assert.That(
                tags.Values.Distinct().Count(),
                Is.EqualTo(tags.Count),
                "two seams share a tag, so the counter cannot attribute a forfeiture to the "
                + "surface that caused it");
        });
    }
}
