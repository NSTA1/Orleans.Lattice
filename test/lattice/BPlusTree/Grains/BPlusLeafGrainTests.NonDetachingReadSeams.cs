using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The read seams on <see cref="BPlusLeafGrain"/> must answer without detaching
/// the lazy hydration frame (issue #2368).
/// <para>
/// <c>LeafEntryCache.HydrateAll</c> ends in <c>DetachSnapshot</c>, which nulls
/// the frame permanently for the life of the activation. Because a leaf divides
/// cheaply only while the frame is attached - picking a pivot and planning
/// transfer batches from frame keys alone, without materialising a row - the
/// FIRST whole-cache accessor to run forfeits the cheap division for good, and
/// an oversized leaf can then never divide without becoming wholly resident.
/// </para>
/// <para>
/// Every assertion here is on the cache's own hydration state at the moment
/// after the operation, deliberately rather than on an outcome that detachment
/// would merely make slower. Nothing in this fixture touches
/// <c>EntriesForTest</c>: that property returns the live backing dictionary
/// through <c>UnderlyingRows</c>, so it detaches the frame BEFORE any assertion
/// could run, and a test written this way would exercise the detached fallback
/// while appearing to exercise the attached path - passing identically with and
/// without the fix.
/// </para>
/// <para>
/// The assertions use <c>LastDetachSeam</c> rather than a bare
/// <c>HasPendingHydration</c> boolean, because "the frame is gone" is not by
/// itself a defect. <see cref="LeafSnapshotDetachSeam.RangeHydrationCompleted"/>
/// records a release that a bounded walk paid for one window at a time, which is
/// benign; the four whole-cache accessors record a release that consumed the
/// frame in one go, which is the defect. Collapsing that distinction back to a
/// boolean would make each arm's verdict depend on the budget-to-leaf ratio
/// instead of on whether the seam is correct.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    // Small enough that the corpus spans several hydration windows, so
    // TrimToBudget can actually evict behind the walk. Mirrors the budget the
    // sibling eviction tests use (32 bytes per row over 256 rows). The ratio
    // matters: these arms are deliberately OVER budget, which is what lets them
    // take the strict None assertion below.
    private const long NonDetachingBudgetBytes = 32L * 256L;

    private static async Task<BPlusLeafGrain> WindowedLeafAsync()
        => await RehydratedLeafAsync(HydrationRows(), residentBudgetBytes: NonDetachingBudgetBytes);

    /// <summary>
    /// Asserts that the frame survived the operation and that nothing released
    /// it. Valid only on an arm whose leaf is larger than the resident budget,
    /// where eviction is guaranteed; on an arm that does not pin that ratio the
    /// honest assertion is that no whole-cache ACCESSOR detached, since
    /// <see cref="LeafSnapshotDetachSeam.RangeHydrationCompleted"/> is also a
    /// pass and which of the two occurs is decided by the ratio.
    /// </summary>
    private static void AssertFrameSurvived(BPlusLeafGrain grain, string because)
    {
        Assert.Multiple(() =>
        {
            Assert.That(grain.CacheForTest.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None), because);
            Assert.That(grain.CacheForTest.HasPendingHydration, Is.True, because);
        });
    }

    [Test]
    public async Task An_unbounded_count_does_not_detach_the_hydration_frame()
    {
        // The regression that motivated the clip. CountAsync() forwards
        // (null, null), so a single Cache.EnumerateRange over those bounds spans
        // every block; HydrateRange then protects the whole span in its own
        // TrimToBudget call, nothing is evictable, every block lands resident at
        // once, and HydrateBlock's IsFullyHydrated check detaches exactly as
        // HydrateAll would. Walking budget-sized windows is what keeps the frame.
        var grain = await WindowedLeafAsync();

        var count = await grain.CountAsync();

        AssertFrameSurvived(grain,
            "an unbounded count must not forfeit the cheap frame-only division");
        Assert.That(count, Is.GreaterThan(0));
    }

    [Test]
    public async Task An_unbounded_key_listing_does_not_detach_the_hydration_frame()
    {
        // GetKeysAsync defaults every bound to null, so it read as already
        // bounded while behaving exactly like the unbounded case above. It is
        // invisible to a HydrateAll / Keys / EnumerateRows / UnderlyingRows scan.
        var grain = await WindowedLeafAsync();

        var keys = await grain.GetKeysAsync();

        AssertFrameSurvived(grain,
            "an already-ranged seam called without bounds is still a detaching seam");
        Assert.That(keys, Is.Not.Empty);
    }

    [Test]
    public async Task An_unbounded_entry_listing_does_not_detach_the_hydration_frame()
    {
        var grain = await WindowedLeafAsync();

        var entries = await grain.GetEntriesAsync();

        AssertFrameSurvived(grain, "listing every entry must not consume the frame");
        Assert.That(entries, Is.Not.Empty);
    }

    [Test]
    public async Task Collecting_leaf_stats_does_not_detach_the_hydration_frame()
    {
        var grain = await WindowedLeafAsync();

        _ = await grain.GetStatsAsync();

        AssertFrameSurvived(grain, "a diagnostics read must not cost the division fast path");
    }

    [Test]
    public async Task Reading_every_live_entry_does_not_detach_the_hydration_frame()
    {
        // This one genuinely retains every live value, so its own peak is the
        // live set either way. What changes is that the copy belongs to the
        // caller and is released with it, rather than the CACHE staying wholly
        // resident for the rest of the activation.
        var grain = await WindowedLeafAsync();

        var live = await grain.GetLiveEntriesAsync();

        AssertFrameSurvived(grain, "the copy belongs to the caller, not to the cache");
        Assert.That(live, Is.Not.Empty);
    }

    [Test]
    public async Task Reading_every_raw_entry_does_not_detach_the_hydration_frame()
    {
        var grain = await WindowedLeafAsync();

        var all = await grain.GetAllRawEntriesAsync();

        AssertFrameSurvived(grain, "raw rows are copied out, so nothing stays resident");
        Assert.That(all, Is.Not.Empty);
    }

    [Test]
    public async Task A_leaf_smaller_than_the_hydration_budget_releases_the_frame_to_the_bounded_path()
    {
        // The adversarial case, asserted deliberately rather than avoided.
        //
        // Windowing does NOT preserve the frame because coverage is partial -
        // it preserves it because TrimToBudget evicts behind the walk, and
        // eviction calls MarkEvicted, which decrements the source's resident
        // block count. IsFullyHydrated is _hydratedBlocks == _blockCount, an
        // "all blocks resident AT ONCE" test rather than "all blocks ever
        // read", so a fold whose coverage is total still never completes the
        // source while the budget is smaller than the leaf.
        //
        // Where the budget EXCEEDS the leaf nothing is evictable, so the fold
        // materialises the final outstanding block and the frame is released.
        // The assertion is that the release is attributed to
        // RangeHydrationCompleted and NOT to a whole-cache accessor. That
        // distinction is the whole point: this arm is a pass because the rows
        // were paid for one bounded window at a time, which is exactly what a
        // bare "nothing detached" boolean could not express - it would fail
        // here by construction, and the cheapest way to satisfy it would be to
        // delete this arm.
        //
        // The boundary is also benign, and structurally so: a leaf that fits
        // inside the 1 MiB default budget is about two orders of magnitude
        // below the 64 MiB division threshold, so it has no cheap division to
        // forfeit. The condition under which windowing stops helping is
        // co-extensive with the condition under which the defect cannot bite.
        var rows = HydrationRows(16);
        var grain = await RehydratedLeafAsync(rows, residentBudgetBytes: 8L * 1024 * 1024);

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True,
            "the leaf must start lazily attached, or this case proves nothing");

        _ = await grain.CountAsync();

        Assert.That(grain.CacheForTest.LastDetachSeam,
            Is.EqualTo(LeafSnapshotDetachSeam.RangeHydrationCompleted),
            "a within-budget leaf has nothing to evict, so the bounded walk "
            + "completes the source - but the release must still be attributed "
            + "to the bounded path rather than to a whole-cache accessor");
    }

    [Test]
    public async Task The_windowed_seams_answer_exactly_as_a_fully_hydrated_leaf_does()
    {
        // Bounding the walk must change only the peak footprint, never the
        // answer. The windows are disjoint, exhaustive and ascending, so each
        // fold sees every row exactly once.
        var rows = HydrationRows();
        var windowed = await RehydratedLeafAsync(rows, residentBudgetBytes: NonDetachingBudgetBytes);
        var full = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);

        Assert.That(await windowed.CountAsync(), Is.EqualTo(await full.CountAsync()));
        Assert.That(await windowed.GetKeysAsync(), Is.EqualTo(await full.GetKeysAsync()));
        Assert.That(
            (await windowed.GetEntriesAsync()).Select(kv => kv.Key).ToArray(),
            Is.EqualTo((await full.GetEntriesAsync()).Select(kv => kv.Key).ToArray()));
        Assert.That(
            (await windowed.GetLiveEntriesAsync()).Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            Is.EqualTo((await full.GetLiveEntriesAsync()).Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray()));
        Assert.That(
            (await windowed.GetAllRawEntriesAsync()).Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            Is.EqualTo((await full.GetAllRawEntriesAsync()).Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray()));
    }

    [Test]
    public async Task A_ranged_count_matches_the_equivalent_ranged_key_listing()
    {
        // Clipping each hydration window to the caller's range is what keeps a
        // genuinely ranged read cheap rather than whole-leaf; it must not move
        // the boundary rows.
        var grain = await WindowedLeafAsync();
        var start = HydrationKey(64);
        var end = HydrationKey(192);

        var counted = await grain.CountAsync(start, end);
        var listed = await grain.GetKeysAsync(start, end);

        Assert.That(counted, Is.EqualTo(listed.Count));
        Assert.That(listed, Is.All.Matches<string>(k =>
            string.CompareOrdinal(k, start) >= 0 && string.CompareOrdinal(k, end) < 0));
        AssertFrameSurvived(grain, "a genuinely ranged read must stay frame-only");
    }
}
