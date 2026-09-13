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
/// <c>HasPendingHydration</c> boolean, because the seam names WHICH surface
/// released the frame. Only the three whole-cache accessors detach; a bounded or
/// ranged walk retains the frame even when it completes the source (issue
/// #2843), so its seam stays <see cref="LeafSnapshotDetachSeam.None"/>.
/// Asserting the seam rather than the boolean keeps each arm's verdict on
/// whether the correct surface ran, not on the budget-to-leaf ratio.
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
    /// it. Since issue #2843 a ranged or bounded walk retains the frame even
    /// when it completes the source, so this holds regardless of the
    /// budget-to-leaf ratio; only a whole-cache accessor would detach and flip
    /// it.
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
        // TrimToBudget call, nothing is evictable, and every block lands
        // resident at once. Since issue #2843 completing the source no longer
        // detaches the frame, but a single whole-span window still pins the
        // whole leaf resident. Walking budget-sized windows is what keeps the
        // peak footprint bounded and the frame both attached and evictable.
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
    public async Task A_leaf_smaller_than_the_hydration_budget_retains_the_frame_through_completion()
    {
        // The former adversarial case, closed by issue #2843.
        //
        // Where the budget EXCEEDS the leaf nothing is evictable, so a bounded
        // walk materialises the final outstanding block and completes the
        // source. That used to detach the frame (recorded as
        // RangeHydrationCompleted): benign for this leaf's memory, but a
        // forfeited bisect for any leaf that later has to divide. Since #2843
        // the completion RETAINS the frame instead - finishing a ranged read is
        // not a reason to release the frame a division pivots from.
        //
        // The boundary that made this "adversarial" is gone. A leaf that fits
        // inside the 1 MiB default budget is about two orders of magnitude below
        // the 64 MiB division threshold, so it had no cheap division to forfeit
        // anyway - but retaining the frame means it keeps the fast path
        // regardless of the ratio.
        var rows = HydrationRows(16);
        var grain = await RehydratedLeafAsync(rows, residentBudgetBytes: 8L * 1024 * 1024);

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True,
            "the leaf must start lazily attached, or this case proves nothing");

        _ = await grain.CountAsync();

        // Non-vacuity, independent of the retention property: the bounded walk
        // ran to completion. Every row is resident, which only a completed
        // hydration achieves; an unconverted path that short-circuited would
        // leave this below the row count and fail here first.
        Assert.That(grain.CacheForTest.HydratedRowCount, Is.EqualTo(rows.Length),
            "the within-budget walk must complete the source, or this arm proves nothing");

        Assert.Multiple(() =>
        {
            Assert.That(grain.CacheForTest.LastDetachSeam,
                Is.EqualTo(LeafSnapshotDetachSeam.None),
                "completing a within-budget ranged hydration must not detach the frame (issue #2843)");
            Assert.That(grain.CacheForTest.HasPendingHydration, Is.True,
                "the frame is retained so a division can still bisect");
            Assert.That(grain.CacheForTest.TryGetBisectingKeyWithoutHydrating(out _, out var reason),
                Is.True,
                "the split path takes its pivot from the retained frame");
            Assert.That(reason, Is.EqualTo(LeafBisectRefusalReason.None));
        });
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
