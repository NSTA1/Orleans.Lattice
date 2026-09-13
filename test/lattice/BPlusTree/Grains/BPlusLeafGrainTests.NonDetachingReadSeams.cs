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
/// Every assertion here is on <c>HasPendingHydration</c> at the moment after the
/// operation, deliberately rather than on an outcome that detachment would
/// merely make slower. Nothing in this fixture touches
/// <c>EntriesForTest</c>: that property returns the live backing dictionary
/// through <c>UnderlyingRows</c>, so it detaches the frame BEFORE any assertion
/// could run, and a test written this way would exercise the detached fallback
/// while appearing to exercise the attached path - passing identically with and
/// without the fix.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    // Small enough that the corpus spans several hydration windows, so
    // TrimToBudget can actually evict behind the walk. Mirrors the budget the
    // sibling eviction tests use (32 bytes per row over 256 rows).
    private const long NonDetachingBudgetBytes = 32L * 256L;

    private static async Task<BPlusLeafGrain> WindowedLeafAsync()
        => await RehydratedLeafAsync(HydrationRows(), residentBudgetBytes: NonDetachingBudgetBytes);

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

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True,
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

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True,
            "an already-ranged seam called without bounds is still a detaching seam");
        Assert.That(keys, Is.Not.Empty);
    }

    [Test]
    public async Task An_unbounded_entry_listing_does_not_detach_the_hydration_frame()
    {
        var grain = await WindowedLeafAsync();

        var entries = await grain.GetEntriesAsync();

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
        Assert.That(entries, Is.Not.Empty);
    }

    [Test]
    public async Task Collecting_leaf_stats_does_not_detach_the_hydration_frame()
    {
        var grain = await WindowedLeafAsync();

        _ = await grain.GetStatsAsync();

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
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

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
        Assert.That(live, Is.Not.Empty);
    }

    [Test]
    public async Task Reading_every_raw_entry_does_not_detach_the_hydration_frame()
    {
        var grain = await WindowedLeafAsync();

        var all = await grain.GetAllRawEntriesAsync();

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
        Assert.That(all, Is.Not.Empty);
    }

    [Test]
    public async Task A_leaf_smaller_than_the_hydration_budget_still_detaches_on_a_total_walk()
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
        // Where the budget EXCEEDS the leaf nothing is evictable,
        // GetFullScanWindowsWithoutHydrating collapses to a single unbounded
        // window, every block lands resident together and HydrateBlock
        // detaches. That is the honest boundary of this fix, and it is benign:
        // a leaf that fits inside the 1 MiB default budget is about two orders
        // of magnitude below the 64 MiB division threshold, so it has no cheap
        // division to forfeit. The defect is structurally confined to leaves
        // that cannot suffer from it.
        var rows = HydrationRows(16);
        var grain = await RehydratedLeafAsync(rows, residentBudgetBytes: 8L * 1024 * 1024);

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True,
            "the leaf must start lazily attached, or this case proves nothing");

        _ = await grain.CountAsync();

        Assert.That(grain.CacheForTest.HasPendingHydration, Is.False,
            "a budget larger than the leaf leaves nothing to evict, so a total "
            + "walk completes the source and detaches - windowing bounds peak "
            + "footprint, it does not preserve a frame it cannot evict behind");
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
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);
    }
}
