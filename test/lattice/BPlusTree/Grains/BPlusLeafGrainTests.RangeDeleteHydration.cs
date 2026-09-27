using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(64, 68, true)]
    [TestCase(320, 321, false)]
    [TestCase(0, 0, true)]
    public async Task DeleteRange_bounded_walk_preserves_frame_and_exact_results(int start, int end, bool pastRange)
    {
        var rows = HydrationRows();
        var grain = await RehydratedLeafAsync(rows, residentBudgetBytes: NonDetachingBudgetBytes);
        var full = await RehydratedLeafAsync(rows, partialHydrationEnabled: false);

        var expected = await full.DeleteRangeAsync(HydrationKey(start), HydrationKey(end));
        var actual = await grain.DeleteRangeAsync(HydrationKey(start), HydrationKey(end));

        Assert.Multiple(() =>
        {
            Assert.That(actual.Deleted, Is.EqualTo(expected.Deleted));
            Assert.That(actual.PastRange, Is.EqualTo(pastRange));
            Assert.That(grain.CacheForTest.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
            Assert.That(grain.CacheForTest.HydratedRowCount,
                Is.LessThanOrEqualTo(start == 64 ? LeafSnapshotHydrationSource.BlockRows : 0));
        });
        var actualRows = await grain.GetAllRawEntriesAsync();
        var expectedRows = await full.GetAllRawEntriesAsync();
        Assert.That(actualRows.Keys, Is.EquivalentTo(expectedRows.Keys));
        foreach (var (key, row) in expectedRows)
        {
            Assert.That(actualRows[key].IsTombstone, Is.EqualTo(row.IsTombstone), key);
            Assert.That(actualRows[key].Value, Is.EqualTo(row.Value), key);
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task DeleteRange_removed_boundary_keys_do_not_survive_in_PastRange(bool insertResidentKey)
    {
        var grain = await RehydratedLeafAsync(HydrationRows(), residentBudgetBytes: NonDetachingBudgetBytes);
        var end = HydrationKey(315);
        for (var i = 315; i < HydrationCorpusRows; i++)
            Assert.That(grain.CacheForTest.Remove(HydrationKey(i)), Is.True);
        if (insertResidentKey)
            grain.CacheForTest.StoreRow("z", LwwValue<byte[]>.Create([1], HybridLogicalClock.Zero));
        Assert.That(grain.CacheForTest.HasPendingHydration, Is.True);

        var result = await grain.DeleteRangeAsync(HydrationKey(312), end);

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.EqualTo(2));
            Assert.That(result.PastRange, Is.EqualTo(insertResidentKey),
                "hydrated frame ordinals are stale after Remove; only remaining rows count");
            Assert.That(grain.CacheForTest.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
            Assert.That(grain.CacheForTest.HydratedRowCount, Is.LessThan(HydrationCorpusRows));
        });
    }

    [Test]
    public async Task DeleteRange_removed_candidate_still_finds_later_unhydrated_block()
    {
        var grain = await RehydratedLeafAsync(HydrationRows(), residentBudgetBytes: NonDetachingBudgetBytes);
        for (var i = 280; i < 288; i++)
            Assert.That(grain.CacheForTest.Remove(HydrationKey(i)), Is.True);

        var result = await grain.DeleteRangeAsync(HydrationKey(278), HydrationKey(280));

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.EqualTo(1));
            Assert.That(result.PastRange, Is.True,
                "the next unhydrated block still owns keys beyond the removed candidate");
            Assert.That(grain.CacheForTest.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
            Assert.That(grain.CacheForTest.HydratedRowCount, Is.LessThan(HydrationCorpusRows));
        });
    }
}
