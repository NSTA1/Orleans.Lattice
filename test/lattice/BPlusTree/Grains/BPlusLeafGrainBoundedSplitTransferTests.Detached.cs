using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class BPlusLeafGrainBoundedSplitTransferTests
{
    [TestCase(64, true)]
    [TestCase(2048, true)]
    [TestCase(8192, true)]
    [TestCase(2048, false)]
    public async Task Set_on_resident_leaf_bounds_split_transfers_by_bytes(int rowCount, bool detachSnapshot)
    {
        const long budget = 16L * 1024;
        var (grain, _, recorder, _) = detachSnapshot
            ? await RehydratedDividableLeafAsync(rowCount, maxLeafBytes: 1, residentBudgetBytes: budget)
            : CreateDividableLeaf(maxLeafBytes: 1, residentBudgetBytes: budget);
        var cache = grain.CacheForTest;
        if (detachSnapshot)
        {
            cache.HydrateAll(LeafSnapshotDetachSeam.KeysAccessor);
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor));
        }
        else
        {
            foreach (var row in Rows(rowCount))
            {
                cache.StoreRow(row.Key, row.Value);
            }
        }

        Assert.That(cache.HasPendingHydration, Is.False);
        Assert.That(cache.HydratedRowCount, Is.EqualTo(rowCount));
        Assert.That(cache.StateBytes, Is.GreaterThan(budget), "the leaf must exceed its resident budget");

        // Reach the transfer through the real foreground overflow predicate.
        var result = await grain.SetAsync(Key(0), Payload(0));

        Assert.That(result, Is.Not.Null);
        var expectedKeys = Enumerable.Range(rowCount / 2, rowCount / 2).Select(Key).ToArray();
        Assert.That(recorder.AllTransferredKeys, Is.EqualTo(expectedKeys));
        Assert.That(cache.Keys, Is.EqualTo(Enumerable.Range(0, rowCount / 2).Select(Key)));
        var expectedBytes = (long)(rowCount / 2) * (Key(0).Length + Payload(0).Length);
        Assert.That(recorder.Batches.Count, expectedBytes > budget ? Is.GreaterThan(1) : Is.EqualTo(1),
            "MergeEntriesAsync must receive bounded batches even when there is no snapshot frame");
        foreach (var batch in recorder.Batches)
        {
            Assert.That(batch.Sum(row => LeafEntryCache.EntryBytes(row.Key, row.Value.Value)),
                Is.LessThanOrEqualTo(budget));
            foreach (var (key, value) in batch)
            {
                var index = int.Parse(key.AsSpan(1));
                Assert.That(value.Value, Is.EqualTo(Payload(index)));
                Assert.That(value.Timestamp.WallClockTicks, Is.EqualTo(100L + index));
            }
        }
    }

    [Test]
    public void Transfer_boundaries_on_resident_rows_account_for_deferred_bytes_without_materialising()
    {
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        var calls = 0;
        var metadata = LwwValue<byte[]>.Create(null!, new HybridLogicalClock { WallClockTicks = 100 });
        foreach (var key in new[] { "a", "b", "c" })
        {
            cache.StoreDeferredRow(key, metadata, () => { calls++; return new byte[100]; }, 100);
        }

        Assert.That(cache.GetTransferBatchBoundariesWithoutHydrating("a", 202), Is.EqualTo(new[] { "c" }));
        Assert.That(calls, Is.Zero, "planning must use recorded lengths, not serialize deferred payloads");
        Assert.That(cache.GetTransferBatchBoundariesWithoutHydrating("b", 202), Is.Empty);
        Assert.That(cache.GetTransferBatchBoundariesWithoutHydrating("z", 1), Is.Empty);
        Assert.That(cache.GetTransferBatchBoundariesWithoutHydrating("a", 0), Is.Empty);
    }

    [Test]
    public void Transfer_boundaries_on_resident_rows_isolate_oversized_rows_and_count_utf8_tombstone_keys()
    {
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        var timestamp = new HybridLogicalClock { WallClockTicks = 100 };
        cache.StoreRow("a", LwwValue<byte[]>.Create(new byte[50], timestamp));
        cache.StoreRow("b", LwwValue<byte[]>.Create(new byte[500], timestamp));
        cache.StoreRow("c", LwwValue<byte[]>.Create(new byte[50], timestamp));
        cache.StoreRow("\u00e9", LwwValue<byte[]>.Create(null!, timestamp) with { IsTombstone = true });

        Assert.That(cache.GetTransferBatchBoundariesWithoutHydrating("a", 52),
            Is.EqualTo(new[] { "b", "c", "\u00e9" }));
        Assert.That(cache.GetTransferBatchBoundariesWithoutHydrating("a", long.MaxValue), Is.Empty);
    }
}
