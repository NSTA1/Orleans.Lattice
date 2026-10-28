using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5130, leaf side: a migration import onto a row the leaf still holds
/// sealed, in a slot a split took away, is resolved by last-writer-wins. The
/// guard that keeps a migration import from overwriting a non-migrated row
/// protects a post-split foreground commit, and a sealed slot has none: the
/// shard refuses every write to it, so the row predates the split, and a fold
/// returning the slot must be able to supersede it.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const int SealedImportVirtualShardCount = 16;

    [Test]
    public async Task MergeMany_migration_import_supersedes_an_older_row_in_a_sealed_slot()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", [1]);
        var orphanStamp = grain.EntriesForTest["k"].Timestamp;
        await grain.MarkSlotsMovedAwayAsync([ShardMap.GetVirtualSlot("k", SealedImportVirtualShardCount)], SealedImportVirtualShardCount);

        var newer = LwwValue<byte[]>.Create([2], HybridLogicalClock.Tick(orphanStamp));
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>> { ["k"] = newer }, isCrossShardMigration: true);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 2 }),
            "A sealed row predates the split, so the donor's newer copy must win.");
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.True);
    }

    [Test]
    public async Task MergeMany_migration_tombstone_supersedes_an_older_row_in_a_sealed_slot()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", [1]);
        var orphanStamp = grain.EntriesForTest["k"].Timestamp;
        await grain.MarkSlotsMovedAwayAsync([ShardMap.GetVirtualSlot("k", SealedImportVirtualShardCount)], SealedImportVirtualShardCount);

        await grain.MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>> { ["k"] = LwwValue<byte[]>.Tombstone(HybridLogicalClock.Tick(orphanStamp)) },
            isCrossShardMigration: true);

        Assert.That(grain.EntriesForTest["k"].IsTombstone, Is.True);
    }

    [Test]
    public async Task MergeMany_migration_import_still_never_overwrites_a_foreground_row_in_an_unsealed_slot()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", [1]);
        var foregroundStamp = grain.EntriesForTest["k"].Timestamp;

        var higher = LwwValue<byte[]>.Create([2], HybridLogicalClock.Tick(foregroundStamp));
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>> { ["k"] = higher }, isCrossShardMigration: true);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 1 }),
            "Off a sealed slot the guard still protects the foreground commit.");
    }

    [Test]
    public async Task MergeMany_migration_import_does_not_regress_a_sealed_row_with_a_newer_stamp()
    {
        var grain = CreateGrain();
        var older = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 0 };
        await grain.SetAsync("k", [1]);
        await grain.MarkSlotsMovedAwayAsync([ShardMap.GetVirtualSlot("k", SealedImportVirtualShardCount)], SealedImportVirtualShardCount);

        await grain.MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>> { ["k"] = LwwValue<byte[]>.Create([2], older) },
            isCrossShardMigration: true);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 1 }),
            "The import is last-writer-wins, not an overwrite.");
    }
}
