using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 2436: <c>LeafSnapshotStorageGrain.MergeMonotone</c>
/// could resurrect a deleted key once tombstone compaction had retired the
/// tombstone that recorded the delete.
/// <para>
/// The merge's slow path - taken whenever an incoming capture would lower any
/// partition's coverage - unions the two row sets. A union treats an absence as
/// the bottom element of the lattice: "this side never saw the key, keep the
/// other side's row". That reading holds only while every delete is still
/// represented by a tombstone row. <c>BPlusLeafGrain.CompactTombstonesAsync</c>
/// PHYSICALLY REMOVES a tombstone once it is older than the grace period, after
/// which the deleted key is simply ABSENT from every later capture - so absence
/// means "deleted, and the marker is gone", a value strictly ABOVE the stored
/// live row, not below it.
/// </para>
/// <para>
/// Retaining the stored row was therefore not conservative but a resurrection,
/// and the element-wise <c>Math.Max</c> coverage made it permanent: the merged
/// blob claimed the incoming capture's higher offset, so replay restarted PAST
/// the delete and nothing in the WAL was left to contradict the live row. The
/// key came back on every subsequent cold restart.
/// </para>
/// <para>
/// The repair declines the merge - keeping the stored blob verbatim - whenever
/// the incoming capture does not carry every key the stored blob holds, mirroring
/// the decline already taken at the same seam for a segmented stored blob. The
/// fixtures below pin both directions: the resurrection is foreclosed, AND the
/// decline is narrow enough that an ordinary delete, an ordinary row merge, and
/// the non-regressing fast path all still behave exactly as before.
/// </para>
/// </summary>
public sealed partial class LeafSnapshotStorageGrainTests
{
    /// <summary>
    /// Builds a two-partition blob with explicit per-partition coverage. The
    /// scalar offset mirrors partition 0, as a real capture's does.
    /// </summary>
    private static LeafSnapshotBlob CompactionBlob(long partition0, long partition1, params LeafSnapshotRow[] rows) =>
        new()
        {
            SnapshotOffset = partition0,
            Rows = rows,
            CapturedAtTicks = 12345,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = [partition0, partition1],
        };

    /// <summary>
    /// A live row. Operands in these fixtures differ ONLY in
    /// <c>HybridLogicalClock.WallClockTicks</c>, which is the first and decisive
    /// comparison in <c>LwwValue{T}.Merge</c>, so no assertion here rests on
    /// <c>OriginClusterId</c> - which is observer-relative and therefore not a
    /// replica-invariant discriminator.
    /// </summary>
    private static LeafSnapshotRow CompactionLiveRow(string key, byte[] value, long wallClockTicks) =>
        new(key, LwwValue<byte[]>.Create(value, new HybridLogicalClock { WallClockTicks = wallClockTicks }));

    /// <summary>A delete marker, as a capture carries it before compaction reaps it.</summary>
    private static LeafSnapshotRow CompactionTombstoneRow(string key, long wallClockTicks) =>
        new(key, LwwValue<byte[]>.Tombstone(new HybridLogicalClock { WallClockTicks = wallClockTicks }));

    /// <summary>
    /// THE DEFECT. A capture taken after the delete's tombstone was reaped omits
    /// the key entirely; if that same capture also regresses any partition it
    /// lands on the row-merging slow path, where the union retained the stored
    /// live row with no comparison at all while advancing coverage past the
    /// delete.
    /// </summary>
    [Test]
    public async Task SaveAsync_does_not_advance_coverage_past_a_delete_whose_tombstone_compaction_reaped()
    {
        var (grain, state) = CreateGrain();

        // The stored snapshot predates the delete: it covers partition 0 through
        // offset 100 and carries "k" live.
        var stored = CompactionBlob(100L, 50L, CompactionLiveRow("k", [1, 2, 3], wallClockTicks: 900L));
        await grain.SaveAsync(stored, CancellationToken.None);

        // A later capture. "k" was deleted and its tombstone has since been
        // reaped by CompactTombstonesAsync, so the capture carries no row for it
        // at all. Partition 1 regressed (50 -> 40), which is what drives the
        // merge off its fast path and onto the row union.
        var afterCompaction = CompactionBlob(200L, 40L);
        await grain.SaveAsync(afterCompaction, CancellationToken.None);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(loaded!.SnapshotOffsetsByPartition, Is.Not.Null);

            // RED before the fix: coverage for partition 0 was raised to 200 -
            // past the delete - while "k" was still carried live, so replay
            // restarted beyond the delete and the key was live forever.
            Assert.That(loaded.SnapshotOffsetsByPartition![0], Is.EqualTo(100L),
                "coverage MUST NOT advance past an offset whose mutations include a delete the merged row "
                + "set no longer reflects; the incoming capture omits \"k\" because compaction reaped its "
                + "tombstone, so claiming the capture's higher offset while retaining the stored live row "
                + "resurrects the key permanently (issue 2436)");

            // The complementary half of the same property: declining must not be
            // implemented by regressing instead. Coverage authorises the WAL GC
            // trim floor, so lowering it strands an already-trimmed prefix.
            Assert.That(loaded.SnapshotOffsetsByPartition[1], Is.EqualTo(50L),
                "declining the incoming capture must keep the stored coverage verbatim, never lower it - "
                + "the coverage-gated WAL GC has already trimmed partition 1's prefix to 50");

            // And it must not be implemented by dropping the row either: a cold
            // or partial rehydrate can legitimately leave a live key out of the
            // cache, so dropping a stored-only row would lose data.
            Assert.That(loaded.Rows!.Select(r => r.Key), Is.EquivalentTo(new[] { "k" }),
                "the stored blob is kept verbatim; dropping the stored-only row instead would lose a key "
                + "that a partial rehydrate legitimately left out of the incoming capture");
        });

        Assert.That(state.State, Is.SameAs(stored),
            "a declined merge must not rewrite durable state at all - SaveAsync elides the write when the "
            + "merge returns the stored blob by reference");
    }

    /// <summary>
    /// The decline keys on a STORED-ONLY key, not on an empty incoming capture:
    /// a capture that carries other rows but has lost the reaped one is the same
    /// hazard and must decline too.
    /// </summary>
    [Test]
    public async Task SaveAsync_declines_when_a_regressing_capture_omits_only_one_stored_key()
    {
        var (grain, _) = CreateGrain();

        var stored = CompactionBlob(
            100L,
            50L,
            CompactionLiveRow("a", [1], wallClockTicks: 900L),
            CompactionLiveRow("k", [2], wallClockTicks: 900L));
        await grain.SaveAsync(stored, CancellationToken.None);

        // "a" is still live and freshly captured; only "k" vanished with its
        // reaped tombstone. Partition 1 regresses, so the slow path runs.
        var afterCompaction = CompactionBlob(200L, 40L, CompactionLiveRow("a", [9], wallClockTicks: 950L));
        await grain.SaveAsync(afterCompaction, CancellationToken.None);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(loaded!.SnapshotOffsetsByPartition![0], Is.EqualTo(100L),
                "one stored-only key is enough to make the whole merged coverage unsound, so the merge "
                + "declines even though the capture carried a fresher row for every other key");
            Assert.That(loaded.Rows!.Select(r => r.Key), Is.EquivalentTo(new[] { "a", "k" }));
            Assert.That(loaded.Rows!.Single(r => r.Key == "a").Value.Timestamp.WallClockTicks, Is.EqualTo(900L),
                "declining keeps the stored blob verbatim, so the fresher row for \"a\" is dropped along "
                + "with the rest of the capture rather than being merged into an unsound coverage claim");
        });
    }

    /// <summary>
    /// THE GUARD IS NARROW. A regressing capture that carries every stored key
    /// still takes the row-merging slow path and still advances the partitions it
    /// legitimately advanced - the fix must not degrade into an unconditional
    /// decline, which would silently freeze durable coverage.
    /// </summary>
    [Test]
    public async Task SaveAsync_still_merges_a_regressing_capture_that_carries_every_stored_key()
    {
        var (grain, _) = CreateGrain();

        var stored = CompactionBlob(100L, 50L, CompactionLiveRow("k", [1, 2, 3], wallClockTicks: 900L));
        await grain.SaveAsync(stored, CancellationToken.None);

        // Regresses partition 1 (50 -> 40) but carries "k", so the union is a
        // genuine join and the merge is sound.
        var regressing = CompactionBlob(200L, 40L, CompactionLiveRow("k", [4], wallClockTicks: 200L));
        await grain.SaveAsync(regressing, CancellationToken.None);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(loaded!.SnapshotOffsetsByPartition![0], Is.EqualTo(200L),
                "the capture advanced partition 0 and carries every stored key, so the element-wise max "
                + "coverage is row-backed and must still be taken");
            Assert.That(loaded.SnapshotOffsetsByPartition[1], Is.EqualTo(50L),
                "the stored coverage of the regressing partition is retained");
            Assert.That(loaded.Rows!.Single(r => r.Key == "k").Value.Timestamp.WallClockTicks, Is.EqualTo(900L),
                "the per-key winner is still decided by LWW on the hybrid logical clock, so the stored row "
                + "that backs the retained partition-1 coverage survives the merge");
        });
    }

    /// <summary>
    /// The hazard is ABSENCE, not deletion. A delete whose tombstone has NOT yet
    /// been reaped is carried by the capture, merges by ordinary last-writer-wins,
    /// and must still reach durable state - otherwise the fix would block deletes
    /// from ever being snapshotted.
    /// </summary>
    [Test]
    public async Task SaveAsync_merges_a_regressing_capture_whose_tombstone_supersedes_the_stored_row()
    {
        var (grain, _) = CreateGrain();

        var stored = CompactionBlob(100L, 50L, CompactionLiveRow("k", [1, 2, 3], wallClockTicks: 100L));
        await grain.SaveAsync(stored, CancellationToken.None);

        // "k" was deleted and the tombstone is still within the grace period, so
        // the capture carries it. Partition 1 regresses, so the slow path runs.
        var withTombstone = CompactionBlob(200L, 40L, CompactionTombstoneRow("k", wallClockTicks: 900L));
        await grain.SaveAsync(withTombstone, CancellationToken.None);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(loaded!.SnapshotOffsetsByPartition![0], Is.EqualTo(200L),
                "a delete the capture still represents as a tombstone is an ordinary LWW merge, so coverage "
                + "advances exactly as it did before the fix");
            Assert.That(loaded.Rows!.Single(r => r.Key == "k").Value.IsTombstone, Is.True,
                "the later tombstone must win over the stored live row, so the delete reaches durable state");
        });
    }

    /// <summary>
    /// The fast path is untouched. A capture taken after compaction normally
    /// ADVANCES every partition, so it supersedes the stored blob verbatim and the
    /// reaped key is dropped - which is the correct outcome and the reason the
    /// decline above costs nothing in steady state.
    /// </summary>
    [Test]
    public async Task SaveAsync_drops_a_compacted_key_when_the_capture_advances_every_partition()
    {
        var (grain, _) = CreateGrain();

        var stored = CompactionBlob(100L, 50L, CompactionLiveRow("k", [1, 2, 3], wallClockTicks: 900L));
        await grain.SaveAsync(stored, CancellationToken.None);

        // Advances both partitions, so no coverage regresses and the merge takes
        // its fast path: the capture is authoritative verbatim.
        var afterCompaction = CompactionBlob(200L, 60L);
        await grain.SaveAsync(afterCompaction, CancellationToken.None);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(loaded!.SnapshotOffsetsByPartition, Is.EqualTo(new[] { 200L, 60L }).AsCollection,
                "a non-regressing capture is a superset and still overwrites outright");
            Assert.That(loaded.GetRowCount(), Is.Zero,
                "the compacted key is correctly absent: the capture's own coverage backs the delete, so "
                + "there is nothing to resurrect and nothing to decline");
        });
    }
}
