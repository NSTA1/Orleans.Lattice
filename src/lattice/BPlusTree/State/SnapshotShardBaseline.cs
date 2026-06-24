namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Durable, per-cursor, per-shard frozen projection baseline captured at
/// snapshot-cursor open time. Holds the fully materialised committed
/// projection for one physical shard at a single tree-wide WAL point
/// (<see cref="CapturedHeadPerPartition"/>), so a zero-observable-writes
/// snapshot cursor can serve its range scans without replaying the
/// write-ahead log.
/// <para>
/// This is the fix for the class of bug where a snapshot scan replayed the
/// WAL from offset 0 and silently returned empty / partial results once
/// <c>LatticeWalGc</c> trimmed the committed prefix the replay needed: an
/// ephemeral snapshot reader is not a registered WAL cursor, so nothing
/// protected that prefix. Materialising the projection once at open and
/// serving it from this durable row removes the dependency on the WAL prefix
/// entirely - a subsequent trim cannot perturb an already-frozen baseline,
/// and rebuild-after-eviction reloads the same rows so the point-in-time view
/// is stable across silo failover.
/// </para>
/// <para>
/// The rows are the union of every leaf in the shard's chain, each folded
/// exactly once over its own <c>(leaf_frontier, capturedHead]</c> WAL tail so
/// non-idempotent CRDT folds are never double-counted. Incomplete sagas
/// (prepared but not committed by <see cref="CapturedHeadPerPartition"/>) are
/// hidden, matching the live read path's registry-snapshot visibility rules.
/// </para>
/// <para>
/// Grain key format: <c>{treeId}/{shardIndex}/{baselineToken:N}</c>, where
/// <c>baselineToken</c> is the per-cursor
/// <see cref="LatticeSnapshotCoordinate.SnapshotBaselineToken"/>. One row per
/// (cursor, shard); deleted when the cursor closes or its idle TTL expires.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotShardBaseline)]
internal sealed class SnapshotShardBaseline
{
    /// <summary>
    /// Materialised, key-ordered committed projection rows for the shard at
    /// <see cref="CapturedHeadPerPartition"/>. Tombstones may be present and
    /// are filtered out at scan time, matching the leaf-snapshot blob
    /// convention. Never <see langword="null"/>; empty when the shard had no
    /// live keys at capture.
    /// </summary>
    [Id(0)] public IReadOnlyList<LeafSnapshotRow> Rows { get; set; } = Array.Empty<LeafSnapshotRow>();

    /// <summary>
    /// Per-partition next-to-be-assigned WAL offsets the baseline was frozen
    /// at, indexed by WAL partition number. Every leaf in the shard was folded
    /// up to exactly these offsets. Diagnostic / pin anchor only - serving the
    /// baseline does not read the WAL. Defaults to an empty array on the
    /// "nothing captured" sentinel state row.
    /// </summary>
    [Id(1)] public IReadOnlyList<long> CapturedHeadPerPartition { get; set; } = Array.Empty<long>();

    /// <summary>
    /// Wall-clock <see cref="DateTime.Ticks"/> at the moment the baseline was
    /// captured. Diagnostic only.
    /// </summary>
    [Id(2)] public long CapturedAtTicks { get; set; }

    /// <summary>
    /// Precomputed byte-accurate footprint of <see cref="Rows"/> using the same
    /// UTF-8-key + stored-value-length formula the leaf surface uses for its
    /// state-byte accounting. Populated once at capture so footprint reads are
    /// a constant-time field read.
    /// </summary>
    [Id(3)] public long RowBytes { get; set; }

    /// <summary>
    /// Marks this state row as a real captured baseline rather than a
    /// default-allocated row returned by the storage provider for a key that
    /// has never been written. <see cref="CapturedHeadPerPartition"/> is empty
    /// on the default row, so an empty array is the "nothing captured"
    /// sentinel; a captured baseline always carries at least one partition
    /// head. Kept explicit so an empty-shard capture (zero rows but a real
    /// head) is still distinguishable from the never-written default.
    /// </summary>
    [Id(4)] public bool Captured { get; set; }
}
