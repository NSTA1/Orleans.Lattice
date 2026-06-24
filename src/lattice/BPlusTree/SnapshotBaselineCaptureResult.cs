namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result of <see cref="IShardRootGrain.CaptureSnapshotBaselineAsync"/>: the
/// per-partition WAL head the shard's frozen baseline was captured at, plus
/// the materialised row count used by the snapshot-open budget gate.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.SnapshotBaselineCaptureResult)]
internal readonly record struct SnapshotBaselineCaptureResult(
    /// <summary>
    /// Per-partition WAL head (next-to-be-assigned offset) the baseline was
    /// frozen at, indexed by WAL partition number. Carried onto the snapshot
    /// coordinate's per-shard offsets for the WAL retention pin and
    /// diagnostics; the baseline itself is served without re-reading the WAL.
    /// </summary>
    [property: Id(0)] long[] CapturedHeadPerPartition,
    /// <summary>
    /// Number of materialised rows (including tombstones) in the persisted
    /// baseline. This is the per-shard memory footprint the snapshot leaf will
    /// seed, so it is the cost the open path gates against
    /// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/> - far cheaper
    /// than the old whole-WAL-prefix cost once the WAL has been GC-trimmed.
    /// </summary>
    [property: Id(1)] long RowCount);
