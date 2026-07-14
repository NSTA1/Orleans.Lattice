namespace Orleans.Lattice.Api.State;

/// <summary>
/// Selects the cursor isolation an entry scan
/// (<see cref="ILatticeStateQuery.ScanEntriesAsync"/>) opens. The wire default
/// is <see cref="Snapshot"/> so an existing caller that never set the field
/// keeps the released point-in-time semantics unchanged; the live modes are an
/// explicit, cheaper opt-in that avoids the all-shard snapshot-baseline capture.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryScanMode)]
public enum EntryScanMode
{
    /// <summary>
    /// Strict point-in-time snapshot isolation. The first page captures a
    /// tree-wide baseline (a per-shard frozen projection) and every page reads
    /// that same frozen view, so a multi-page scan never observes a concurrent
    /// write, split, or reshard. This is the heaviest open - it fans a
    /// baseline capture out to every shard root - and is the default only for
    /// wire back-compatibility. Prefer a live mode for casual browsing.
    /// </summary>
    Snapshot = 0,

    /// <summary>
    /// A live cursor with no baseline capture. Paging is keyed on the last
    /// yielded key (a key-exclusive continuation), so it never duplicates an
    /// already-returned key and tolerates concurrent shard splits per step, but
    /// it is not a single-instant view: writes committed after the scan opened
    /// can appear on later pages, and a value reflects its state at read time.
    /// The cheapest mode; the right default for interactive browsing.
    /// </summary>
    Live = 1,

    /// <summary>
    /// A live cursor that additionally pins the in-flight-saga decision view at
    /// open time, so every page sees the same saga-visibility snapshot without
    /// the per-shard baseline capture of <see cref="Snapshot"/>. Use it when a
    /// scan must make a stable decision across pages but does not need strict
    /// isolation against every concurrent foreground write.
    /// </summary>
    LivePointInTime = 2,
}
