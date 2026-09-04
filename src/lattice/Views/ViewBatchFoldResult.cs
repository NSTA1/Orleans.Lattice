namespace Orleans.Lattice;

/// <summary>
/// The result of folding one view drain batch: the coalesced survivors and the
/// re-key collisions observed while coalescing them. Produced by
/// <see cref="ViewBatchFold.Fold(IReadOnlyList{ViewWrite})"/>.
/// </summary>
/// <param name="Survivors">
/// One survivor per <see cref="ViewWrite.Key"/>, each the highest-<see cref="ViewWrite.Timestamp"/>
/// write for its key, in the order the keys were first seen. Identical to what
/// <see cref="ViewWriteCoalescer.Coalesce(IEnumerable{ViewWrite})"/> returns for
/// the same batch.
/// </param>
/// <param name="Collisions">
/// The view keys produced by more than one distinct source key, in first-seen
/// order. Identical to what
/// <see cref="ViewKeyCollisionDetector.Detect(IEnumerable{ViewWrite})"/> returns
/// for the same batch. Empty means the batch is collision-free.
/// </param>
internal readonly record struct ViewBatchFoldResult(
    IReadOnlyList<ViewWrite> Survivors,
    IReadOnlyList<string> Collisions);
