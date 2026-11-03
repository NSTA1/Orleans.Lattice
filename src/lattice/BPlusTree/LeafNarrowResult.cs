namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a leaf hands a shard's bulk operation whose new leaves take over its range: the
/// rows it held there, for the operation to relocate, and how far it had replayed the
/// WAL when they were read. <see cref="IBPlusLeafGrain.NarrowHighKeyAsync"/> returns it
/// to a bulk graft and <see cref="IBPlusLeafGrain.CopyRowsForBulkLoadAsync"/> to a bulk
/// load. Not part of the end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafNarrowResult)]
[Immutable]
internal sealed record LeafNarrowResult
{
    /// <summary>
    /// A copy of every row at or above the new bound, tombstones included, with its
    /// stamp and metadata (issue #5610). Empty when the leaf held none.
    /// </summary>
    [Id(0)] public List<LwwEntry> Rows { get; init; } = [];

    /// <summary>
    /// Per WAL partition, the leaf's in-memory projection checkpoint when this call
    /// narrowed it (inclusive; <c>-1</c> for none), read before the narrow so no record
    /// it skipped as out of span is behind it (issue #5752). Replay, an armed live
    /// advance and checkpoint hints move it; a split donor's own advance to one below
    /// the heads is a hint that is not capped, so on such a leaf the frontier carries
    /// that residue (issue #5756). Every record of the narrowed-away range the leaf had not applied lies
    /// above it, so an appended leaf that starts no higher replays every one.
    /// <see langword="null"/> when a narrow did not narrow - the leaf was already
    /// narrowed by an earlier attempt, after which its replay may have stepped past a
    /// record of that range - so no read position can be inferred.
    /// </summary>
    [Id(1)] public long[]? ReadFrontier { get; init; }
}