namespace Orleans.Lattice;

/// <summary>
/// Classifies a single <see cref="HistoryRow"/> in a durable per-key history
/// view. Each source mutation appends exactly one history row keyed
/// <c>{sourceKey}/{encodedHlc}</c>; the kind records what the underlying source
/// mutation was so a reader can render the revision timeline without consulting
/// the source tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HistoryRowKind)]
public enum HistoryRowKind
{
    /// <summary>
    /// An LWW (last-writer-wins) value write. The row carries the value bytes
    /// (<see cref="HistoryRetentionMode.FullValue"/> / recent
    /// <see cref="HistoryRetentionMode.Hybrid"/>) or only a content hash and
    /// length (<see cref="HistoryRetentionMode.MetadataOnly"/>).
    /// </summary>
    Set = 0,

    /// <summary>
    /// A point delete or tombstone reap of the source key. A delete is itself a
    /// revision in an append-only history, so it is recorded as a row rather than
    /// removing prior revisions.
    /// </summary>
    Delete = 1,

    /// <summary>
    /// A CRDT mutation stored as its author delta (the increment, not the merged
    /// state). The compact, doubling-free representation that converges when
    /// replayed and that the element-level provenance decoder reads.
    /// </summary>
    CrdtDelta = 2,

    /// <summary>
    /// A marker recording that an unconstrained range delete swept the half-open
    /// range <c>[<see cref="HistoryRow.SourceKey"/>, <see cref="HistoryRow.EndKey"/>)</c>.
    /// In an append-only history a range delete does not erase the fact that prior
    /// values existed, so it is recorded as a marker rather than reconciled by a
    /// rebuild.
    /// </summary>
    RangeTombstone = 3,
}
