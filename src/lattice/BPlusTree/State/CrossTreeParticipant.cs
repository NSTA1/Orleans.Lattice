namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One participating tree's slice of a cross-tree atomic write as persisted in
/// <see cref="CrossTreeTxState"/>: the tree to write into, the (defensively
/// copied) entries, the optional guard predicate, and the recorded prepare
/// vote. Mutable so the coordinator can stamp the <see cref="Vote"/> after the
/// prepare phase without reallocating the list.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeParticipant)]
internal sealed class CrossTreeParticipant
{
    /// <summary>The logical tree this slice writes into.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The key/value entries to commit on <see cref="TreeId"/>. A defensive
    /// copy of the caller's list (and each value buffer) taken by the
    /// coordinator before persisting, so a caller-side mutation after submit
    /// cannot corrupt the in-flight saga.
    /// </summary>
    [Id(1)] public List<KeyValuePair<string, byte[]>> Entries { get; set; } = [];

    /// <summary>
    /// Optional server-side guard predicate evaluated against each key's
    /// pre-saga value during the prepare phase. <see langword="null"/> for an
    /// unguarded slice.
    /// </summary>
    [Id(2)] public LatticePredicateNode? Predicate { get; set; }

    /// <summary>
    /// The vote recorded for this participant after the prepare phase, or
    /// <see langword="null"/> while the prepare dispatch is still in flight.
    /// </summary>
    [Id(3)] public CrossTreePrepareVote? Vote { get; set; }

    /// <summary>
    /// Optional per-entry author-delta carry aligned 1:1 with
    /// <c>Entries</c>: <c>EntryDeltas[i]</c> is the opaque,
    /// Orleans-serialised typed CRDT delta to stamp onto the per-key emit for
    /// <c>Entries[i]</c>, or <see langword="null"/> for a plain
    /// last-writer-wins value write. The whole list is <see langword="null"/>
    /// when no entry on this slice carries a delta (the common case). A
    /// defensive copy taken by the coordinator alongside <c>Entries</c>,
    /// then forwarded to the per-tree sub-saga's
    /// <see cref="Grains.AtomicWriteGrain.PrepareForCoordinatorAsync"/> so each
    /// flag-CRDT membership row converges by replaying the author's enable-dot
    /// delta. Wire-compatible: a missing field on legacy persisted state
    /// decodes to <see langword="null"/>.
    /// </summary>
    [Id(4)] public List<byte[]?>? EntryDeltas { get; set; }

    /// <summary>
    /// Optional per-entry delete (tombstone) channel aligned 1:1 with
    /// <c>Entries</c>: <c>EntryDeletes[i]</c> is <see langword="true"/>
    /// when <c>Entries[i]</c> is a retraction delete that rides the
    /// all-or-nothing batch alongside the upserts, or <see langword="false"/>
    /// for a value upsert. The whole list is <see langword="null"/> when the
    /// slice carries only upserts (the common case). A defensive copy taken by
    /// the coordinator alongside <c>Entries</c>, then forwarded to the
    /// per-tree sub-saga's
    /// <see cref="Grains.AtomicWriteGrain.PrepareForCoordinatorAsync"/> so the
    /// mixed set+delete batch flips visible (or rolls back) on the same
    /// per-shard terminal. Wire-compatible: a missing field on legacy persisted
    /// state decodes to <see langword="null"/>.
    /// </summary>
    [Id(5)] public List<bool>? EntryDeletes { get; set; }
}
