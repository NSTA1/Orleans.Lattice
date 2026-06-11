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
}
