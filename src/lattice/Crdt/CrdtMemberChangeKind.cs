namespace Orleans.Lattice;

/// <summary>
/// Classifies a single <see cref="CrdtMemberChange"/> emitted by an
/// <see cref="ICrdtProvenanceDecoder"/>. A CRDT's element-level history is a
/// sequence of additions and removals of individual members; the kind tells a
/// consumer which side of that membership transition a decoded event records.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrdtMemberChangeKind)]
public enum CrdtMemberChangeKind
{
    /// <summary>
    /// The element was added to the collection by the originating replica.
    /// For an observed-remove set this corresponds to a fresh add dot.
    /// </summary>
    Added = 0,

    /// <summary>
    /// The element was removed (observed-removed) by the originating replica.
    /// For an observed-remove set this corresponds to a tombstoned dot - the
    /// removal cancels only the add dots the remover observed, so a concurrent
    /// add on another replica with a distinct dot still surfaces its own
    /// <see cref="Added"/> event.
    /// </summary>
    Removed = 1,
}
