namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One prepared mutation covering a key, as seen by
/// <see cref="AtomicVisibilityGate.SelectDecidingPrepare"/>.
/// </summary>
/// <param name="Status">The owning saga's outcome under the read's registry view.</param>
/// <param name="AlreadyTerminal">Whether the leaf has already applied that saga's terminal.</param>
/// <param name="SupersededByRow">
/// Whether the leaf's committed row for the key is a non-migrated row whose
/// timestamp dominates the prepared value's, so the saga's commit drain would skip
/// the key and leave that row in place.
/// </param>
/// <param name="Timestamp">The prepared value's hybrid logical clock timestamp.</param>
internal readonly record struct PreparedCandidate(
    TxStatus Status,
    bool AlreadyTerminal,
    bool SupersededByRow,
    HybridLogicalClock Timestamp);
