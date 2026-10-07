namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The registry's answer to a move coordinator raising or renewing its durable
/// move fence (issue #4525). Decided by <see cref="WalMoveFenceCore.EvaluateRaise"/>.
/// </summary>
internal enum WalMoveFenceRaise
{
    /// <summary>No fence holds the partition: raise this move's fence.</summary>
    Raise,

    /// <summary>This move already holds the fence: extend its lease.</summary>
    Renew,

    /// <summary>Another move's fence has lapsed without a release: replace it with this move's fence.</summary>
    TakeOver,

    /// <summary>Another move holds a live fence on the partition: refuse.</summary>
    RefusedHeldByOtherMove,

    /// <summary>
    /// A renewal found the move's fence gone: it was released after its lease
    /// lapsed, so the source may have served appends the copy never saw. Refuse;
    /// the move must abort rather than cut over.
    /// </summary>
    RefusedReleased,
}
