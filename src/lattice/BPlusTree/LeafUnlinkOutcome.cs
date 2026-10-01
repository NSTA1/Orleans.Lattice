namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Why a predecessor accepted or refused to unlink its successor
/// (<c>IBPlusLeafGrain.TryUnlinkSuccessorAsync</c>).
/// <para>
/// This replaced a <see langword="bool"/>, and the reason is issue #2160's
/// reopen. Three genuinely different conditions decline that call, and the
/// caller could not tell them apart, so every one of them was reported by a
/// single log line that named only the first: <em>"its predecessor no longer
/// points at it, so a split landed underneath the reclaim"</em>. For two of
/// the three that sentence is simply untrue.
/// </para>
/// <para>
/// <b>The cost of that was not cosmetic.</b> A production audit of a tree
/// accumulating orphaned leaves searched 13 MB of container logs for that line,
/// found zero occurrences, and concluded from its absence that the split guard
/// was never firing - when the line is emitted at Debug, which a deployed
/// container does not enable, and would have been the wrong line in any case.
/// A diagnostic that collapses distinct causes into one message does not merely
/// fail to help; it supports a confident wrong inference, which is worse than
/// silence.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafUnlinkOutcome)]
internal enum LeafUnlinkOutcome
{
    /// <summary>
    /// The successor was unlinked and its range absorbed, in one persist.
    /// </summary>
    Unlinked = 0,

    /// <summary>
    /// This leaf no longer points at the successor the caller named, so a
    /// division landed between the two AFTER the caller built its plan. The
    /// compare half of the compare-and-swap.
    /// </summary>
    DeclinedPredecessorMoved = 1,

    /// <summary>
    /// This leaf is mid-division INTO the successor the caller named, so the
    /// caller built its plan after the division's intent landed. The opposite
    /// ordering to <see cref="DeclinedPredecessorMoved"/>, and invisible to it:
    /// the pointer comparison agrees, because the division itself set the
    /// pointer. See issue #2160.
    /// </summary>
    DeclinedSplitInFlight = 2,

    /// <summary>
    /// This leaf carries a moved-away seal, so widening it onto the vacated
    /// range would make it the legitimate owner of keys its own read gate
    /// refuses to serve. A question about the PREDECESSOR rather than about the
    /// successor. See issue #2143 and <c>HasWidenBlockingState</c>.
    /// </summary>
    DeclinedWidenSealed = 3,
}
