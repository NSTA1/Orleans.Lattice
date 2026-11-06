namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Verified core for a leaf's live checkpoint advance (issues #5572, #5593; epic
/// #5605, member 4): the decision whether a leaf may advance one WAL partition's
/// projection checkpoint over the appends it issued and applied itself, without
/// reading the WAL. <c>BPlusLeafGrain.TryAdvanceLiveCheckpointAsync</c> gathers the
/// inputs and stamps the target this core returns, pinned by
/// <c>LeafLiveAdvanceCoreTests</c>.
/// <para>
/// Advancing partition <c>p</c> to the highest own offset <c>H</c> claims the leaf
/// holds every entry it must apply at or below <c>H</c>. The leaf knows only its own
/// appends, so the claim rests on the fences of
/// <c>spec/wal/design/LiveAdvanceDesign.md</c>, whose model is
/// <c>WalLiveAdvance.tla</c>. <see cref="TargetFor"/> is its <c>Armed</c> predicate,
/// conjunct for conjunct:
/// </para>
/// <list type="bullet">
///   <item><description>The activation's claim at <c>p</c> is raised under its
///   current epoch (<c>cstage = 3</c>): any other activation of the leaf is then
///   rejected at <c>p</c>, so nothing above the claim head is a duplicate's.</description></item>
///   <item><description>It has read to the claim head (<c>rp &gt;= claimHead - 1</c>):
///   everything appended before the claim, by any author, has been read.</description></item>
///   <item><description>No hand-off overlapping it is open, its marker state is
///   initialised, and it has read to every overlapping close's heads
///   (<c>~MarkerOpen</c>, <c>mInit</c>, <c>rp &gt;= MarkerClosedAt - 1</c>): a foreign
///   author's records lie below those heads.</description></item>
///   <item><description>It has read past every unstamped offset reported to it
///   (<c>rp &gt;= seenU</c>): an unstamped append below one of its own is reported
///   by that append's response; one above its last own append lies above the
///   target.</description></item>
///   <item><description>Every own append is applied (<see cref="IsQuiescent"/>,
///   <c>\A o \in inflight : author[o] # a</c>).</description></item>
/// </list>
/// </summary>
internal static class LeafLiveAdvanceCore
{    /// <summary>
    /// Whether nothing currently stops a live advance on any partition.
    /// </summary>
    /// <param name="poisoned">The tracker can no longer vouch for every own append.</param>
    /// <param name="unappliedAppends">Own appends issued in the activation whose apply has not been recorded.</param>
    /// <param name="replayBarrierClear">The activation replay has completed, or none was armed.</param>
    /// <param name="replayInFlight">Another replay (a starvation drive) or a warm rescue is in flight.</param>
    /// <param name="sagaPending">
    /// A saga prepare or delta is pending on the leaf. A terminal is appended to
    /// the partition its shard index names, not the prepare's, so this holds
    /// every partition, not only the prepare's.
    /// </param>
    /// <returns><see langword="true"/> when an advance may be considered.</returns>
    public static bool IsQuiescent(
        bool poisoned,
        int unappliedAppends,
        bool replayBarrierClear,
        bool replayInFlight,
        bool sagaPending)
        => !poisoned
            && unappliedAppends == 0
            && replayBarrierClear
            && !replayInFlight
            && !sagaPending;

    /// <summary>
    /// The checkpoint a quiescent leaf may advance one partition to, or <c>-1</c> when
    /// it is not armed there (see the type remarks for each condition).
    /// </summary>
    /// <param name="claimRaised">The activation's claim at the partition is raised under its current epoch, and its view of the gate is open.</param>
    /// <param name="claimHead">The claim head: the allocator's next offset when the claim was raised.</param>
    /// <param name="handOffClear">The leaf's marker state is initialised and no hand-off overlapping it is open.</param>
    /// <param name="closeHead">The highest head any closed hand-off overlapping the leaf recorded at the partition; <c>-1</c> when none.</param>
    /// <param name="unstampedSeen">The highest unstamped offset reported to the activation at the partition, its own included; <c>-1</c> when none.</param>
    /// <param name="ownHigh">The highest own offset applied on the partition; <c>-1</c> when none.</param>
    /// <param name="current">The partition's current checkpoint, pending advance included: the leaf has read every entry it owns at or below it.</param>
    /// <param name="staleLatched">The partition is latched stale, so its checkpoint must not move.</param>
    /// <returns>The offset to stamp, or <c>-1</c>.</returns>
    public static long TargetFor(
        bool claimRaised,
        long claimHead,
        bool handOffClear,
        long closeHead,
        long unstampedSeen,
        long ownHigh,
        long current,
        bool staleLatched)
    {
        if (ownHigh < 0 || staleLatched || ownHigh <= current)
        {
            return -1L;
        }

        var armed = claimRaised
            && handOffClear
            && current >= claimHead - 1
            && current >= closeHead - 1
            && current >= unstampedSeen;
        return armed ? ownHigh : -1L;
    }

    /// <summary>
    /// The claimless advance's target at a partition (#5837, <c>FreeTarget</c>): the
    /// highest own applied offset there, fixed from the tracker's snapshot before the
    /// marker view is read, so every own append it passes predates the read.
    /// </summary>
    /// <param name="ownHighSnapshot">The tracker's snapshot, per partition.</param>
    /// <param name="partition">The partition.</param>
    public static long FreeTarget(long[] ownHighSnapshot, int partition)
    {
        ArgumentNullException.ThrowIfNull(ownHighSnapshot);
        return partition >= 0 && partition < ownHighSnapshot.Length ? ownHighSnapshot[partition] : -1L;
    }

    /// <summary>
    /// The claimless advance's commit (#5837, <c>FreeCommit</c>): the target
    /// <paramref name="freeTarget"/>, or -1 when the leaf may not advance there without a
    /// claim. It may iff the marker state is initialised with no hand-off open
    /// (<paramref name="handOffClear"/>), and the read position is at or above every close
    /// head, the bound its latest stamped response reported - the foreign high, the shared
    /// watermark and, while a donor close is pending, its stale high, all folded into
    /// <paramref name="reportedBound"/> - and below the target. A partition with no report
    /// covering every pending close passes no <paramref name="reportedBound"/> and never
    /// advances this way.
    /// </summary>
    public static long TryFreeAdvance(
        bool hasReport,
        long reportedBound,
        bool handOffClear,
        long closeHead,
        long freeTarget,
        long current,
        bool staleLatched) =>
        hasReport
            ? TargetFor(
                claimRaised: true,
                claimHead: reportedBound + 1,
                handOffClear: handOffClear,
                closeHead: closeHead,
                unstampedSeen: reportedBound,
                ownHigh: freeTarget,
                current: current,
                staleLatched: staleLatched)
            : -1L;

    /// <summary>
    /// The bound a claimless advance that failed only on its reported bound must read to
    /// (#5851, a5e2e2ef's ruling 2), or -1. It is
    /// <paramref name="reportedBound"/> iff the advance had a report, no hand-off is open,
    /// the partition is not latched stale, the read position is at or above every close
    /// head and below the target, and it is below the reported bound: every guard of
    /// <see cref="TryFreeAdvance"/> but the bound holds, so a replay to the bound (C's
    /// <c>Replay</c>) is all that stands between the leaf and the advance.
    /// </summary>
    public static long CatchUpBound(
        bool hasReport,
        long reportedBound,
        bool handOffClear,
        long closeHead,
        long freeTarget,
        long current,
        bool staleLatched) =>
        hasReport
        && handOffClear
        && !staleLatched
        && current >= closeHead - 1
        && freeTarget > current
        && current < reportedBound
            ? reportedBound
            : -1L;

    /// <summary>
    /// The read a claimless advance that failed must catch up to (#5851, ruling 2 widened by
    /// a5e2e2ef's W1-W5), or -1. Where every close head is passed, it is
    /// <see cref="CatchUpBound"/>. Where a close head holds, it is that head minus one iff
    /// the hold is by concrete close heads alone: <paramref name="closesConcrete"/> (W3, no
    /// close bearing on the leaf is deferred anywhere), no donor bound is pending
    /// (<paramref name="boundPending"/>, W4) and the reported bound is already passed. That
    /// bound folds the shared unstamped watermark in and cannot be told apart from it, so a
    /// hold by both never catches up here (W4); the split's cut, which reports the two
    /// apart, can (<see cref="SplitCatchUpTarget"/>).
    /// </summary>
    /// <param name="hasReport">The partition has a claimless view covering every pending close.</param>
    /// <param name="reportedBound">The view's bound.</param>
    /// <param name="handOffClear">The marker state is initialised and no hand-off overlapping the leaf is open.</param>
    /// <param name="closeHead">The highest close head bearing on the leaf at the partition.</param>
    /// <param name="closesConcrete">No close bearing on the leaf is deferred at any partition.</param>
    /// <param name="boundPending">A donor bound bearing on the leaf is pending (its sibling's link unwitnessed).</param>
    /// <param name="freeTarget">The claimless advance's target.</param>
    /// <param name="current">The partition's current checkpoint.</param>
    /// <param name="staleLatched">The partition is latched stale.</param>
    public static long CatchUpTarget(
        bool hasReport,
        long reportedBound,
        bool handOffClear,
        long closeHead,
        bool closesConcrete,
        bool boundPending,
        long freeTarget,
        long current,
        bool staleLatched)
    {
        if (current >= closeHead - 1)
        {
            return CatchUpBound(hasReport, reportedBound, handOffClear, closeHead, freeTarget, current, staleLatched);
        }

        return hasReport
            && handOffClear
            && !staleLatched
            && freeTarget > current
            && IsConcreteCloseHold(closeHead, closesConcrete, boundPending)
            && current >= reportedBound
                ? closeHead - 1
                : -1L;
    }

    /// <summary>
    /// The read a donor whose split hint the cut capped must catch up to (#5851, ruling 2
    /// widened by W1-W5), or -1; the caller has already found the hint not raisable. Where
    /// every close head is passed, it is the cut's reported bound,
    /// <c>max(<paramref name="foreignHigh"/>, <paramref name="watermark"/>)</c>. Where a close
    /// head holds, it is <c>max(<paramref name="foreignHigh"/>, closeHead - 1)</c> iff the
    /// hold is by the foreign high - every stale high the cut's reader carried included -
    /// and concrete close heads alone: no close is deferred anywhere (W3), no donor bound
    /// is pending (W4), and the shared watermark is passed (W4).
    /// </summary>
    /// <param name="current">The donor's checkpoint at the partition, read after the drain.</param>
    /// <param name="closeHead">The highest close head bearing on the donor at the partition.</param>
    /// <param name="closesConcrete">No close bearing on the donor is deferred at any partition.</param>
    /// <param name="boundPending">A donor bound bearing on the donor is pending.</param>
    /// <param name="foreignHigh">The cut's foreign high, the carried closes' stale highs folded in.</param>
    /// <param name="watermark">The cut's shared unstamped watermark.</param>
    public static long SplitCatchUpTarget(
        long current,
        long closeHead,
        bool closesConcrete,
        bool boundPending,
        long foreignHigh,
        long watermark)
    {
        if (current >= closeHead - 1)
        {
            var reported = Math.Max(foreignHigh, watermark);
            return current < reported ? reported : -1L;
        }

        return IsConcreteCloseHold(closeHead, closesConcrete, boundPending) && current >= watermark
            ? Math.Max(foreignHigh, closeHead - 1)
            : -1L;
    }

    /// <summary>
    /// Whether a close-head hold may be caught up past (W3, W4): every close is concrete -
    /// a deferred head is the arm's to resolve (G4), never a replay's target - and no donor
    /// bound is pending, whose stale high an unwitnessed sibling may still raise.
    /// </summary>
    private static bool IsConcreteCloseHold(long closeHead, bool closesConcrete, bool boundPending) =>
        closesConcrete
        && !boundPending
        && closeHead > 0
        && closeHead != Grains.HandOffMarkerCore.DeferredHead;
}
