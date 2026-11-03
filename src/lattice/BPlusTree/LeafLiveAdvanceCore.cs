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
}