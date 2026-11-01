namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Verified core for a leaf's live checkpoint advance (issues #5572, #5593): the
/// decision whether a leaf may advance one WAL partition's projection checkpoint
/// over the appends it issued and applied itself, without reading the WAL.
/// <c>BPlusLeafGrain.TryAdvanceLiveCheckpointAsync</c> gathers the inputs and
/// stamps the target this core returns, so the rule <c>WalDurability.tla</c>'s
/// <c>LiveAdvance</c> models is one pure function, pinned by
/// <c>LeafLiveAdvanceCoreTests</c>.
/// <para>
/// Advancing partition <c>p</c> to the highest own offset <c>H</c> claims the
/// leaf holds every entry it must apply at or below <c>H</c>. The leaf knows
/// only its own appends, so the claim rests on two things. Every own append of
/// the activation is applied (<see cref="IsQuiescent"/>). And no entry the leaf
/// must apply lies at or below <c>H</c> other than its own: either the
/// activation created the leaf (born), so nothing it must apply predates it, or
/// the checkpoint has already reached one below the lowest own offset, and an
/// entry the leaf owned from before that point has been read
/// (<see cref="TargetFor"/>). Both rest on the premises the refinement note's
/// gap register states: one activation per leaf, and no foreign author writing
/// a record the leaf owns between its own appends.
/// </para>
/// </summary>
internal static class LeafLiveAdvanceCore
{
    /// <summary>
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
    /// The checkpoint a quiescent leaf may advance one partition to, or
    /// <c>-1</c> when it may not advance that partition.
    /// </summary>
    /// <param name="born">Whether the activation created the leaf (issue #5593).</param>
    /// <param name="ownLow">The lowest own offset applied on the partition; <see cref="long.MaxValue"/> when none.</param>
    /// <param name="ownHigh">The highest own offset applied on the partition; <c>-1</c> when none.</param>
    /// <param name="current">The partition's current checkpoint, pending advance included.</param>
    /// <param name="staleLatched">The partition is latched stale, so its checkpoint must not move.</param>
    /// <returns>The offset to stamp, or <c>-1</c>.</returns>
    public static long TargetFor(bool born, long ownLow, long ownHigh, long current, bool staleLatched)
    {
        if (ownHigh < 0 || staleLatched || ownHigh <= current)
        {
            return -1L;
        }

        return born || current >= ownLow - 1 ? ownHigh : -1L;
    }
}
