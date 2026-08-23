namespace Orleans.Lattice;

/// <summary>
/// Pure, allocation-free decision core for the <b>resumable</b> tail-copy phase
/// of a WAL placement move. Extracted from <c>LatticeAdminGrain</c>'s
/// <c>RunMoveCopyPhasesAsync</c> so the exact production re-drive arithmetic can
/// be driven under systematic (Coyote) interleaving without a silo: a violation
/// the model finds is a violation of the real move path.
/// </summary>
/// <remarks>
/// <para>
/// A WAL move copies a source partition's retained tail (offsets
/// <c>[srcLowest..srcHighest]</c>, dense and offset-preserving) to the target
/// provider before the placement pin is flipped. The copy must be <b>idempotent
/// under re-drive</b>: a coordinator that crashes or aborts mid-copy leaves a
/// prefix already on the target, and the next attempt must resume from exactly
/// where the target left off - never re-appending an offset the target already
/// holds (a duplicate) and never skipping one (a gap). The pin is flipped only
/// after a clean copy, so an interrupted move is always safe to retry.
/// </para>
/// <para>
/// The load-bearing decisions are: (1) the target must be a clean prefix of the
/// source - its highest offset may not exceed the source's, or the two have
/// diverged and the move must abort rather than corrupt the target
/// (<see cref="IsTargetCleanPrefix"/>); and (2) the resume cursor is the higher
/// of the reserved trim floor (<c>srcLowest - 1</c>) and what the target already
/// holds, so the copy re-appends exactly the missing suffix
/// (<see cref="ResumeCursor"/>). Resuming from a fixed point instead - always the
/// floor, or always the source tail - respectively duplicates or strands offsets.
/// </para>
/// </remarks>
internal static class WalMoveResumeCore
{
    /// <summary>
    /// Whether the target is a clean prefix of the source tail and the copy may
    /// proceed: the target's highest retained offset must not exceed the source's
    /// highest. A target that holds an offset beyond the source has diverged (it
    /// is not a prefix of what is being copied), so the move must abort rather
    /// than interleave two histories.
    /// </summary>
    /// <param name="dstHighestBefore">The target partition's current highest offset, or <c>-1</c> when empty.</param>
    /// <param name="srcHighest">The source partition's highest retained offset.</param>
    /// <returns><see langword="true"/> when the target may be extended by the copy.</returns>
    public static bool IsTargetCleanPrefix(long dstHighestBefore, long srcHighest)
        => dstHighestBefore <= srcHighest;

    /// <summary>
    /// The exclusive cursor the resumable copy starts just past: the higher of
    /// the reserved destination trim floor (<paramref name="srcLowest"/> minus
    /// one) and the highest offset the target already holds
    /// (<paramref name="dstHighestBefore"/>). Copying <c>(cursor, srcHighest]</c>
    /// therefore re-appends exactly the suffix the target is missing, whether the
    /// target is empty (resume from the floor) or a prior attempt already landed a
    /// prefix (resume past it).
    /// </summary>
    /// <param name="srcLowest">The source partition's lowest retained offset (its trim floor plus one).</param>
    /// <param name="dstHighestBefore">The target partition's current highest offset, or <c>-1</c> when empty.</param>
    /// <returns>The exclusive offset to resume the copy just past.</returns>
    public static long ResumeCursor(long srcLowest, long dstHighestBefore)
        => Math.Max(srcLowest - 1, dstHighestBefore);

    /// <summary>
    /// Whether the destination trim floor must be reserved before the copy: the
    /// target is empty or sits below the source's retained floor
    /// (<paramref name="srcLowest"/> minus one) and the source has actually
    /// trimmed a prefix (<paramref name="srcLowest"/> is positive). Reserving the
    /// floor keeps the first copied offset contiguous with the source's retained
    /// range.
    /// </summary>
    /// <param name="dstHighestBefore">The target partition's current highest offset, or <c>-1</c> when empty.</param>
    /// <param name="srcLowest">The source partition's lowest retained offset.</param>
    /// <returns><see langword="true"/> when the target trim floor must be reserved first.</returns>
    public static bool NeedsFloorReserve(long dstHighestBefore, long srcLowest)
        => dstHighestBefore < srcLowest - 1 && srcLowest > 0;
}
