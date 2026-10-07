namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Pure, allocation-free decision core for the WAL shard's placement-move
/// protocol: the two seams a move coordinator's <c>QuiesceForMoveAsync</c> races
/// against an active writer. Extracted verbatim from <c>WalShardGrain</c> so the
/// exact production decisions can be driven under systematic (Coyote)
/// interleaving without a silo: a violation the model finds is a violation of the
/// real move path.
/// </summary>
/// <remarks>
/// <para>
/// A placement move copies a source partition's WAL tail to a new backend and
/// then flips the durable placement pin. For the copy to be lossless the source
/// tail must be <b>stable</b> at the moment the coordinator reads its highest
/// offset: no writer may assign a new offset once the move fence is up. The grain
/// enforces this by re-checking the fence and assigning the next offset
/// <b>atomically</b> under its state gate; <see cref="IsAppendAdmitted"/> is that
/// re-check. Reading the fence and assigning the offset in one indivisible step
/// is the load-bearing guard: split them and a writer that observed the fence
/// down can assign an offset after the coordinator captured the tail, stranding
/// the entry the move never copied.
/// </para>
/// <para>
/// <see cref="ShouldAbortStaleQuiesce"/> is the complementary admission check: a
/// coordinator whose expected placement version is older than the version this
/// activation has already resolved must abort without fencing, or it would fence
/// a provider the activation has already moved past.
/// </para>
/// </remarks>
internal static class WalMoveFenceCore
{
    /// <summary>
    /// Decides whether an append may assign the next WAL offset, given the
    /// activation's current move-fence state. Must be evaluated and acted on
    /// atomically with the offset assignment (under the grain's state gate): an
    /// admitted append commits an offset, so a fence raised between the check and
    /// the assignment would be bypassed.
    /// </summary>
    /// <param name="moveFenced">
    /// Whether this activation is currently fenced for an in-progress placement
    /// move.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the append may proceed; <see langword="false"/>
    /// when the fence is up and the append must be refused.
    /// </returns>
    public static bool IsAppendAdmitted(bool moveFenced) => !moveFenced;

    /// <summary>
    /// Decides whether a quiesce request must abort without fencing because its
    /// coordinator expects an older placement version than this activation has
    /// already resolved. A lagging coordinator that fenced here would quiesce the
    /// wrong provider for the move it is planning.
    /// </summary>
    /// <param name="observedPlacementVersion">
    /// The placement version this activation resolved its provider at.
    /// </param>
    /// <param name="expectedPlacementVersion">
    /// The placement version the requesting coordinator expects.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the quiesce must abort; <see langword="false"/>
    /// when it is safe to fence and drain.
    /// </returns>
    public static bool ShouldAbortStaleQuiesce(
        long observedPlacementVersion,
        long expectedPlacementVersion)
        => observedPlacementVersion > expectedPlacementVersion;

    /// <summary>
    /// Decides what a WAL shard activation does about the durable move fence it
    /// read with its placement pin (issue #4525). The in-memory fence dies with
    /// its activation; this decision is what makes every later activation of the
    /// source fenced too. Only an activation that resolved the fence's source key
    /// is affected: once the placement has moved on, a leftover fence is inert.
    /// </summary>
    /// <param name="fence">The partition's durable fence, or <see langword="null"/> when none is held.</param>
    /// <param name="resolvedProviderKey">The provider key the activation resolved from the same pin.</param>
    /// <param name="utcNowTicks">The current UTC time, in <see cref="DateTime.Ticks"/>.</param>
    public static WalMoveFenceActivation EvaluateActivationFence(
        State.WalMoveFence? fence,
        string resolvedProviderKey,
        long utcNowTicks)
    {
        if (fence is null
            || !string.Equals(fence.SourceProviderKey, resolvedProviderKey, StringComparison.Ordinal))
        {
            return WalMoveFenceActivation.Unfenced;
        }
        return utcNowTicks < fence.LeaseExpiresUtcTicks
            ? WalMoveFenceActivation.Fenced
            : WalMoveFenceActivation.ReleaseExpired;
    }

    /// <summary>
    /// Decides a move coordinator's request to raise (<paramref name="renew"/>
    /// <see langword="false"/>) or renew (<see langword="true"/>) its durable fence
    /// on a partition. A renewal never re-creates a fence that has gone: a fence
    /// that lapsed and was released may have let the source serve appends, so the
    /// move must abort. A fence whose source key is not the partition's current
    /// key is inert and treated as absent.
    /// </summary>
    /// <param name="existing">The fence currently held on the partition, if any.</param>
    /// <param name="currentProviderKey">The provider key the partition is placed on now.</param>
    /// <param name="moveId">The requesting move's identity.</param>
    /// <param name="renew">Whether this is a renewal of a fence the move already raised.</param>
    /// <param name="utcNowTicks">The current UTC time, in <see cref="DateTime.Ticks"/>.</param>
    public static WalMoveFenceRaise EvaluateRaise(
        State.WalMoveFence? existing,
        string currentProviderKey,
        string moveId,
        bool renew,
        long utcNowTicks)
    {
        if (existing is null
            || !string.Equals(existing.SourceProviderKey, currentProviderKey, StringComparison.Ordinal))
        {
            return renew ? WalMoveFenceRaise.RefusedReleased : WalMoveFenceRaise.Raise;
        }
        if (string.Equals(existing.MoveId, moveId, StringComparison.Ordinal))
        {
            return WalMoveFenceRaise.Renew;
        }
        if (renew)
        {
            return WalMoveFenceRaise.RefusedReleased;
        }
        return utcNowTicks >= existing.LeaseExpiresUtcTicks
            ? WalMoveFenceRaise.TakeOver
            : WalMoveFenceRaise.RefusedHeldByOtherMove;
    }

    /// <summary>
    /// Decides whether a fence may be released: it must be held by
    /// <paramref name="moveId"/>, and when <paramref name="onlyIfExpired"/> (a
    /// source activation releasing a lapsed fence, rather than the coordinator
    /// aborting) its lease must have passed.
    /// </summary>
    /// <param name="fence">The fence currently held on the partition, if any.</param>
    /// <param name="moveId">The move whose fence is to be released.</param>
    /// <param name="onlyIfExpired">Whether the release is admitted only once the lease has lapsed.</param>
    /// <param name="utcNowTicks">The current UTC time, in <see cref="DateTime.Ticks"/>.</param>
    public static bool IsReleaseAdmitted(
        State.WalMoveFence? fence,
        string moveId,
        bool onlyIfExpired,
        long utcNowTicks)
        => fence is not null
            && string.Equals(fence.MoveId, moveId, StringComparison.Ordinal)
            && (!onlyIfExpired || utcNowTicks >= fence.LeaseExpiresUtcTicks);

    /// <summary>
    /// Decides whether a move may flip a partition's placement: the partition must
    /// still carry the fence that move raised. This is the check that closes issue
    /// #4525. While the fence has been held without a break, every activation of
    /// the source since the final quiesce came up fenced, so the source cannot
    /// hold an acknowledged append the copy missed. Once it has been released,
    /// that is no longer true, and the cutover must not happen. The lease is
    /// deliberately not consulted: a lapsed fence that nobody has released still
    /// guards, because an activation must release it before it serves an append.
    /// </summary>
    /// <param name="fence">The fence currently held on the partition, if any.</param>
    /// <param name="moveId">The flipping move's identity.</param>
    public static bool IsFlipAdmitted(State.WalMoveFence? fence, string moveId)
        => fence is not null && string.Equals(fence.MoveId, moveId, StringComparison.Ordinal);
}
