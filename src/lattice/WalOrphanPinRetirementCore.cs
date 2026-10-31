using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice;

/// <summary>
/// The pure decision core of the WAL GC's orphaned materialiser pin retirement
/// (issues #3101, #3105, #4238, #4246, #5358), shared by the bulk orphan sweep,
/// the starvation drive's retirement and the pin grain's removal fence, and
/// executed by the <c>WalPinRetirementModel</c> Coyote model.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a husk proves the publisher gone, and only at the moment of the
/// read.</b> A leaf's pin registration is birth-gated on a persisted tree id,
/// so a row with no record, or a record with no tree id, establishes that the
/// row was cleared after the pin was published. It does not establish that the
/// grain id stays empty: a purge-cleared leaf can be populated again (a
/// recovery, a reused tree id re-seeding a deterministic root leaf, a bulk load
/// re-using a leaf id), and its birth seeds a block pin under the very same
/// consumer id. The read and the removal are separate steps, so the removal is
/// fenced: <see cref="MayRemove"/> refuses it once the consumer has been merged
/// after the retirement's token was taken (issue #5358).
/// </para>
/// <para>
/// <b>Fail-closed.</b> Every uncertain case keeps the pin: an unreadable read,
/// a consumer id the gate cannot read as a leaf's own, a token from another
/// activation of the pin grain.
/// </para>
/// </remarks>
internal static class WalOrphanPinRetirementCore
{
    /// <summary>
    /// Whether a durable leaf read proves the pin's publisher was cleared after
    /// it published: no record at all, or a record with no tree id.
    /// </summary>
    /// <param name="state">The classification of the pinned leaf's durable row.</param>
    /// <returns><see langword="true"/> for a husk.</returns>
    public static bool IsHusk(WalGcBlockingPinState state) =>
        state is WalGcBlockingPinState.Orphaned or WalGcBlockingPinState.NoDurableState;

    /// <summary>
    /// Decides one pin from the pinned leaf's durable read, the removal gate's
    /// verdict on the consumer id and whether the pass's retirement budget has
    /// room.
    /// </summary>
    /// <param name="state">The classification of the pinned leaf's durable row.</param>
    /// <param name="verdict">The removal gate's verdict on the consumer id.</param>
    /// <param name="budgetAvailable">Whether the pass may attempt another retirement.</param>
    /// <returns>The decision.</returns>
    public static OrphanPinRemovalDecision Decide(
        WalGcBlockingPinState state,
        ConsumerIdVerdict verdict,
        bool budgetAvailable)
    {
        if (!IsHusk(state))
        {
            return state == WalGcBlockingPinState.Unreadable
                ? OrphanPinRemovalDecision.KeepUnreadable
                : OrphanPinRemovalDecision.KeepLive;
        }

        return verdict switch
        {
            ConsumerIdVerdict.LeafPublished => budgetAvailable
                ? OrphanPinRemovalDecision.Retire
                : OrphanPinRemovalDecision.Defer,
            ConsumerIdVerdict.MalformedId => OrphanPinRemovalDecision.RefuseMalformedId,
            ConsumerIdVerdict.AmbiguousPartition => OrphanPinRemovalDecision.RefuseAmbiguousPartition,
            _ => throw new ArgumentOutOfRangeException(nameof(verdict), verdict, "Not a consumer id verdict."),
        };
    }

    /// <summary>
    /// The pin grain's removal fence (issue #5358): a retirement removal may
    /// proceed only on the activation that issued its token, and only when the
    /// consumer has not been merged since the token was taken.
    /// </summary>
    /// <param name="token">The token the retirement took before its read.</param>
    /// <param name="activationEpoch">The pin grain's current activation epoch.</param>
    /// <param name="lastMergedSequence">The sequence of the consumer's last merge on this activation, or <see langword="null"/> when it has not been merged on it.</param>
    /// <returns><see langword="true"/> when the removal may proceed.</returns>
    public static bool MayRemove(in WalMaterialiserPinToken token, Guid activationEpoch, long? lastMergedSequence) =>
        token.Epoch == activationEpoch
        && (lastMergedSequence is not { } merged || merged <= token.Sequence);
}
