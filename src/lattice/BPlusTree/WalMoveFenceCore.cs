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
}
