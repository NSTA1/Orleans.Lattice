using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Coordinator grain that drives a single online shard consolidation for a
/// tree: the inverse of an adaptive split. It folds one physical donor shard's
/// virtual slots back onto an adjacent physical survivor shard and retires the
/// donor from the routing map, reducing the tree's physical shard count
/// without taking any shard offline or quiescing the tree.
/// <para>
/// One coordinator activation per <c>(tree, donor-shard)</c> pair, so a driver
/// can heal an over-split tree by running several folds at once up to
/// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/>.
/// </para>
/// <para>
/// <b>Driver seam.</b> The four members below are deliberately a start / poll
/// / cancel / idle surface rather than an all-in-one background loop, so an
/// automated healing orchestrator owns the scheduling policy - which pairs, in
/// what order, how many at a time, and when to stop - while this grain owns
/// only the correctness of a single fold. Every member is idempotent and every
/// phase transition is persisted, so a driver may crash, restart and re-issue
/// the same calls without corrupting an in-flight operation.
/// </para>
/// <para>
/// Key format: <c>{treeId}/{donorShardIndex}</c>.
/// </para>
/// </summary>
[Alias(TypeAliases.ITreeShardConsolidationGrain)]
internal interface ITreeShardConsolidationGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts an online consolidation folding this coordinator's donor shard
    /// onto <paramref name="survivorShardIndex"/>. Returns as soon as the
    /// intent is persisted; the phase machine then runs in the background,
    /// anchored by a reminder so it survives silo restarts.
    /// <para>
    /// Idempotent in three distinct ways, all of which a driver relies on:
    /// re-issuing the call for the same survivor while the fold is in flight
    /// is a no-op; issuing it for a donor that already owns no virtual slot is
    /// a no-op because the pair is already consolidated; and issuing it for a
    /// <em>different</em> survivor while a fold is in flight is refused with
    /// <see cref="InvalidOperationException"/> rather than silently
    /// re-targeting.
    /// </para>
    /// </summary>
    /// <param name="survivorShardIndex">Physical shard index that absorbs the donor's slots.</param>
    Task StartAsync(int survivorShardIndex);

    /// <summary>
    /// Runs the consolidation forward by phases until it completes, is
    /// cancelled, or reaches a phase whose bounded work is not yet exhausted.
    /// Used by tests and by a driver that wants to push a fold along rather
    /// than wait for the background timer. No-op when nothing is in flight.
    /// </summary>
    Task RunConsolidationPassAsync();

    /// <summary>
    /// Returns a point-in-time <see cref="ShardConsolidationProgress"/> for
    /// this donor shard, derived entirely from persisted coordinator state so
    /// it is stable across reactivation. Safe to poll.
    /// </summary>
    Task<ShardConsolidationProgress> GetProgressAsync();

    /// <summary>
    /// Requests that an in-flight consolidation stop.
    /// <para>
    /// The request is honoured at the next phase boundary strictly before
    /// <see cref="ShardConsolidationPhase.Swap"/>, where the tree's routing is
    /// still untouched: the donor's migration record is cleared and the tree
    /// is left exactly as it was, with the survivor holding a harmless
    /// LWW-converged copy of already-drained entries. Once the routing map has
    /// flipped the request is recorded but deliberately not acted on, because
    /// abandoning after the flip would strand the donor mid-retirement. A
    /// driver can therefore always ask to stop and can never tear the tree by
    /// asking.
    /// </para>
    /// <para>
    /// Returns <see langword="true"/> when the request was accepted for an
    /// in-flight operation that can still be abandoned, and
    /// <see langword="false"/> when there was nothing to cancel or the
    /// operation is already past the point of no return.
    /// </para>
    /// </summary>
    Task<bool> CancelAsync();

    /// <summary>
    /// Returns <see langword="true"/> when the coordinator is idle - either no
    /// consolidation has ever been started for this donor, or the last one has
    /// finished or been abandoned. Returns <see langword="false"/> while a fold
    /// is in flight.
    /// </summary>
    Task<bool> IsIdleAsync();
}
