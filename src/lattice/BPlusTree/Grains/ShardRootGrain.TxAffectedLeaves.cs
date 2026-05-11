namespace Orleans.Lattice.BPlusTree.Grains;

// Per-saga affected-leaves tracking for the terminal-mark fan-out.
//
// Background. AppendTxTerminalAsync delivers the saga terminal mark
// via two channels:
//   1. A WAL append (durability — covers leaves that may have
//      deactivated between prepare and terminal; the leaf's
//      ILeafProjection.Apply switch flips the per-leaf pending bucket
//      on activation-time WAL replay).
//   2. A foreground RPC fan-out — best-effort immediate visibility
//      for already-live leaves so a continuous reader observes the
//      post-saga state without waiting for replay.
//
// Naïvely fanning Channel 2 over the entire shard's leaf chain
// activates every leaf in the chain via Orleans grain materialisation,
// which on a wide tree (thousands of leaves) is a significant
// activation-pressure spike for what is, for most leaves, a no-op
// (a leaf with no pending bucket under the saga's transaction id
// short-circuits inside ApplyTxTerminalAsync). The same waste applies
// to ComputeTerminalHlcAsync, which fans GetClockAsync over the same
// chain solely to compute a max HLC — for most leaves the clock is
// irrelevant because they hold no prepare for this saga.
//
// Optimisation. ShardRootGrain is the routing layer between
// LatticeGrain and BPlusLeafGrain. Every prepare-phase per-key write
// in a saga (whether driven source-side by AtomicWriteGrain or
// receiver-side by LatticeGrain.ApplyPreparedSetAsync /
// ApplyPreparedDeleteAsync) routes through SetAsync / DeleteAsync on
// this grain, which lands on a specific leaf via the routing-table
// snapshot. At that exact moment the routing layer knows precisely
// which leaf the prepare touched, so it records the leaf id under
// the saga's transaction id in a per-activation in-memory map.
//
// AppendTxTerminalAsync then consumes this map: when the entry exists
// it fans Channel 2 only to the recorded subset, and ComputeTerminalHlcAsync
// queries clocks only on that subset (the touched leaves are the only
// ones holding prepare HLCs for this saga; untouched leaves contribute
// nothing to the max). When the entry is missing — the shard-root
// deactivated between prepare and terminal, the saga touched no leaves
// on this shard, or the call arrives via a path that bypasses the
// routing layer — the code falls back to walking the full chain, which
// is the pre-optimisation behaviour.
//
// Cross-shard participant tracking (closes the reshard-race orphaning).
// In addition to the per-activation in-memory affected-leaves map, the
// hook also registers this shard with the per-tree TxRegistry as a
// participant in the saga (gated by a per-activation HashSet so the
// RPC fires exactly once per saga, regardless of how many keys it
// prepared on this shard). AtomicWriteGrain.BroadcastTerminalsAsync
// queries the registry for the authoritative participant set and
// unions it into the saga's TouchedShards slot before fanning out
// terminals — so a saga whose prepare-time routing snapshot is stale
// (e.g. an in-flight shard split landing between prepare and execute,
// or between execute and broadcast) still has its terminal delivered
// to every shard that holds a pending bucket. Without this seam, the
// drift-correction pass in BroadcastTerminalsAsync could miss a shard
// that held a bucket but is no longer the routing target for the
// affected keys, orphaning the bucket and causing readers to surface
// the destination's pre-saga value indefinitely.
//
// Lifetime / leak considerations.
//   * The map lives only in the activation; it is not persisted. On
//     shard-root reactivation the map is empty, and AppendTxTerminalAsync
//     falls back to the chain walk for any in-flight saga whose
//     prepares predated the reactivation. This is acceptable because
//     Channel 1 (the WAL durability path) is independent of this map.
//   * Each call to AppendTxTerminalAsync consumes-and-removes the
//     entry under TryConsumeAffectedLeaves, so the steady-state size
//     is bounded by concurrent in-flight sagas × leaves-per-saga
//     (sagas typically touch a small handful of keys, so this is
//     small).
//   * The registry-participant dedup set is bounded by the number of
//     distinct sagas this activation has participated in; entries are
//     not actively GC'd within the activation (the post-terminal
//     ForgetAsync on the registry is the authoritative cleanup). If
//     the shard-root deactivates and reactivates the dedup set is
//     reset, which is correct: a fresh activation has to re-register
//     with the registry on any in-flight saga's next prepare-phase
//     write so the registry's participant set survives reactivation
//     transparently.
//   * If a saga's coordinator fails between recording prepares and
//     calling AppendTxTerminalAsync the entry leaks for the
//     remaining lifetime of this activation — bounded by the
//     activation's collect-age. A future enhancement could add a
//     TTL-based sweep, but the cost is small enough that it is not
//     wired up in this commit.
//
// Tracking gate. The hook records only when the ambient
// LatticeTransactionContext carries a non-empty transaction id AND
// LatticePreparedContext is active. The two-condition gate ensures
// that non-saga writes (which never produce a pending bucket on the
// leaf and never receive a terminal) are not recorded, and that no
// leak can result from a stray transaction id leaking from an outer
// scope.
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Per-activation map from saga transaction id to the set of
    /// leaf grain ids that received a prepare-phase write on this
    /// shard during the saga. The map is in-memory only; on
    /// activation it is empty. See the file-level comment for the
    /// full lifetime/correctness rationale.
    /// </summary>
    private Dictionary<Guid, HashSet<GrainId>>? _affectedLeavesByTx;

    /// <summary>
    /// Per-activation dedup set: every saga txid for which this shard
    /// has already registered as a participant in the per-tree
    /// TxRegistry. Gates the registry RPC inside
    /// <see cref="RecordAffectedLeafIfPreparedAsync"/> so a saga that
    /// prepares <i>K</i> keys on this shard pays exactly one registry
    /// RPC, not <i>K</i>. The set is in-memory only and is reset on
    /// reactivation; see the file-level comment for the correctness
    /// argument under reactivation.
    /// </summary>
    private HashSet<Guid>? _registeredParticipantTxids;

    /// <summary>
    /// Records <paramref name="leafId"/> as a participant in the
    /// ambient saga's prepare phase, when (and only when) both
    /// <see cref="LatticeTransactionContext.Current"/> is non-empty
    /// and <see cref="LatticePreparedContext.Current"/> is true.
    /// Outside that window this is a cheap, allocation-free no-op
    /// that completes synchronously.
    /// <para>
    /// On the first call for a given saga on this shard, this method
    /// also awaits an idempotent
    /// <see cref="ITxRegistryGrain.RegisterParticipantAsync"/> RPC so
    /// the per-tree registry records this shard's participation.
    /// Subsequent calls for the same saga short-circuit on the
    /// per-activation dedup set and complete synchronously.
    /// </para>
    /// <para>
    /// Correctness note. The dedup set is updated <i>before</i> the
    /// awaited RPC and rolled back inside the catch on failure so a
    /// transient failure (e.g. an in-flight grain rebalance, a
    /// storage throttle) does not leave the activation believing the
    /// shard is registered when the registry has no record. A
    /// silent loss here would cause the saga's terminal-broadcast
    /// participant union to omit this shard, reverting to the
    /// stale-snapshot orphaning the registry-participant primitive
    /// is designed to close.
    /// </para>
    /// </summary>
    private async Task RecordAffectedLeafIfPreparedAsync(GrainId leafId)
    {
        if (!LatticePreparedContext.Current)
            return;

        var txid = LatticeTransactionContext.Current;
        if (txid == Guid.Empty)
            return;

        _affectedLeavesByTx ??= [];

        if (!_affectedLeavesByTx!.TryGetValue(txid, out var set))
        {
            set = [];
            _affectedLeavesByTx[txid] = set;
        }
        set.Add(leafId);

        _registeredParticipantTxids ??= [];
        if (!_registeredParticipantTxids.Add(txid))
            return;

        try
        {
            // First prepare for this saga on this shard — register
            // with the per-tree registry so the saga's terminal
            // broadcast can discover us authoritatively, regardless
            // of any routing flips between prepare and broadcast.
            var registry = grainFactory.GetGrain<ITxRegistryGrain>(TreeId);
            await registry.RegisterParticipantAsync(txid, MyShardIndex);
        }
        catch
        {
            // Roll back the dedup gate so a subsequent prepare on
            // this saga retries the registration. A silent loss here
            // would defeat the registry-participant primitive's
            // purpose.
            _registeredParticipantTxids.Remove(txid);
            throw;
        }
    }

    /// <summary>
    /// Removes and returns the affected-leaves set for
    /// <paramref name="transactionId"/>, or <see langword="null"/>
    /// when no entry exists. Used by
    /// <see cref="AppendTxTerminalAsync"/> to drive Channel 2
    /// fan-out and the terminal-HLC computation against the precise
    /// touched subset rather than the full chain. A null return
    /// signals the caller to fall back to the full chain walk.
    /// </summary>
    private HashSet<GrainId>? TryConsumeAffectedLeaves(Guid transactionId)
    {
        if (transactionId == Guid.Empty || _affectedLeavesByTx is null)
            return null;
        return _affectedLeavesByTx.Remove(transactionId, out var set) ? set : null;
    }
}
