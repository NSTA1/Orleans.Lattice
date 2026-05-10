using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal apply-side seam used by <c>Orleans.Lattice.Replication</c> to
/// install a remote mutation onto the local tree while preserving the
/// authoring cluster's <see cref="HybridLogicalClock"/> and origin-cluster
/// id verbatim. Unlike the public <see cref="ILattice"/> write surface —
/// which always stamps a fresh local HLC at commit time — these methods
/// route the incoming entry through the LWW-merge path so the persisted
/// <see cref="LwwValue{T}"/> carries the source HLC and source
/// <see cref="LwwValue{T}.OriginClusterId"/> exactly as authored on the
/// remote cluster.
/// </summary>
/// <remarks>
/// <para>
/// Implemented by the per-tree <c>LatticeGrain</c> stateless worker so the
/// existing routing machinery (<see cref="LatticeOptionsResolver"/>,
/// shard-map resolution, system-tree guard) is reused. Apply calls for
/// system-prefixed trees are rejected for the same reason public writes
/// are.
/// </para>
/// <para>
/// Set / Delete apply paths route via
/// <see cref="IShardRootGrain.MergeManyAsync"/> — the same primitive used
/// by shard-split shadow-forward and tree-merge — because that is the
/// only entry point that preserves the source HLC end-to-end. Range
/// applies route via the standard <see cref="IShardRootGrain.DeleteRangeAsync"/>
/// wrapped in a <see cref="LatticeOriginContext"/> scope so the
/// receiver-side observer publishes a <see cref="MutationKind.DeleteRange"/>
/// notification stamped with the remote origin (and is therefore filtered
/// back out by the outbound replication ship loop).
/// </para>
/// </remarks>
[Alias(TypeAliases.IReplicationApplyGrain)]
internal interface IReplicationApplyGrain : IGrainWithStringKey
{
    /// <summary>
    /// Installs a Set mutation authored on the remote cluster identified
    /// by <paramref name="originClusterId"/>. The persisted entry carries
    /// <paramref name="sourceHlc"/> as its <see cref="HybridLogicalClock"/>
    /// timestamp, <paramref name="originClusterId"/> as its
    /// <see cref="LwwValue{T}.OriginClusterId"/>,
    /// <paramref name="sourceVectorClock"/> as its
    /// <see cref="LwwValue{T}.VectorClock"/>, and
    /// <paramref name="expiresAtTicks"/> as its absolute UTC expiry
    /// (<c>0</c> for non-expiring entries).
    /// </summary>
    /// <param name="key">The key the remote write targeted.</param>
    /// <param name="value">The committed value bytes.</param>
    /// <param name="sourceHlc">The HLC stamped by the remote cluster.</param>
    /// <param name="originClusterId">The id of the remote cluster that authored the write.</param>
    /// <param name="sourceVectorClock">
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <c>null</c> when the producing cluster does not stamp a
    /// frontier. Stamped verbatim onto the persisted
    /// <see cref="LwwValue{T}.VectorClock"/> so receiver-side
    /// causal-consistency checks see exactly the producer's view.
    /// </param>
    /// <param name="expiresAtTicks">Absolute UTC tick expiry; <c>0</c> means no expiry.</param>
    Task ApplySetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        long expiresAtTicks);

    /// <summary>
    /// Installs a Delete tombstone authored on the remote cluster
    /// identified by <paramref name="originClusterId"/>. The tombstone is
    /// stamped with <paramref name="sourceHlc"/> so LWW resolution against
    /// concurrent local writes is deterministic across clusters, and with
    /// <paramref name="sourceVectorClock"/> so receiver-side
    /// causal-consistency checks see the producer's frontier verbatim.
    /// </summary>
    /// <param name="key">The key the remote delete targeted.</param>
    /// <param name="sourceHlc">The HLC stamped by the remote cluster.</param>
    /// <param name="originClusterId">The id of the remote cluster that authored the delete.</param>
    /// <param name="sourceVectorClock">
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <c>null</c> when the producing cluster does not stamp a
    /// frontier.
    /// </param>
    Task ApplyDeleteAsync(
        string key,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock);

    /// <summary>
    /// Installs a range delete authored on the remote cluster identified
    /// by <paramref name="originClusterId"/>. The receiver walks the leaf
    /// chain locally and stamps each tombstone with a freshly-ticked local
    /// HLC — range deletes do not carry a single source HLC because the
    /// remote walk produced many per-leaf timestamps. The
    /// <see cref="LatticeOriginContext"/> and
    /// <see cref="LatticeVectorClockContext"/> scopes ensure every
    /// per-leaf tombstone produced by the local walk is stamped with the
    /// remote origin and the remote frontier, so the receiver-side
    /// <see cref="MutationKind.DeleteRange"/> observer publishes a
    /// notification stamped with both pieces of metadata and the outbound
    /// ship loop does not loop the range back to the authoring cluster.
    /// </summary>
    /// <param name="startInclusive">Inclusive start key of the range.</param>
    /// <param name="endExclusive">Exclusive end key of the range.</param>
    /// <param name="originClusterId">The id of the remote cluster that authored the range delete.</param>
    /// <param name="sourceVectorClock">
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <c>null</c> when the producing cluster does not stamp a
    /// frontier. Stamped onto every per-leaf tombstone via the ambient
    /// <see cref="LatticeVectorClockContext"/>.
    /// </param>
    Task ApplyDeleteRangeAsync(
        string startInclusive,
        string endExclusive,
        string originClusterId,
        VersionVector? sourceVectorClock);

    /// <summary>
    /// Installs a batch of LWW Set/Delete mutations authored on remote
    /// clusters in a single grain RPC, collapsing what would otherwise be
    /// one <see cref="ApplySetAsync"/> / <see cref="ApplyDeleteAsync"/>
    /// call per item into a single round-trip per affected shard. Each
    /// item carries its own authoring metadata (<see cref="ApplyMergeItem.SourceHlc"/>,
    /// <see cref="ApplyMergeItem.OriginClusterId"/>, 
    /// <see cref="ApplyMergeItem.SourceVectorClock"/>, 
    /// <see cref="ApplyMergeItem.ExpiresAtTicks"/>) so the persisted
    /// <see cref="LwwValue{T}"/> matches the authoring cluster
    /// bit-identically — semantics are equivalent to invoking
    /// <see cref="ApplySetAsync"/> / <see cref="ApplyDeleteAsync"/> for
    /// each item in order, only with the per-item dictionary allocation
    /// and the per-item shard-RPC fan-out elided.
    /// </summary>
    /// <param name="items">
    /// The remote mutations to install. Items targeting different shards
    /// are dispatched in parallel; items targeting the same shard are
    /// merged into a single <see cref="IShardRootGrain.MergeManyAsync"/>
    /// call per shard.
    /// </param>
    Task ApplyMergeManyAsync(IReadOnlyList<ApplyMergeItem> items);

    /// <summary>
    /// Installs a batch of remote mutations atomically as a saga, so the
    /// entire batch either commits to the local tree or rolls back as a
    /// unit. Unlike <see cref="ApplyMergeManyAsync"/> — which collapses
    /// per-key shard fan-out for steady-state throughput but commits
    /// each key independently — this seam dispatches the batch through
    /// the existing <see cref="IAtomicWriteGrain"/> saga keyed by
    /// <c>{treeId}/{transactionId}</c>, providing transactional
    /// all-or-nothing semantics on the receiver side. Each entry's
    /// authoring metadata (<see cref="AtomicApplyEntry.Timestamp"/>,
    /// <see cref="AtomicApplyEntry.VectorClock"/>,
    /// <see cref="AtomicApplyEntry.ExpiresAtTicks"/>) is preserved
    /// verbatim so the persisted <see cref="LwwValue{T}"/> matches the
    /// authoring cluster bit-identically.
    /// </summary>
    /// <param name="applyEntries">
    /// The per-key remote mutations to install. Each entry carries its
    /// own source HLC and frontier so the saga's per-step dispatch
    /// stamps the leaf grain's persisted entry with the source-side
    /// metadata exactly as authored.
    /// </param>
    /// <param name="transactionId">
    /// Caller-supplied idempotency key. Resubmissions with the same id
    /// re-attach to the original saga and inherit its terminal outcome
    /// without re-executing the writes. Must be a non-empty
    /// <see cref="Guid"/>.
    /// </param>
    /// <param name="originClusterId">
    /// The id of the remote cluster that authored the batch. Stamped
    /// onto every per-key write via the ambient
    /// <see cref="LatticeOriginContext"/> so the receiver-side observer
    /// publishes mutations bearing the remote origin (and the outbound
    /// ship loop filters them back out).
    /// </param>
    /// <param name="sourceVectorClock">
    /// The saga-wide vector-clock frontier captured by the remote
    /// cluster at commit time, used to seed the saga's
    /// <see cref="LatticeMutation.VectorClock"/> stamp during the
    /// Prepare-phase pre-saga capture. Per-entry
    /// <see cref="AtomicApplyEntry.VectorClock"/> values take precedence
    /// during each per-key dispatch.
    /// </param>
    /// <param name="cancellationToken">Token to cancel the call.</param>
    /// <returns>
    /// An <see cref="AtomicApplyResult"/> carrying the saga's terminal
    /// <see cref="AtomicApplyOutcome"/> — <see cref="AtomicApplyOutcome.Committed"/>
    /// when every entry committed, <see cref="AtomicApplyOutcome.Compensated"/>
    /// when any entry failed and the saga rolled back. The structured
    /// return shape lets callers branch on the outcome rather than
    /// catching the local saga's terminal exception.
    /// </returns>
    Task<AtomicApplyResult> ApplyManyAtomicAsync(
        IReadOnlyList<AtomicApplyEntry> applyEntries,
        Guid transactionId,
        string originClusterId,
        VersionVector? sourceVectorClock,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Installs a single saga prepare-phase Set authored on a remote
    /// cluster into this tree's per-leaf pending-transaction map. The
    /// receiver leaf sees the same ambient context stack the source
    /// saga's prepare step would have produced
    /// (<see cref="LatticePreparedContext"/>, 
    /// <see cref="LatticeOriginContext"/>,
    /// <see cref="LatticeVectorClockContext"/>, 
    /// <see cref="LatticeHlcOverrideContext"/>, 
    /// <see cref="LatticeAtomicBatchContext"/>, and the
    /// <c>LatticeTransactionContext</c> request-scope value), so the
    /// resulting per-leaf entry routes into the leaf's
    /// <c>_pendingTx[transactionId]</c> bucket and bears the source
    /// cluster's HLC, origin cluster id, vector-clock frontier, and
    /// atomic-batch coordinates verbatim.
    /// </summary>
    /// <param name="key">The key the source saga prepared.</param>
    /// <param name="value">The committed value bytes.</param>
    /// <param name="sourceHlc">The HLC stamped by the source cluster on the prepare.</param>
    /// <param name="originClusterId">The id of the source cluster that authored the prepare.</param>
    /// <param name="sourceVectorClock">The source-side vector-clock frontier captured at prepare time.</param>
    /// <param name="expiresAtTicks">Absolute UTC tick expiry for TTL'd entries; <c>0</c> means no expiry.</param>
    /// <param name="transactionId">The source saga's transaction id; routes the entry into the receiver leaf's pending bucket. Must not be <see cref="Guid.Empty"/>.</param>
    /// <param name="atomicBatchSize">The source saga's batch size; stamped onto the receiver-side <see cref="LatticeMutation.AtomicBatchSize"/>.</param>
    /// <param name="atomicBatchIndex">The source saga's per-step batch index; stamped onto the receiver-side <see cref="LatticeMutation.AtomicBatchIndex"/>.</param>
    Task ApplyPreparedSetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        long expiresAtTicks,
        Guid transactionId,
        int atomicBatchSize,
        int atomicBatchIndex);

    /// <summary>
    /// Installs a single saga prepare-phase Delete authored on a remote
    /// cluster into this tree's per-leaf pending-transaction map. The
    /// receiver leaf sees the same ambient context stack the source
    /// saga's prepare step would have produced (see
    /// <see cref="ApplyPreparedSetAsync"/> for the full list), so the
    /// resulting tombstone routes into the leaf's
    /// <c>_pendingTx[transactionId]</c> bucket and bears the source
    /// cluster's HLC, origin cluster id, vector-clock frontier, and
    /// atomic-batch coordinates verbatim.
    /// </summary>
    /// <param name="key">The key the source saga prepared a delete for.</param>
    /// <param name="sourceHlc">The HLC stamped by the source cluster on the prepare.</param>
    /// <param name="originClusterId">The id of the source cluster that authored the prepare.</param>
    /// <param name="sourceVectorClock">The source-side vector-clock frontier captured at prepare time.</param>
    /// <param name="transactionId">The source saga's transaction id; routes the entry into the receiver leaf's pending bucket. Must not be <see cref="Guid.Empty"/>.</param>
    /// <param name="atomicBatchSize">The source saga's batch size; stamped onto the receiver-side <see cref="LatticeMutation.AtomicBatchSize"/>.</param>
    /// <param name="atomicBatchIndex">The source saga's per-step batch index; stamped onto the receiver-side <see cref="LatticeMutation.AtomicBatchIndex"/>.</param>
    Task ApplyPreparedDeleteAsync(
        string key,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        Guid transactionId,
        int atomicBatchSize,
        int atomicBatchIndex);

    /// <summary>
    /// Installs a saga terminal mark (commit or abort) authored on a
    /// remote cluster onto this tree, marking the per-tree
    /// <see cref="ITxRegistryGrain"/> with the saga outcome (the
    /// tree-wide linearization point readers dial back through) and
    /// driving the per-shard
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> under a
    /// <see cref="LatticeHlcOverrideContext"/> + 
    /// <see cref="LatticeOriginContext"/> stack so the receiver's local
    /// WAL append re-stamps the source cluster's terminal HLC and
    /// origin verbatim. Idempotent on repeated delivery via the
    /// per-leaf <c>_recentlyTerminal</c> dedup and the registry's
    /// repeat-same-outcome no-op. Pairs with
    /// <see cref="ApplyPreparedSetAsync"/> /
    /// <see cref="ApplyPreparedDeleteAsync"/> to deliver cross-cluster
    /// atomic visibility on the receiver side.
    /// </summary>
    /// <param name="transactionId">The source saga's transaction id. Must not be <see cref="Guid.Empty"/>.</param>
    /// <param name="committed"><c>true</c> for commit terminals; <c>false</c> for abort terminals.</param>
    /// <param name="shardIndex">The source-shard index the terminal applies to. Maps to the receiver's same-numbered shard root.</param>
    /// <param name="terminalHlc">The HLC the source cluster stamped on the terminal record. Re-stamped verbatim on the receiver via the ambient HLC override.</param>
    /// <param name="originClusterId">The id of the source cluster that authored the terminal.</param>
    /// <param name="cancellationToken">Token to cancel the call.</param>
    Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        int shardIndex,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        CancellationToken cancellationToken = default);
}
