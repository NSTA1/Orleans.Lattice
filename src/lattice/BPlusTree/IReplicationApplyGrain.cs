using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal apply-side seam used by <c>Orleans.Lattice.Replication</c> to
/// install a remote mutation onto the local tree while preserving the
/// authoring cluster's <see cref="HybridLogicalClock"/> and origin-cluster
/// id verbatim. Unlike the public <see cref="ILattice"/> write surface -
/// which always stamps a fresh local HLC at commit time - these methods
/// route the incoming entry through the LWW-merge path so the persisted
/// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> carries the source HLC and source
/// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.OriginClusterId"/> exactly as authored on the
/// remote cluster.
/// </summary>
/// <remarks>
/// <para>
/// Implemented by the per-tree <c>LatticeGrain</c> stateless worker so the
/// existing routing machinery (<see cref="Orleans.Lattice.BPlusTree.LatticeOptionsResolver"/>,
/// shard-map resolution, system-tree guard) is reused. Apply calls for
/// system-prefixed trees are rejected for the same reason public writes
/// are.
/// </para>
/// <para>
/// Set / Delete apply paths route via
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/> - the same primitive used
/// by shard-split shadow-forward and tree-merge - because that is the
/// only entry point that preserves the source HLC end-to-end. Range
/// applies route via the standard <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.DeleteRangeAsync"/>
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
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.OriginClusterId"/>,
    /// <paramref name="sourceVectorClock"/> as its
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.VectorClock"/>, and
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
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.VectorClock"/> so receiver-side
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
    /// chain locally and stamps every per-leaf tombstone with
    /// <paramref name="sourceHlc"/> via the
    /// <see cref="LatticeHlcOverrideContext"/> scope, preserving the
    /// cross-origin LWW invariant: a DeleteRange authored at frontier
    /// <c>T</c> cannot overwrite a foreign-origin write whose HLC is
    /// strictly greater than <c>T</c>. The
    /// <see cref="LatticeOriginContext"/> and
    /// <see cref="LatticeVectorClockContext"/> scopes additionally ensure
    /// every per-leaf tombstone is stamped with the remote origin and
    /// the remote frontier, so the receiver-side
    /// <see cref="MutationKind.DeleteRange"/> observer publishes a
    /// notification stamped with both pieces of metadata and the
    /// outbound ship loop does not loop the range back to the authoring
    /// cluster.
    /// </summary>
    /// <param name="startInclusive">Inclusive start key of the range.</param>
    /// <param name="endExclusive">Exclusive end key of the range.</param>
    /// <param name="sourceHlc">
    /// The producer-side issue HLC stamped on every per-leaf tombstone.
    /// When this value is <see cref="HybridLogicalClock.Zero"/> (the
    /// wire-default produced by a legacy peer that pre-dates this
    /// parameter), the receiver falls back to a freshly-ticked local
    /// HLC for back-compat - the cross-origin LWW invariant is not
    /// preserved in that mode and operators should upgrade producers.
    /// </param>
    /// <param name="originClusterId">The id of the remote cluster that authored the range delete.</param>
    /// <param name="sourceVectorClock">
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <c>null</c> when the producing cluster does not stamp a
    /// frontier. Stamped onto every per-leaf tombstone via the ambient
    /// <see cref="LatticeVectorClockContext"/>.
    /// </param>
    /// <param name="explicitMatchedKeys">
    /// The explicit set of keys a predicate-filtered range delete matched at
    /// the authoring cluster, or <c>null</c> for an unconditional range delete.
    /// When non-<c>null</c> the receiver tombstones exactly this set (each key
    /// routed to its owning shard) instead of re-deriving membership from the
    /// range bounds, so a conditional delete reproduces the producer's tombstone
    /// closure without re-evaluating the predicate against the receiver's
    /// (possibly divergent) values.
    /// </param>
    Task ApplyDeleteRangeAsync(
        string startInclusive,
        string endExclusive,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        IReadOnlyList<string>? explicitMatchedKeys = null);

    /// <summary>
    /// Installs a batch of LWW Set/Delete mutations authored on remote
    /// clusters in a single grain RPC, collapsing what would otherwise be
    /// one <see cref="ApplySetAsync"/> / <see cref="ApplyDeleteAsync"/>
    /// call per item into a single round-trip per affected shard. Each
    /// item carries its own authoring metadata (<see cref="ApplyMergeItem.SourceHlc"/>,
    /// <see cref="ApplyMergeItem.OriginClusterId"/>, 
    /// <see cref="ApplyMergeItem.SourceVectorClock"/>, 
    /// <see cref="ApplyMergeItem.ExpiresAtTicks"/>) so the persisted
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> matches the authoring cluster
    /// bit-identically - semantics are equivalent to invoking
    /// <see cref="ApplySetAsync"/> / <see cref="ApplyDeleteAsync"/> for
    /// each item in order, only with the per-item dictionary allocation
    /// and the per-item shard-RPC fan-out elided.
    /// </summary>
    /// <param name="items">
    /// The remote mutations to install. Items targeting different shards
    /// are dispatched in parallel; items targeting the same shard are
    /// merged into a single <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>
    /// call per shard.
    /// </param>
    Task ApplyMergeManyAsync(IReadOnlyList<ApplyMergeItem> items);

    /// <summary>
    /// Installs a batch of non-prepared typed-CRDT delta mutations
    /// authored on remote clusters in a single grain RPC, collapsing what
    /// would otherwise be one read-merge-write (optimistic-concurrency)
    /// apply per item into a single round-trip that folds every delta into
    /// the receiver's current visible state inside one grain turn. Each
    /// item carries its own authoring metadata
    /// (<see cref="ApplyCrdtDeltaItem.SourceHlc"/>,
    /// <see cref="ApplyCrdtDeltaItem.OriginClusterId"/>,
    /// <see cref="ApplyCrdtDeltaItem.SourceVectorClock"/>) and its typed
    /// delta + <see cref="ApplyCrdtDeltaItem.Mode"/>, so the folded
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> matches the per-entry CRDT apply path
    /// bit-identically.
    /// <para>
    /// Because the grain is single-threaded and non-reentrant, the whole
    /// batch folds without interleaving any other apply or local write to
    /// this tree, so the per-entry optimistic-concurrency retry loop the
    /// historical path required is unnecessary. CRDT folds are commutative,
    /// associative, and idempotent, so the batch is equivalent to invoking
    /// the per-entry CRDT apply for each item in order, with the per-item
    /// grain round-trip and full-state read-merge-write elided.
    /// </para>
    /// </summary>
    /// <param name="items">
    /// The remote typed-CRDT deltas to fold. Every item's
    /// <see cref="ApplyCrdtDeltaItem.Mode"/> must be a CRDT mode (never
    /// <see cref="LatticeMergeMode.LwwRegister"/>).
    /// </param>
    Task ApplyCrdtDeltaManyAsync(IReadOnlyList<ApplyCrdtDeltaItem> items);

    /// <summary>
    /// Off-batch per-entry fallback for a steady-state delta-carrying CRDT
    /// entry that also carries a per-entry absolute expiry. Mirrors the
    /// public <see cref="Orleans.Lattice.ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], System.Threading.CancellationToken)"/>
    /// seam the non-expiry per-entry path uses (the receiver advances its own
    /// clock and stamps the ambient remote origin), but folds the merged row's
    /// <paramref name="expiresAtTicks"/> under the max-absolute-ticks expiry
    /// join so a TTL'd CRDT write expires on this replica too. The expiry is an
    /// absolute UTC <see cref="System.DateTime.Ticks"/> value applied verbatim
    /// (never re-resolved from a relative TTL), keeping it strictly convergent
    /// across replicas. An <paramref name="expiresAtTicks"/> of <c>0</c> leaves
    /// any existing expiry unchanged.
    /// </summary>
    /// <param name="key">The key the remote CRDT delta targeted.</param>
    /// <param name="mode">The CRDT convergence rule to fold the delta under.</param>
    /// <param name="deltaBytes">The remote typed CRDT delta bytes.</param>
    /// <param name="expiresAtTicks">
    /// The absolute UTC expiry tick to fold onto the merged row, or <c>0</c>
    /// for a durable entry.
    /// </param>
    Task ApplyCrdtDeltaWithExpiryAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, long expiresAtTicks);

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
    /// <param name="delta">
    /// The typed CRDT delta the source saga staged for this key, or
    /// <see langword="null"/> for a plain last-writer-wins prepared write.
    /// When present (and <paramref name="mode"/> is a CRDT mode), the
    /// receiver folds this delta into its current visible state on the
    /// saga's terminal commit instead of installing <paramref name="value"/>
    /// verbatim, so two clusters writing the same CRDT key concurrently
    /// through staged atomic writes converge by the per-replica typed-delta
    /// union rather than by last-writer-wins of their merged states.
    /// </param>
    /// <param name="mode">
    /// The source tree's merge mode for this key. <see cref="LatticeMergeMode.LwwRegister"/>
    /// (the default) keeps the entry on the byte-for-byte unchanged LWW
    /// prepared path; any CRDT mode routes the terminal commit through the
    /// typed-delta fold.
    /// </param>
    Task ApplyPreparedSetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        long expiresAtTicks,
        Guid transactionId,
        int atomicBatchSize,
        int atomicBatchIndex,
        byte[]? delta = null,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister);

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
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.AppendTxTerminalAsync"/> under a
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
    /// <param name="atomicShardCount">
    /// Producer-stamped count of distinct source-shard terminals the
    /// enclosing saga ships - i.e. the size of the saga's authoritative
    /// participant union at terminal-broadcast time. The receiver uses
    /// this value to gate the per-tree
    /// <see cref="ITxRegistryGrain"/> linearization mark until every
    /// per-source-shard terminal of the saga has been observed, so a
    /// reader concurrent with cross-cluster replication of a
    /// multi-shard <c>SetManyAtomicAsync</c> never observes a strict
    /// subset of the saga's keys at the new value. Defaults to
    /// <c>0</c>, which selects the legacy "mark on first terminal"
    /// semantic - the same behaviour as a legacy producer that does
    /// not stamp the gate. Production receivers
    /// (<c>ReplicationApplier.ApplyTxTerminalCoreAsync</c>) always pass
    /// the producer-stamped <see cref="WalRecord.AtomicShardCount"/>
    /// verbatim; the default exists for back-compat with unit tests
    /// that exercise the apply seam directly without going through the
    /// replication wire.
    /// </param>
    /// <param name="crossTreeOperationId">
    /// The cross-tree operation id when this terminal belongs to a
    /// multi-tree atomic write, else <c>null</c>. When set, the receiver
    /// defers the per-tree linearization mark and fan-out to a
    /// <see cref="ILatticeCrossTreeReceiverGrain"/> barrier so every
    /// replicated participating tree on this receiver flips visible
    /// together - preserving the authoring cluster's all-or-nothing
    /// cross-tree visibility guarantee on the receiver side. A
    /// <c>null</c> value selects the legacy single-tree path (mark +
    /// fan out as soon as the per-shard gate completes).
    /// </param>
    /// <param name="crossTreeWaitSet">
    /// The participant tree-ids of the cross-tree batch that are
    /// replicated on <i>this</i> receiver
    /// (<c>participants ∩ trees-replicated-here</c>). Computed by the
    /// applier and passed to the receiver barrier as its wait set, so a
    /// cross-tree batch that spans trees not replicated here still flips
    /// atomically on the subset that is present (partial-replication
    /// batches are valid). Ignored when <paramref name="crossTreeOperationId"/>
    /// is <c>null</c>.
    /// </param>
    /// <param name="cancellationToken">Token to cancel the call.</param>
    Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        int shardIndex,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        int atomicShardCount = 0,
        string? crossTreeOperationId = null,
        IReadOnlyList<string>? crossTreeWaitSet = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Materializes one tree's slice of a decided cross-tree atomic write on
    /// this receiver: marks this tree's <see cref="ITxRegistryGrain"/> with the
    /// global verdict and fans the terminal out to the tree's leaves. Called by
    /// the <see cref="ILatticeCrossTreeReceiverGrain"/> barrier's coordinating
    /// <c>LatticeGrain</c> (after <c>NotifyTerminalAsync</c> returns a decision)
    /// for every <i>sibling</i> participating tree; the coordinating tree
    /// materializes its own slice inline. Idempotent on redelivery (the registry
    /// repeat-same-outcome no-op and per-leaf terminal dedup), so re-invoking it
    /// for an already-finalized tree is safe.
    /// </summary>
    /// <param name="transactionId">The replicated sub-saga's transaction id on this tree.</param>
    /// <param name="committed">The global cross-tree verdict to record on this tree.</param>
    /// <param name="observedSourceShards">The source-shard indices seeding the terminal fan-out.</param>
    /// <param name="terminalHlc">The source cluster's terminal HLC, re-stamped verbatim on fan-out.</param>
    /// <param name="originClusterId">The id of the source cluster that authored the terminal.</param>
    /// <param name="cancellationToken">Token to cancel the call.</param>
    Task FinalizeCrossTreeTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyList<int> observedSourceShards,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        CancellationToken cancellationToken = default);
}
