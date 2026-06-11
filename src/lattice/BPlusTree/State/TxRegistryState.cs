namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TxRegistryGrain"/>. Holds the
/// recorded commit/abort decisions for atomic-write sagas on a single
/// tree, plus the per-saga set of shards that participated in the
/// saga's prepare phase. Entries are added when the saga grain calls
/// <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> (decisions) and
/// when shard roots route prepare-phase writes
/// (<c>RegisterParticipantAsync</c>); decisions are tombstoned via the
/// <see cref="ForgottenAt"/> map when the saga grain calls
/// <c>ForgetAsync</c> (post-fan-out cleanup), and physically removed
/// after their tombstone TTL elapses. The persisted footprint is
/// therefore bounded by the number of in-flight + tombstoned-within-TTL
/// sagas, not by the lifetime size of the tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistryState)]
internal sealed class TxRegistryState
{
    /// <summary>
    /// Map from saga txid to its recorded outcome. A saga whose decision
    /// has been forgotten (post-cleanup) remains in this map for the
    /// duration of <c>LatticeOptions.TxDecisionRetention</c> with a
    /// matching entry in <see cref="ForgottenAt"/>; once the tombstone
    /// TTL elapses the entry is dropped from both maps and subsequent
    /// lookups resolve to <see cref="TxStatus.InFlight"/>. By the time
    /// a decision is physically dropped, no leaf can still have the
    /// txid in its pending bucket (every touched leaf applied its
    /// terminal during the saga's broadcast fan-out, and any orphan
    /// pending bucket installed by a concurrent shard-split sweep is
    /// drained by the sweep's own post-sweep cleanup pass before the
    /// tombstone TTL elapses), so the absence is consistent with the
    /// absence of any pending mutation.
    /// </summary>
    [Id(0)] public Dictionary<Guid, TxStatus> Decisions { get; set; } = [];

    /// <summary>
    /// Map from saga txid to the set of physical shard indices that
    /// routed a prepare-phase write under that saga. Populated by
    /// shard-root <c>RecordAffectedLeafIfPrepared</c> hooks during
    /// prepare; queried by <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>
    /// so the terminal fan-out reaches every shard that holds a
    /// pending bucket regardless of subsequent routing flips.
    /// <para>
    /// The persisted shape is <see cref="HashSet{T}"/> for set semantics
    /// on insert/idempotency. Cleared by <c>ForgetAsync</c> alongside
    /// the decision entry so the persisted footprint stays bounded by
    /// in-flight + recently-completed sagas.
    /// </para>
    /// <para>
    /// Wire-compatibility: this slot was added after the registry
    /// shipped with only <see cref="Decisions"/>. A legacy persisted
    /// state with no Id-1 slot decodes the property to an empty
    /// dictionary, which is the correct semantic default
    /// (zero recorded participants for every txid).
    /// </para>
    /// </summary>
    [Id(1)] public Dictionary<Guid, HashSet<int>> Participants { get; set; } = [];

    /// <summary>
    /// Tombstone timestamps for decisions that have been forgotten by
    /// their saga but whose outcome must remain queryable for a bounded
    /// window so concurrent shard-split sweeps can drain orphan pending
    /// buckets they install on destination shards after the saga's
    /// terminal fan-out completed. Populated by
    /// <see cref="Grains.TxRegistryGrain.ForgetAsync(Guid)"/>; drained
    /// by the inline prune pass once
    /// <c>UtcNow - ForgottenAt[txid] &gt; options.TxDecisionRetention</c>.
    /// A subsequent <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c>
    /// on a tombstoned txid clears the tombstone and re-records the
    /// decision, mirroring the pre-tombstone "forget-then-remark"
    /// semantic.
    /// <para>
    /// Wire-compatibility: this slot was added after the registry
    /// shipped with only <see cref="Decisions"/> and
    /// <see cref="Participants"/>. A legacy persisted state with no
    /// Id-2 slot decodes the property to an empty dictionary, which
    /// is the correct semantic default ("no tombstones yet").
    /// </para>
    /// </summary>
    [Id(2)] public Dictionary<Guid, DateTimeOffset> ForgottenAt { get; set; } = [];

    /// <summary>
    /// Per-saga tally of distinct source-cluster shard indices whose
    /// cross-cluster terminal records have arrived on this receiver.
    /// Used by <c>LatticeGrain.ApplyTxTerminalAsync</c> to gate the
    /// per-tree linearization mark
    /// (<see cref="Decisions"/> [txid] = commit/abort) until every
    /// per-source-shard terminal of the saga has been observed, so a
    /// reader concurrent with cross-cluster replication of a multi-shard
    /// <c>SetManyAtomicAsync</c> never observes a strict subset of the
    /// saga's keys at the new value. The tally is keyed by
    /// <c>(receiver-side TreeId, txid, sourceShardIndex)</c>; the
    /// receiver-side shard layout is independent of the source's
    /// (adaptive splits and operator resize on either side can diverge
    /// the counts) and the gate only consults the producer-stamped
    /// source-side count.
    /// <para>
    /// Cleared by <c>ForgetAsync</c> alongside the decision entry once
    /// the saga's terminal fan-out is complete, so the persisted
    /// footprint stays bounded.
    /// </para>
    /// <para>
    /// Wire-compatibility: legacy persisted state with no Id-3 slot
    /// decodes to an empty dictionary, which is the correct semantic
    /// default (no terminals tallied yet).
    /// </para>
    /// </summary>
    [Id(3)] public Dictionary<Guid, HashSet<int>> TerminalArrivals { get; set; } = [];

    /// <summary>
    /// Per-saga snapshot of the producer-stamped
    /// <c>AtomicShardCount</c> (largest value observed across every
    /// terminal record of the saga that has arrived on this receiver
    /// so far). Used together with <see cref="TerminalArrivals"/> to
    /// decide whether a per-tree linearization mark is yet safe to
    /// flip: a saga whose tallied terminal count meets or exceeds
    /// <c>ExpectedTerminals[txid]</c> has had every per-source-shard
    /// terminal observed, so the gate flips and the per-tree
    /// <see cref="Decisions"/> entry is recorded.
    /// <para>
    /// Adopts <c>max(seen, incoming)</c> on each arrival so that a
    /// producer-side mid-saga shadow-forward split (which grows the
    /// source-side touched-shard set between successive per-shard
    /// terminals) is absorbed without ever under-counting. A receiver
    /// that observes a fully-tallied saga and later receives a
    /// duplicate-delivery retry of one of its terminals is a safe
    /// no-op (the registry's decision-repeat path is idempotent).
    /// </para>
    /// <para>
    /// Wire-compatibility: legacy persisted state with no Id-4 slot
    /// decodes to an empty dictionary, which is the correct semantic
    /// default (no expected-terminal count recorded yet).
    /// </para>
    /// </summary>
    [Id(4)] public Dictionary<Guid, int> ExpectedTerminals { get; set; } = [];

    /// <summary>
    /// Per-cursor pin records protecting a point-in-time saga-decision
    /// snapshot captured by <see cref="LatticeCursorSpec.PointInTime"/>
    /// cursor opens. Each entry's <see cref="SnapshotPin.Txids"/> set is
    /// honoured by the registry's prune pass: a tombstoned decision
    /// whose txid is in the union of every unexpired pin's <c>Txids</c>
    /// is held back from physical removal even when its tombstone TTL
    /// has elapsed. Pins themselves are evicted from this map when
    /// their <see cref="SnapshotPin.ExpiresAt"/> elapses, or explicitly
    /// via <c>UnpinSnapshotAsync</c>.
    /// <para>
    /// Wire-compatibility: legacy persisted state with no Id-5 slot
    /// decodes to an empty dictionary, which is the correct semantic
    /// default (no active pins).
    /// </para>
    /// </summary>
    [Id(5)] public Dictionary<Guid, SnapshotPin> SnapshotPins { get; set; } = [];

    /// <summary>
    /// Monotonic revision counter bumped on every successful mutation
    /// of <see cref="Decisions"/>. Used by reader-side fast paths in
    /// <c>LatticeGrain</c> (e.g. <c>GetManyAsyncCore</c>) to replace
    /// the snap2 dictionary fetch of the double-checked retry with a
    /// cheap version probe: when the reader observes the same revision
    /// before and after its fan-out, snap1 is provably still
    /// authoritative and no second dictionary serialization is needed.
    /// <para>
    /// Bumped under the registry grain's single-turn token in the same
    /// step that mutates <see cref="Decisions"/>, so a reader observing
    /// <c>revision = N</c> is guaranteed to also observe every
    /// decision change that produced <c>revision = N</c> in any
    /// subsequent <c>SnapshotAsync</c> on the same grain. Any mutation
    /// of <see cref="Decisions"/> bumps the counter, including Aborted
    /// transitions and tombstone-driven physical removals - this is
    /// strictly conservative (the reader's fall-through path re-runs
    /// the existing <c>IsSnapshotStable</c> check on a freshly fetched
    /// snap2, which already filters to Committed transitions).
    /// </para>
    /// <para>
    /// Wire-compatibility: legacy persisted state with no Id-6 slot
    /// decodes to <c>0L</c>. A reactivated grain whose persisted
    /// <see cref="Decisions"/> is non-empty but whose persisted
    /// <c>DecisionsRevision</c> is 0 (migration path) seeds the
    /// revision to <c>1</c> on first activation so newly-arriving
    /// readers observe a non-zero value before the first post-migration
    /// mutation; the revision is opaque (only equality / inequality
    /// matter) so the seed value is arbitrary.
    /// </para>
    /// </summary>
    [Id(6)] public long DecisionsRevision { get; set; }

    /// <summary>
    /// Per-saga delegation records for cross-tree atomic writes: maps a
    /// sub-saga's txid to the key of the
    /// <see cref="Grains.LatticeCrossTreeTxGrain"/> that owns the single global
    /// commit/abort decision for the cross-tree batch. Populated when a
    /// participating tree's saga parks in
    /// <see cref="AtomicWritePhase.Prepared"/> (via
    /// <c>RegisterExternalDecisionAuthorityAsync</c>). While a txid is present
    /// here and has no terminal entry in <see cref="Decisions"/>, every status
    /// query resolves it by dialling the coordinator's
    /// <c>GetDecisionAsync</c> rather than reading the local map - so the
    /// cross-tree batch becomes visible on this tree at the exact moment the
    /// coordinator records its decision, never before. Once the coordinator's
    /// verdict is terminal the registry caches it into <see cref="Decisions"/>
    /// (bumping <see cref="DecisionsRevision"/>) and drops the delegation entry,
    /// so subsequent reads resolve locally. Also dropped by
    /// <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> (the sub-saga's
    /// finalize records the authoritative local decision) and by
    /// <c>ForgetAsync</c>.
    /// <para>
    /// Wire-compatibility: legacy persisted state with no Id-7 slot decodes to
    /// an empty dictionary, which is the correct semantic default (no cross-tree
    /// delegations - every saga resolves purely from <see cref="Decisions"/>).
    /// </para>
    /// </summary>
    [Id(7)] public Dictionary<Guid, string> ExternalAuthorities { get; set; } = [];

    /// <summary>
    /// Per-saga delegation records for the <b>receiver side</b> of a cross-tree
    /// atomic write: maps a replicated sub-saga's txid to the compound key
    /// (<c>originClusterId</c> + <c>operationId</c>) of the
    /// <see cref="Grains.LatticeCrossTreeReceiverGrain"/> that owns the single
    /// global commit/abort decision for the cross-tree batch <i>on this
    /// receiver cluster</i>. Distinct from <see cref="ExternalAuthorities"/>
    /// (which resolves to the authoring-cluster
    /// <see cref="Grains.LatticeCrossTreeTxGrain"/>): the receiver never hosts
    /// the authoring coordinator, so it delegates to a local receiver
    /// coordinator instead. Populated when a cross-tree terminal's per-shard
    /// gate completes on the receiver (via
    /// <c>RegisterReceiverDecisionAuthorityAsync</c>), strictly <i>before</i>
    /// the receiver notifies that coordinator, so no reader can resolve one
    /// participating tree committed while the last tree is still legacy-local.
    /// While a txid is present here with no terminal entry in
    /// <see cref="Decisions"/>, status queries resolve it by dialling the
    /// receiver coordinator's <c>GetDecisionAsync</c> - so every participating
    /// tree on the receiver flips visible at the single instant the receiver
    /// coordinator's wait set completes, never partially. The receiver
    /// coordinator's deferred materialization (<c>FinalizeCrossTreeTerminalAsync</c>)
    /// later records the authoritative local decision via
    /// <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c>, which drops this
    /// entry. Also dropped by <c>ForgetAsync</c>.
    /// <para>
    /// Wire-compatibility: legacy persisted state with no Id-8 slot decodes to
    /// an empty dictionary (no receiver delegations).
    /// </para>
    /// </summary>
    [Id(8)] public Dictionary<Guid, string> ReceiverDecisionAuthorities { get; set; } = [];
}
