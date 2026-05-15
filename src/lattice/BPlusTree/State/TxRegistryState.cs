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
}
