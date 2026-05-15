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
}
