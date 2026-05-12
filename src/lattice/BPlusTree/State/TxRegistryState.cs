namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TxRegistryGrain"/>. Holds the
/// recorded commit/abort decisions for atomic-write sagas on a single
/// tree, plus the per-saga set of shards that participated in the
/// saga's prepare phase. Entries are added when the saga grain calls
/// <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> (decisions) and
/// when shard roots route prepare-phase writes
/// (<c>RegisterParticipantAsync</c>); both are removed when every
/// touched leaf has applied its terminal (the saga grain's post-fan-out
/// cleanup invokes <c>ForgetAsync</c>). The persisted footprint is
/// therefore bounded by the number of in-flight + recently-completed-/// but-not-yet-cleaned-up sagas, not by the lifetime size of the tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistryState)]
internal sealed class TxRegistryState
{
    /// <summary>
    /// Map from saga txid to its recorded outcome. Sagas that have been
    /// forgotten (post-cleanup) are absent from this map and resolve to
    /// <see cref="TxStatus.InFlight"/> on lookup - by which point no
    /// leaf has the txid in its pending bucket anymore so that
    /// observation is consistent with the absence of any pending
    /// mutation.
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
}
