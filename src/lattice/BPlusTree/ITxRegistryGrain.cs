namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Per-tree saga decision registry. Acts as the single tree-wide
/// linearization point for atomic-write saga commit/abort decisions.
/// <para>
/// During an atomic-write saga the per-leaf prepared mutations sit in
/// each touched leaf's pending-tx bucket and are hidden from readers
/// by the leaf read-path filter. Once every prepare has succeeded the
/// saga grain calls <see cref="MarkCommittedAsync"/> on this registry
/// <b>before</b> beginning the commit-terminal fan-out. Conversely,
/// when the saga aborts mid-execute the saga grain calls
/// <see cref="MarkAbortedAsync"/> before fanning out abort terminals.
/// The registry write is the single moment at which the saga becomes
/// visible to readers tree-wide; the subsequent terminal fan-out is
/// best-effort lazy garbage collection of the per-leaf pending bucket.
/// </para>
/// <para>
/// Readers in <c>BPlusLeafGrain</c> consult the registry whenever a
/// requested key has an entry in the leaf's pending-tx bucket: a
/// <see cref="TxStatus.Committed"/> outcome surfaces the prepared
/// (post-saga) value, while both <see cref="TxStatus.Aborted"/> and
/// <see cref="TxStatus.InFlight"/> fall through to the pre-saga value
/// in <c>LeafNodeState.Entries</c>. Treating <c>InFlight</c> as
/// equivalent to <c>Aborted</c> at read time is the strict-isolation
/// rule that gives the saga its all-or-nothing visibility: until the
/// registry has flipped to <c>Committed</c>, the prepared bucket is
/// invisible and readers continue to see the pre-saga state.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
[Alias(TypeAliases.ITxRegistryGrain)]
internal interface ITxRegistryGrain : IGrainWithStringKey
{
    /// <summary>
    /// Atomically records that the saga identified by <paramref name="txid"/>
    /// has committed. Idempotent: repeated calls with the same
    /// <paramref name="txid"/> are no-ops. Throws
    /// <see cref="InvalidOperationException"/> if the saga was previously
    /// recorded as <see cref="TxStatus.Aborted"/>.
    /// </summary>
    Task MarkCommittedAsync(Guid txid);

    /// <summary>
    /// Atomically records that the saga identified by <paramref name="txid"/>
    /// has aborted. Idempotent: repeated calls with the same
    /// <paramref name="txid"/> are no-ops. Throws
    /// <see cref="InvalidOperationException"/> if the saga was previously
    /// recorded as <see cref="TxStatus.Committed"/>.
    /// </summary>
    Task MarkAbortedAsync(Guid txid);

    /// <summary>
    /// Returns the recorded outcome for <paramref name="txid"/>. Returns
    /// <see cref="TxStatus.InFlight"/> when no decision has been recorded
    /// (the saga is still preparing or has been forgotten via
    /// <see cref="ForgetAsync"/>).
    /// </summary>
    Task<TxStatus> GetStatusAsync(Guid txid);

    /// <summary>
    /// Batched form of <see cref="GetStatusAsync"/>. Returns a map from
    /// every requested <paramref name="txids"/> to its current status
    /// (<see cref="TxStatus.InFlight"/> for unknown ids).
    /// </summary>
    Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(IReadOnlyList<Guid> txids);

    /// <summary>
    /// Returns a snapshot of every recorded saga decision currently in
    /// the registry. Used by the lattice-level read fan-out path to
    /// capture a single tree-wide view of decisions before parallel
    /// per-shard scans, ensuring every leaf in the same scan applies
    /// the same registry decision view (linearizable scan over the
    /// registry's transition moment). Decisions not present in the
    /// returned map default to <see cref="TxStatus.InFlight"/> at the
    /// caller - consistent with "decision not yet recorded as of this
    /// snapshot's wall-clock moment".
    /// </summary>
    Task<Dictionary<Guid, TxStatus>> SnapshotAsync();

    /// <summary>
    /// Drops the recorded outcome for <paramref name="txid"/>. Called
    /// after every touched leaf has applied its terminal so the
    /// registry's persisted footprint stays bounded. After this call
    /// <see cref="GetStatusAsync"/> returns <see cref="TxStatus.InFlight"/>
    /// - by which point no leaf has the txid in its pending bucket
    /// anymore so that observation is consistent with the absence of
    /// any pending mutation.
    /// </summary>
    Task ForgetAsync(Guid txid);

    /// <summary>
    /// Idempotently records that the shard identified by
    /// <paramref name="shardIndex"/> routed at least one prepare-phase
    /// write under the saga identified by <paramref name="txid"/>.
    /// Repeated calls with the same <paramref name="txid"/> /
    /// <paramref name="shardIndex"/> pair are no-ops and do not
    /// trigger a state write.
    /// <para>
    /// Each <c>ShardRootGrain</c> calls this exactly once per saga it
    /// participates in (gated by a per-activation dedup set), so a
    /// saga touching <i>N</i> shards produces at most <i>N</i>
    /// registry writes regardless of how many keys it prepared per
    /// shard. The participant set is queried at terminal-broadcast
    /// time by <see cref="GetParticipantsAsync"/> so the fan-out
    /// reaches every shard that holds a pending bucket, even when
    /// routing flips between the prepare and broadcast windows
    /// (e.g. an in-flight shard split landing mid-saga).
    /// </para>
    /// <para>
    /// Cleared by <see cref="ForgetAsync"/> alongside the decision
    /// entry so the persisted footprint stays bounded by in-flight +
    /// recently-completed sagas.
    /// </para>
    /// </summary>
    Task RegisterParticipantAsync(Guid txid, int shardIndex);

    /// <summary>
    /// Returns the sorted set of physical shard indices that have
    /// registered as participants in the saga identified by
    /// <paramref name="txid"/>. Returns an empty list when no
    /// participant has registered (the saga touched no shards on this
    /// tree, or its participants have already been forgotten via
    /// <see cref="ForgetAsync"/>). The result is sorted ascending so
    /// callers can use it as a deterministic broadcast target list.
    /// </summary>
    Task<IReadOnlyList<int>> GetParticipantsAsync(Guid txid);
}

/// <summary>
/// Outcome of an atomic-write saga, as recorded by
/// <see cref="ITxRegistryGrain"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxStatus)]
public enum TxStatus
{
    /// <summary>
    /// No commit/abort decision has been recorded for this saga (it is
    /// still preparing, or its decision has been forgotten after every
    /// touched leaf applied its terminal).
    /// </summary>
    InFlight = 0,

    /// <summary>
    /// The saga committed. Any leaf that has the saga in its pending
    /// bucket should surface the prepared (post-saga) value to readers.
    /// </summary>
    Committed = 1,

    /// <summary>
    /// The saga aborted. Any leaf that has the saga in its pending
    /// bucket should surface the pre-saga value to readers.
    /// </summary>
    Aborted = 2,
}
