using Orleans.Concurrency;

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
    /// Atomically captures the registry's
    /// <see cref="SnapshotAsync"/> result together with the matching
    /// <see cref="GetDecisionsRevisionAsync"/> reading, in a single
    /// turn-serialised call. Used by reader-side double-checked
    /// retries in <c>LatticeGrain</c> to feed the cheap revision-probe
    /// stability check without the inter-call skew that a sequential
    /// <c>(SnapshotAsync, GetDecisionsRevisionAsync)</c> pair would
    /// admit (a writer's turn can interleave between the two calls,
    /// producing a snapshot whose dict reflects revision <c>N</c>
    /// alongside a probe reading <c>N+1</c>).
    /// </summary>
    Task<TxRegistrySnapshot> SnapshotWithRevisionAsync();

    /// <summary>
    /// Cheap monotonic-revision probe paired with
    /// <see cref="SnapshotAsync"/>. The returned <see cref="long"/>
    /// changes (strictly increases, by an unspecified amount) on every
    /// mutation of the registry's recorded-decisions map; two probe
    /// values <c>v1</c> and <c>v2</c> taken before and after an
    /// arbitrary work window with <c>v1 == v2</c> are sufficient
    /// evidence that <em>no</em> decision mutation occurred during
    /// the window. Reader-side fast paths in <c>LatticeGrain</c>
    /// (e.g. the double-checked retry in
    /// <c>GetManyAsyncCore</c> / <c>CountAsync</c>) use this to replace
    /// the snap2 dictionary fetch of the double-checked retry with a
    /// cheap version probe: when <c>v1 == v2</c> the snap1 dictionary
    /// is provably still authoritative and the second dictionary
    /// serialization is elided.
    /// <para>
    /// Multi-silo safety: the revision is persisted as part of the
    /// registry's grain state and is observed atomically with each
    /// decision mutation under the registry's single-turn token, so a
    /// reader on any silo issuing this probe gets the same authoritative
    /// view (the probe is still a grain RPC to the single-activation
    /// registry; the saving is the elided dictionary payload, not the
    /// RPC turn itself).
    /// </para>
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> so the probe
    /// bypasses the registry's per-turn token entirely. Under heavy
    /// saga workloads the registry's main RPCs
    /// (<c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> /
    /// <c>ForgetAsync</c>) hold the turn for the duration of their
    /// state-write await; without interleave the probe queues behind
    /// every in-flight saga decision, defeating the whole point of the
    /// cheap-probe optimisation. Interleave is safe because the writer
    /// performs the in-memory Decisions mutation AND the revision bump
    /// synchronously before its first await
    /// (<c>state.WriteStateAsync()</c>), so a probe that observes the
    /// post-bump revision necessarily also observes the post-mutation
    /// in-memory dictionary; conversely a probe that interleaves
    /// before the writer started its turn observes both the pre-bump
    /// revision and the pre-mutation dict. There is no observable
    /// window where the two diverge. Aligned <see cref="long"/> reads
    /// are JIT-atomic on every supported runtime architecture and the
    /// surrounding Task continuation establishes the memory barrier
    /// needed to see the most recent committed write.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<long> GetDecisionsRevisionAsync();

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
    /// Bulk-register variant of <see cref="RegisterParticipantAsync"/>:
    /// inserts every distinct entry of <paramref name="shardIndices"/>
    /// into the saga's participant set under a single
    /// <c>WriteStateAsync</c> turn. Per-shard
    /// <see cref="RegisterParticipantAsync"/> calls that arrive
    /// subsequently observe their slot already populated and
    /// short-circuit without writing state.
    /// <para>
    /// Used by the saga coordinator to pre-register the touched-shard
    /// set captured at <c>PrepareAsync</c> time, collapsing what would
    /// otherwise be N per-shard registration RPCs (each with its own
    /// <c>WriteStateAsync</c> against the per-tree registry, which is
    /// non-reentrant and forces them to serialise) into one bulk
    /// write. The per-shard <see cref="RegisterParticipantAsync"/>
    /// path remains the authoritative drift-correction primitive: it
    /// still runs from each <c>ShardRootGrain.RecordAffectedLeafIfPreparedAsync</c>
    /// call and covers the case where a key's prepare lands on a
    /// shard outside the saga's pre-registered set (e.g. an in-flight
    /// shard split arriving between Prepare and Execute).
    /// </para>
    /// <para>
    /// Idempotent: a duplicate bulk call (e.g. on saga reminder
    /// replay) is a no-op when every requested index is already
    /// present in the participant set. The implementation only writes
    /// state when at least one index was newly added; an empty or
    /// fully-redundant <paramref name="shardIndices"/> list is a free
    /// in-memory check.
    /// </para>
    /// </summary>
    Task RegisterParticipantsAsync(Guid txid, IReadOnlyList<int> shardIndices);

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

    /// <summary>
    /// Records the arrival of a single cross-cluster terminal record
    /// for the saga identified by <paramref name="txid"/>. Used by the
    /// receiver-side replication apply path
    /// (<see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/>) to
    /// gate the per-tree linearization mark until every per-source-shard
    /// terminal of the saga has been observed, so a reader concurrent
    /// with cross-cluster replication of a multi-shard
    /// <c>SetManyAtomicAsync</c> never observes a strict subset of the
    /// saga's keys at the new value.
    /// <para>
    /// Idempotent on duplicate-delivery retries: a repeat call with the
    /// same <paramref name="sourceShardIndex"/> is a no-op and does not
    /// double-count the arrival. Adopts
    /// <c>max(seen, expectedShardCount)</c> for the saga's expected
    /// total so a producer-side mid-saga shadow-forward split that
    /// grows the touched-shard set between successive per-shard
    /// terminals never under-counts. The same gate applies to commit
    /// and abort terminals - a saga's outcome is either committed by
    /// every per-source-shard terminal or aborted by every
    /// per-source-shard terminal; a mixed-outcome arrival sequence
    /// throws <see cref="InvalidOperationException"/> via the same
    /// invariant that protects <see cref="MarkCommittedAsync"/> /
    /// <see cref="MarkAbortedAsync"/>.
    /// </para>
    /// <para>
    /// When <paramref name="expectedShardCount"/> is <c>0</c> (a legacy
    /// producer that pre-dates this gate), the call short-circuits to
    /// the legacy "mark on first terminal" semantic by reporting the
    /// arrival as <see cref="TerminalTallyResult.IsFinal"/> immediately
    /// - preserving today's best-effort cross-cluster atomic-visibility
    /// contract during rolling upgrades.
    /// </para>
    /// </summary>
    /// <param name="txid">Source saga's transaction id.</param>
    /// <param name="sourceShardIndex">
    /// The authoring-cluster's shard index from which this terminal was
    /// shipped. Used as the dedup key for the tally - the receiver's
    /// own shard layout is irrelevant.
    /// </param>
    /// <param name="committed">
    /// <c>true</c> for commit terminals, <c>false</c> for abort
    /// terminals.
    /// </param>
    /// <param name="expectedShardCount">
    /// The producer-stamped <c>AtomicShardCount</c> on the incoming
    /// terminal record. <c>0</c> means the producer did not stamp a
    /// gate (legacy peer); a positive value names the total number of
    /// distinct source-shard terminals the saga shipped.
    /// </param>
    Task<TerminalTallyResult> RecordTerminalArrivalAsync(
        Guid txid,
        int sourceShardIndex,
        bool committed,
        int expectedShardCount);

    /// <summary>
    /// Installs a snapshot pin against the registry. Every txid in
    /// <paramref name="txids"/> is held back from tombstone-prune
    /// physical removal for the lifetime of the pin (the union of every
    /// unexpired pin's txid set is the "do not prune" predicate
    /// consulted by <c>PruneExpired</c>). The pin's
    /// <see cref="State.SnapshotPin.ExpiresAt"/> is set to
    /// <c>now + ttl</c>; a cursor refreshes its pin on every step via
    /// <see cref="RefreshPinAsync"/>, and a pin past <c>ExpiresAt</c> is
    /// evicted by the next prune pass regardless of subsequent
    /// <see cref="RefreshPinAsync"/> attempts.
    /// <para>
    /// Throws <see cref="LatticeCursorRegistryPinExhaustedException"/>
    /// when accepting the pin would push the registry-wide union of
    /// pinned txids over
    /// <see cref="LatticeOptions.MaxPinnedSagaDecisions"/>. The new pin
    /// is not recorded; existing pins are untouched.
    /// </para>
    /// <para>
    /// Idempotent on repeated calls with the same <paramref name="pinId"/>:
    /// the second call replaces the prior pin's txid set and expiry
    /// timestamp wholesale, which matches the cursor-open code path
    /// (a single cursor only ever installs one pin under one pinId).
    /// </para>
    /// </summary>
    Task PinSnapshotAsync(Guid pinId, IReadOnlyCollection<Guid> txids, TimeSpan ttl);

    /// <summary>
    /// Slides the <see cref="State.SnapshotPin.ExpiresAt"/> on the pin
    /// identified by <paramref name="pinId"/> to <c>now + ttl</c>.
    /// Returns <c>true</c> when the pin was found and refreshed;
    /// returns <c>false</c> when the pin has already been evicted
    /// (either by an explicit <see cref="UnpinSnapshotAsync"/> or by
    /// expiry past its prior <c>ExpiresAt</c>). The cursor grain
    /// surfaces a <c>false</c> result as
    /// <see cref="LatticeCursorSnapshotExpiredException"/> on its next
    /// step.
    /// </summary>
    Task<bool> RefreshPinAsync(Guid pinId, TimeSpan ttl);

    /// <summary>
    /// Removes the pin identified by <paramref name="pinId"/>. A
    /// missing pinId is a safe no-op (the pin may have been expired by
    /// the prune pass before the cursor's close call arrived).
    /// </summary>
    Task UnpinSnapshotAsync(Guid pinId);

    /// <summary>
    /// Returns the registry-wide union of pinned saga txids - the size
    /// of the set the prune pass holds back from physical removal.
    /// Exposed for diagnostics and observability (the
    /// <c>orleans.lattice.registry.snapshot.pin_count</c> gauge reads
    /// this).
    /// </summary>
    Task<int> GetPinnedDecisionCountAsync();
}

/// <summary>
/// Outcome of a single
/// <see cref="ITxRegistryGrain.RecordTerminalArrivalAsync(Guid, int, bool, int)"/>
/// call: did this arrival complete the saga's per-source-shard tally,
/// and (when it did) what outcome should the receiver flip the per-tree
/// linearization mark to. Pure value type - the registry has already
/// persisted the tally state by the time this returns, so a crash
/// between the registry update and the caller's subsequent fan-out is
/// safe (the tally remains gated; a re-delivery of any terminal will
/// re-evaluate and re-issue the flip).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TerminalTallyResult)]
[Immutable]
internal readonly record struct TerminalTallyResult
{
    /// <summary>
    /// <c>true</c> when this arrival was the final per-source-shard
    /// terminal of the saga (i.e. the tallied distinct source-shard
    /// count met or exceeded the producer-stamped
    /// <c>AtomicShardCount</c>), <i>or</i> when the producer did not
    /// stamp a gate (<c>expectedShardCount == 0</c>, legacy producer)
    /// so the gate falls back to "mark on first terminal" semantics.
    /// The receiver's <c>ApplyTxTerminalAsync</c> consults this flag
    /// to decide whether to flip the per-tree linearization mark to
    /// <see cref="FinalOutcome"/>; when <c>false</c>, the receiver
    /// records the arrival but does not flip the mark, leaving the
    /// saga's pending bucket invisible to readers until the remaining
    /// per-shard terminals arrive.
    /// </summary>
    [Id(0)] public bool IsFinal { get; init; }

    /// <summary>
    /// The saga outcome stamped on the most recent arrival, or
    /// <see cref="TxStatus.InFlight"/> when no arrival has been
    /// recorded. Meaningful only when <see cref="IsFinal"/> is
    /// <c>true</c> - the caller flips
    /// <see cref="ITxRegistryGrain.MarkCommittedAsync(Guid)"/> /
    /// <see cref="ITxRegistryGrain.MarkAbortedAsync(Guid)"/> from this
    /// value.
    /// </summary>
    [Id(1)] public TxStatus FinalOutcome { get; init; }

    /// <summary>
    /// The full set of source-shard indices observed for this saga so
    /// far, including the current arrival. Sorted ascending so the
    /// caller can use the result deterministically. Populated only when
    /// <see cref="IsFinal"/> is <c>true</c>; an in-progress tally
    /// returns an empty list to avoid shipping interim state that the
    /// caller will not act on. When the legacy fast path fires
    /// (<c>expectedShardCount == 0</c>), the list contains only the
    /// current arrival's <c>sourceShardIndex</c> - matching the
    /// pre-gate one-terminal-per-arrival fan-out semantic.
    /// <para>
    /// Receivers use this list to drive the per-shard terminal fan-out:
    /// the per-leaf pending-bucket flip MUST be deferred until every
    /// per-source-shard terminal has arrived, because once a leaf
    /// drains its pending bucket the bucket is no longer there for
    /// <c>ResolvePendingStatusAsync</c> to dial back through the
    /// registry. Gating the fan-out on <see cref="IsFinal"/> means a
    /// reader concurrent with the saga's replication observes either
    /// the pre-saga value (registry says InFlight, leaf still has a
    /// pending bucket) or every key at the post-saga value (registry
    /// flipped, leaves drained) - never a split view.
    /// </para>
    /// </summary>
    [Id(2)] public IReadOnlyList<int> ObservedSourceShards { get; init; }
}

/// <summary>
/// Outcome of an atomic-write saga, as recorded by
/// <see cref="ITxRegistryGrain"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxStatus)]
internal enum TxStatus
{
    /// <summary>
    /// No commit/abort decision is currently visible for this saga. The
    /// saga is either still preparing (no <c>MarkCommittedAsync</c> /
    /// <c>MarkAbortedAsync</c> has been issued yet), or its decision
    /// was previously recorded and has been forgotten by
    /// <see cref="ITxRegistryGrain.ForgetAsync(Guid)"/> long enough ago
    /// that the registry's tombstone TTL
    /// (<see cref="LatticeOptions.TxDecisionRetention"/>) has elapsed
    /// and the entry has been pruned. A decision that was forgotten
    /// <i>within</i> the retention window remains queryable as
    /// <see cref="Committed"/> / <see cref="Aborted"/> so concurrent
    /// shard-split sweeps can resolve orphan pending buckets they
    /// install on destination shards after the saga's terminal fan-out.
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
