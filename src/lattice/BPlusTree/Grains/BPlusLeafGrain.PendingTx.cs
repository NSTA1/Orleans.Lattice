using System.Diagnostics;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-leaf in-memory pending-transaction map for the saga
/// reader-isolation primitive. Prepared mutations route here instead of
/// the visible projection until the saga's terminal mark
/// (<see cref="MutationKind.TxCommit"/> or
/// <see cref="MutationKind.TxAbort"/>) flips or drops them.
/// <para>
/// Strictly in-memory: under the WAL-as-sole-commit-point model the WAL
/// is the durable record, and the pending-tx map is rebuilt
/// deterministically on activation from the WAL replay. Reads filter
/// pending entries via a local hash lookup with zero RPC cost.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Cached empty outcome map returned by
    /// <see cref="SnapshotPendingForReadAsync"/> on the steady-state
    /// path where the leaf has never participated in a saga since
    /// activation. The vast majority of read fan-outs hit this path;
    /// sharing a single empty instance avoids one zero-content
    /// dictionary allocation per leaf per scan. Callers only ever do
    /// <c>TryGetValue</c> against the returned map - never mutate it -
    /// so it is safe to share the instance across calls and across
    /// leaves.
    /// </summary>
    private static readonly Dictionary<Guid, TxStatus> EmptyOutcomes = new();

    /// <summary>
    /// Cached empty pending-key map returned by
    /// <see cref="SnapshotPendingForReadAsync"/> on the steady-state
    /// path. Same rationale and safety contract as
    /// <see cref="EmptyOutcomes"/>.
    /// </summary>
    private static readonly Dictionary<string, (Guid txid, LwwValue<byte[]> value)> EmptyPendingKeys = new();

    /// <summary>
    /// Keyed by <see cref="LatticeMutation.TransactionId"/> -&gt; key
    /// -&gt; the prepared <see cref="LwwValue{T}"/>. Entries here are
    /// invisible to readers until a matching terminal mark surfaces; on
    /// <see cref="MutationKind.TxCommit"/> every value is merged into
    /// <c>state.State.Entries</c> via
    /// <see cref="LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>; on
    /// <see cref="MutationKind.TxAbort"/> every value is dropped.
    /// <para>
    /// Lazily allocated on the first prepared-mutation apply. The vast
    /// majority of leaves never participate in a saga, so an upfront
    /// allocation per activation would be pure waste - leaf activation
    /// density is the dominant memory-cost knob and the dict's empty
    /// footprint (~80 B) multiplied across thousands of activations is
    /// not free.
    /// </para>
    /// </summary>
    private Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>? _pendingTx;

    /// <summary>
    /// Per-transaction earliest WAL offset of any prepared mutation
    /// recorded under that transaction id. Populated when the replay
    /// coordinator drives <c>ILeafProjection.Apply</c> with a
    /// <see cref="LatticeApplyOffsetContext"/> scope active; left
    /// untouched on the foreground commit path (where there is no WAL
    /// offset to stamp). The minimum value across this map is the
    /// projection-checkpoint clamp floor - advancing the persisted
    /// checkpoint past <c>min - 1</c> would silently lose any prepare
    /// whose terminal mark has not yet replayed, so
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> clamps
    /// requested advances back to that floor.
    /// <para>
    /// Lazily allocated on the first prepared-mutation apply that
    /// carries an ambient offset. The vast majority of leaves never
    /// participate in a saga or are not driven by the replay
    /// coordinator, so an upfront allocation per activation would be
    /// pure waste - see the rationale on <see cref="_pendingTx"/>.
    /// </para>
    /// </summary>
    private Dictionary<Guid, long>? _pendingTxOffsets;

    /// <summary>
    /// Idempotency dedup set. Populated as terminal marks replay so a
    /// re-applied <see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/> for the same transaction id is
    /// a no-op rather than crashing on a missing pending bucket.
    /// Survives only as long as the activation; rebuilt by the replay
    /// coordinator on next activation. Lazily allocated for the same
    /// reason as <see cref="_pendingTx"/>.
    /// </summary>
    private HashSet<Guid>? _recentlyTerminal;

    /// <summary>
    /// Tracks per-saga which keys have already had the cross-migration
    /// LWW backstop applied. Keyed by transaction id; value is the set
    /// of keys whose backstop write has landed on this leaf.
    /// <para>
    /// Per-key (NOT per-saga) granularity is load-bearing for the
    /// shard-split + reshard chaos surface: two terminal deliveries to
    /// the same leaf can legitimately carry DIFFERENT
    /// <c>committedValues</c> subsets - e.g.
    /// </para>
    /// <list type="number">
    ///   <item><description>
    ///     <c>AtomicWriteGrain</c>'s direct fan-out to the destination
    ///     shard with the subset routed to that shard per the saga's
    ///     drift-corrected routing snapshot (typically the keys whose
    ///     slot has already migrated).
    ///   </description></item>
    ///   <item><description>
    ///     A source shard's <c>ForwardSplitTerminalAsync</c> mirror to
    ///     the same destination with a DIFFERENT subset - the keys whose
    ///     prepare landed on the source pre-split but whose slot has
    ///     since migrated to this destination.
    ///   </description></item>
    /// </list>
    /// <para>
    /// A per-saga dedup (the prior shape) would observe (1) first, mark
    /// the saga "backstopped", and short-circuit (2)'s missing keys -
    /// leaving them stuck at the drained pre-saga value. The chaos
    /// pattern <c>split (pre=5, post=11)</c> on the reshard fixture
    /// reproduces this exactly: 5 keys (one source shard's worth)
    /// orphaned because their backstop arrived after another shard's
    /// subset already poisoned the txid's dedup marker.
    /// </para>
    /// <para>
    /// Lazily allocated for the same reason as <see cref="_pendingTx"/>.
    /// The inner <c>HashSet&lt;string&gt;</c> uses <see cref="StringComparer.Ordinal"/>
    /// for consistency with <see cref="Dictionary{TKey,TValue}"/>
    /// instances elsewhere in this file.
    /// </para>
    /// </summary>
    private Dictionary<Guid, HashSet<string>>? _backstoppedTerminals;

    private ITxRegistryGrain? registry;

    /// <summary>
    /// Records a prepared-phase per-key mutation in the pending-tx map.
    /// The entry is invisible to readers until a matching terminal mark
    /// flips or drops it. Idempotent under LWW: a re-applied prepare
    /// for the same <c>(txid, key)</c> uses
    /// <see cref="LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/> so the
    /// strictly-greater HLC always wins.
    /// </summary>
    private void AddPreparedMutation(Guid transactionId, string key, in LwwValue<byte[]> incoming)
    {
        if (transactionId == Guid.Empty)
        {
            // A prepared mutation must carry a non-empty transaction id
            // so the matching terminal mark can find it; surface this
            // as a programmer error rather than silently leaking the
            // mutation into a never-flushed bucket.
            throw new InvalidOperationException(
                "A prepared mutation must carry a non-empty TransactionId. "
                + "The saga coordinator stamps the id via LatticeTransactionContext "
                + "before opening a LatticePreparedContext scope.");
        }

        var pending = _pendingTx ??= new Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>();
        if (!pending.TryGetValue(transactionId, out var bucket))
        {
            bucket = new Dictionary<string, LwwValue<byte[]>>();
            pending[transactionId] = bucket;
        }

        if (bucket.TryGetValue(key, out var existing))
        {
            bucket[key] = LwwValue<byte[]>.Merge(existing, incoming);
        }
        else
        {
            bucket[key] = incoming;
        }

        // Strict atomic-visibility: bump the same-silo revision cookie
        // so a co-located LeafCacheGrain notices the new pending key
        // and refreshes its pending-key set on the next read. Without
        // this the cache could continue serving the pre-saga value
        // from its in-memory cache for the prepared key.
        BumpLocalRevision();

        // Record the earliest WAL offset of any prepare under this
        // transaction id, but only when an apply scope is active -
        // foreground commits author the WAL and have no offset to
        // stamp, so they leave _pendingTxOffsets untouched and the
        // checkpoint clamp degrades to a no-op for foreground-only
        // leaves.
        var ambientOffset = LatticeApplyOffsetContext.Current;
        if (ambientOffset is long offset)
        {
            var offsets = _pendingTxOffsets ??= new Dictionary<Guid, long>();
            if (offsets.TryGetValue(transactionId, out var existingOffset))
            {
                if (offset < existingOffset)
                {
                    offsets[transactionId] = offset;
                }
            }
            else
            {
                offsets[transactionId] = offset;
            }
        }
    }

    /// <summary>
    /// Flips every pending-tx entry under <paramref name="transactionId"/>
    /// into the visible projection via
    /// <see cref="LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>. The
    /// linearization point for the saga on this leaf - every reader
    /// observes either zero of the saga's keys or every one of them
    /// after this call returns. Idempotent: repeated applies for the
    /// same transaction id are no-ops via
    /// <see cref="_recentlyTerminal"/>.
    /// <para>
    /// <b>Foreground single-cluster path (no <c>OriginClusterId</c>
    /// stamped).</b> Re-stamps every drained value's
    /// <see cref="LwwValue{T}.Timestamp"/> with the leaf's current
    /// <c>state.State.Clock</c>. The re-stamp is the cure for the
    /// stuck-key cache delta failure: the cache's per-entry HLC filter
    /// (<c>lww.Timestamp &gt; callerClock</c>) would otherwise exclude
    /// the drained value when intervening foreground writes have
    /// advanced <c>callerClock</c> past the prepared value's original
    /// prepare-time HLC. Re-stamping with <c>state.State.Clock</c>
    /// (which advances on every prepare via
    /// <see cref="AdvanceClockOrOverride"/>) guarantees the drained
    /// value's <see cref="LwwValue{T}.Timestamp"/> is strictly greater
    /// than every <c>callerClock</c> the cache could have observed
    /// during the saga, because the prepare path no longer ticks
    /// <c>state.State.Version[ReplicaId]</c> (only intervening
    /// non-saga writes do), so <c>callerClock</c> at terminal-time
    /// refresh trails <c>state.State.Clock</c> by at least one
    /// prepare-tick.
    /// </para>
    /// <para>
    /// <b>Cross-cluster atomic-apply path (per-entry
    /// <c>OriginClusterId</c> stamped).</b> Preserves every drained
    /// value's <see cref="LwwValue{T}.Timestamp"/> verbatim. The source
    /// cluster's per-entry HLC is the authoritative ordering token for
    /// receiver-side LWW resolution and MUST NOT be clobbered by the
    /// local clock. The cache-delta-filter constraint that motivates
    /// the foreground re-stamp is intrinsic to HLC-based filtering
    /// across clock-skewed clusters and is accepted here; the cache's
    /// revision-bump path delivers these values via full snapshot
    /// reload rather than per-entry delta.
    /// </para>
    /// <para>
    /// The branch decision uses <see cref="LwwValue{T}.OriginClusterId"/>
    /// - a deterministic, persisted signal stamped at prepare time
    /// from <see cref="LatticeOriginContext"/>. Because the flag is
    /// written into the WAL TxPrepare record's <see cref="LwwValue{T}"/>
    /// payload (see
    /// <see cref="BPlusLeafGrain.CommitSetAsync(string, byte[], long)"/>),
    /// foreground and replay observe the same value and therefore
    /// produce bit-identical projection states. Replay must NOT use
    /// <see cref="LatticeHlcOverrideContext"/> as the signal because
    /// that ambient is foreground-only.
    /// </para>
    /// <para>
    /// Replay determinism for the foreground branch: the replay
    /// coordinator drives <see cref="ILeafProjection.Apply"/> over the
    /// WAL in offset order, advancing <c>state.State.Clock</c> via
    /// <see cref="AdvanceProjectionClock"/> on every prior WAL entry.
    /// At terminal-replay time, <c>state.State.Clock</c> equals the
    /// max of all prior WAL <see cref="LatticeMutation.Timestamp"/>
    /// values, which matches what foreground saw when the terminal
    /// was originally appended - so foreground and replay produce
    /// bit-identical drained <see cref="LwwValue{T}.Timestamp"/>
    /// values. The WAL terminal entry itself stamps
    /// <see cref="HybridLogicalClock.Zero"/> by convention (saga-wide
    /// events have no per-key HLC), so we do not consult
    /// <see cref="LatticeMutation.Timestamp"/> for the re-stamp.
    /// </para>
    /// </summary>
    private void ApplyTxCommit(Guid transactionId)
    {
        if (transactionId == Guid.Empty)
            return;

        // Fast-path: leaf never saw a prepared mutation. Record the
        // terminal so a late-arriving prepared mutation under the same
        // id does not silently leak, then exit without touching
        // _pendingTx (which may still be null).
        if (_pendingTx is null || !_pendingTx.Remove(transactionId, out var bucket))
        {
            _pendingTxOffsets?.Remove(transactionId);
            (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
            return;
        }

        // Branch on the persisted OriginClusterId signal. See the
        // method's XML doc for the full rationale and the replay
        // determinism argument.
        var preserveTimestamps = false;
        foreach (var kvp in bucket)
        {
            if (!string.IsNullOrEmpty(kvp.Value.OriginClusterId))
            {
                preserveTimestamps = true;
                break;
            }
        }

        if (preserveTimestamps)
        {
            // Cross-cluster atomic apply: preserve per-entry source HLCs
            // verbatim. Advance state.State.Clock to the max of the
            // bucket's Timestamps so subsequent local reads observe a
            // monotonic clock.
            foreach (var kvp in bucket)
            {
                StoreEntry(kvp.Key, kvp.Value);
                AdvanceProjectionClock(kvp.Value.Timestamp);
            }
        }
        else
        {
            // Foreground single-cluster: re-stamp with terminal-time Clock
            // for cache-delta-filter correctness.
            var terminalStamp = state.State.Clock;
            foreach (var kvp in bucket)
            {
                var restamped = kvp.Value with { Timestamp = terminalStamp };
                StoreEntry(kvp.Key, restamped);
            }
            AdvanceProjectionClock(terminalStamp);
        }

        _pendingTxOffsets?.Remove(transactionId);
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);

        // Bump the same-silo revision cookie so a co-located
        // LeafCacheGrain notices both that the pending bucket has
        // drained AND that Entries now carries the post-saga values,
        // and refreshes its own state on the next read.
        BumpLocalRevision();
    }

    /// <summary>
    /// Drops every pending-tx entry under <paramref name="transactionId"/>
    /// without ever making it visible to readers - the saga's
    /// prepare-phase writes are undone in a single linearization step.
    /// Idempotent.
    /// </summary>
    private void ApplyTxAbort(Guid transactionId)
    {
        if (transactionId == Guid.Empty)
            return;

        var hadPending = _pendingTx is not null && _pendingTx.Remove(transactionId);
        _pendingTxOffsets?.Remove(transactionId);
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);

        // Bump the same-silo revision cookie so a co-located
        // LeafCacheGrain refreshes its pending-key set and stops
        // delegating reads for keys this aborted saga had prepared.
        if (hadPending)
            BumpLocalRevision();
    }

    /// <summary>
    /// Returns <c>true</c> if any pending-tx entry under any transaction
    /// id covers <paramref name="key"/>. Used by the read-path filter
    /// to hide saga prepare-phase writes from concurrent readers
    /// without a per-call RPC. O(pending-txs) - bounded by the small
    /// cardinality of in-flight sagas and the concurrent saga rate;
    /// returns immediately when the pending-tx map has never been
    /// allocated (the steady state for every leaf that has not
    /// participated in a saga since activation).
    /// <para>
    /// Strict atomic-visibility note: this is the cheap presence test;
    /// callers must NOT use it as the read-path verdict by itself.
    /// When it returns <c>true</c> the caller dials back through
    /// <see cref="ResolvePendingStatusAsync"/> (single-key paths) or
    /// <see cref="SnapshotPendingForReadAsync"/> (scan paths) to
    /// consult the per-tree <see cref="ITxRegistryGrain"/> for the
    /// recorded saga outcome. The registry's recorded decision is the
    /// single tree-wide linearization point - without it, a reader
    /// landing on this leaf during the post-commit-decision /
    /// pre-terminal-fan-out window would observe the saga's prepared
    /// keys as hidden while a sibling leaf had already flipped them
    /// visible (a split view).
    /// </para>
    /// </summary>
    private bool IsKeyPending(string key)
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
            return false;

        foreach (var bucket in _pendingTx.Values)
        {
            if (bucket.ContainsKey(key))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Synchronously locates the pending-tx entry for <paramref name="key"/>
    /// (if any) and outputs the owning transaction id and prepared
    /// value. Returns <c>false</c> on the steady-state path where the
    /// pending-tx map is empty or the key has no prepared mutation.
    /// When <c>true</c>, callers MUST consult
    /// <see cref="ResolvePendingStatusAsync"/> with the returned txid
    /// before serving the read - this method does not look at the
    /// per-tree TxRegistry.
    /// <para>
    /// O(pending-txs); bounded by in-flight saga cardinality. If two
    /// independent sagas have prepared the same key (the saga
    /// coordinator should reject this upstream) the first one
    /// encountered wins this lookup and the second one stays hidden
    /// until the first terminates.
    /// </para>
    /// </summary>
    private bool TryFindPendingForKey(string key, out Guid txid, out LwwValue<byte[]> pendingValue)
    {
        txid = Guid.Empty;
        pendingValue = default;
        if (_pendingTx is null || _pendingTx.Count == 0)
            return false;

        foreach (var (id, bucket) in _pendingTx)
        {
            if (bucket.TryGetValue(key, out var value))
            {
                txid = id;
                pendingValue = value;
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Asynchronously resolves the recorded outcome for
    /// <paramref name="txid"/> via the per-tree
    /// <see cref="ITxRegistryGrain"/>. This is the read-path dial-back
    /// that lets a leaf serving a key with a pending-tx entry decide
    /// whether to surface the prepared (post-saga) value, hide the
    /// key, or fall through to the pre-saga value in
    /// <c>state.State.Entries</c>.
    /// <para>
    /// Returns <see cref="TxStatus.InFlight"/> on degenerate inputs
    /// (empty txid or unknown tree id) - the strict-isolation default,
    /// which keeps the key hidden until the registry can be reached.
    /// </para>
    /// </summary>
    private async ValueTask<TxStatus> ResolvePendingStatusAsync(Guid txid)
    {
        if (txid == Guid.Empty) return TxStatus.InFlight;

        // Linearizable-scan fast path: when the lattice-level fan-out
        // has stamped a per-scan registry snapshot via
        // LatticeRegistrySnapshotContext, use the snapshot's recorded
        // status (or InFlight when absent) so this single-key dial-back
        // shares the same registry view as any sibling leaf scan in
        // the same fan-out.
        var ambient = LatticeRegistrySnapshotContext.Current;
        if (ambient is not null)
        {
            return ambient.TryGetValue(txid, out var ambientStatus) ? ambientStatus : TxStatus.InFlight;
        }

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return TxStatus.InFlight;
        registry ??= grainFactory.GetGrain<ITxRegistryGrain>(treeId);
        return await registry.GetStatusAsync(txid);
    }

    /// <summary>
    /// Captures a snapshot of the leaf's current pending-tx state for
    /// a scan-path read: the per-key pending entries plus a single
    /// batched call to the per-tree <see cref="ITxRegistryGrain"/>
    /// resolving every referenced txid's recorded outcome.
    /// <para>
    /// Returns empty maps in the steady-state path where the leaf has
    /// no pending-tx activity, so the scan loop's post-snapshot work
    /// degenerates to dictionary lookups against the empty
    /// <c>pendingKeys</c> map (cheap, no extra allocations beyond two
    /// empty Dictionary instances).
    /// </para>
    /// <para>
    /// On the saga-active path, makes exactly one RPC per scan
    /// regardless of how many keys the scan visits - the batched
    /// registry call collapses N per-key dial-backs into one round
    /// trip. Callers iterate <c>state.State.Entries</c> as usual and,
    /// for each key found in <paramref name="pendingKeys"/>, branch on
    /// the resolved outcome: <see cref="TxStatus.Committed"/> surfaces
    /// the prepared value, <see cref="TxStatus.InFlight"/> hides the
    /// key, and <see cref="TxStatus.Aborted"/> falls through to the
    /// pre-saga <c>Entries</c> value.
    /// </para>
    /// </summary>
    private async ValueTask<(
        Dictionary<Guid, TxStatus> outcomes,
        Dictionary<string, (Guid txid, LwwValue<byte[]> value)> pendingKeys)>
        SnapshotPendingForReadAsync()
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
        {
            return (EmptyOutcomes, EmptyPendingKeys);
        }

        var txids = new List<Guid>(_pendingTx.Count);
        var pendingKeys = new Dictionary<string, (Guid, LwwValue<byte[]>)>();
        foreach (var (txid, bucket) in _pendingTx)
        {
            txids.Add(txid);
            foreach (var (key, value) in bucket)
            {
                pendingKeys.TryAdd(key, (txid, value));
            }
        }

        // Linearizable-scan fast path: when the lattice-level fan-out
        // has stamped a per-scan registry snapshot via
        // LatticeRegistrySnapshotContext, every leaf in the scan must
        // share that exact view of registry decisions - otherwise the
        // registry's InFlight→Committed transition can fall mid-fan-out
        // and produce a split observation across leaves. Use the
        // ambient and skip the per-leaf registry RPC entirely.
        // Decisions not in the ambient default to InFlight (consistent
        // with "decision not yet recorded as of this snapshot's
        // wall-clock moment").	
        var ambient = LatticeRegistrySnapshotContext.Current;
        if (ambient is not null)
        {
            var filtered = new Dictionary<Guid, TxStatus>(txids.Count);
            foreach (var t in txids)
            {
                filtered[t] = ambient.TryGetValue(t, out var s) ? s : TxStatus.InFlight;
            }
            return (filtered, pendingKeys);
        }

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
        {
            // Defensive: no tree id means we cannot consult the
            // registry. Treat every pending entry as InFlight - the
            // strict-isolation default keeps the prepared keys hidden
            // until activation completes its tree-id stamp.
            var hidden = new Dictionary<Guid, TxStatus>(txids.Count);
            foreach (var t in txids) hidden[t] = TxStatus.InFlight;
            return (hidden, pendingKeys);
        }

        registry ??= grainFactory.GetGrain<ITxRegistryGrain>(treeId);
        var outcomes = await registry.GetStatusManyAsync(txids);
        return (outcomes, pendingKeys);
    }

    /// <summary>
    /// Pending-transaction count snapshot for tests. Not on any
    /// public surface.
    /// </summary>
    internal int PendingTransactionCount => _pendingTx?.Count ?? 0;

    /// <summary>
    /// Recently-terminal count snapshot for tests. Not on any
    /// public surface.
    /// </summary>
    internal int RecentlyTerminalCount => _recentlyTerminal?.Count ?? 0;

    /// <summary>
    /// Returns the minimum WAL offset across every unresolved
    /// pending-tx prepare on this leaf, or <c>null</c> when no
    /// prepare-with-offset is currently buffered. Used by
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> to clamp
    /// the persisted checkpoint to <c>min(requested, value - 1)</c>
    /// so crash recovery does not advance past an unresolved prepare.
    /// O(pending-txs) - bounded by the small cardinality of in-flight
    /// sagas; returns immediately when the offset map has never been
    /// allocated (the steady state for foreground-driven leaves).
    /// </summary>
    internal long? MinUnresolvedPrepareOffset
    {
        get
        {
            if (_pendingTxOffsets is null || _pendingTxOffsets.Count == 0)
                return null;

            long min = long.MaxValue;
            foreach (var offset in _pendingTxOffsets.Values)
            {
                if (offset < min)
                    min = offset;
            }
            return min;
        }
    }

    /// <inheritdoc />
    public Task<List<string>> GetPendingKeysAsync()
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
            return Task.FromResult(new List<string>());

        // De-duplicate keys across pending tx buckets - two independent
        // sagas could (rarely) prepare the same key. Set is then
        // materialised into a List for the wire shape.
        var unique = new HashSet<string>(StringComparer.Ordinal);
        foreach (var bucket in _pendingTx.Values)
        {
            foreach (var key in bucket.Keys)
                unique.Add(key);
        }
        return Task.FromResult(new List<string>(unique));
    }

    /// <inheritdoc />
    public async Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues = null)
    {
        if (transactionId == Guid.Empty)
            return;

        // Capture the bucket reference up-front. ApplyTxCommit/ApplyTxAbort
        // remove the bucket from _pendingTx, so we need the snapshot here to
        // compute the per-key backstop set below before the flip path mutates
        // _pendingTx. The reference into the bucket dictionary remains valid
        // after Remove (we only need to read its keys).
        Dictionary<string, LwwValue<byte[]>>? bucket = null;
        if (_pendingTx is not null && _pendingTx.TryGetValue(transactionId, out var existingBucket))
            bucket = existingBucket;
        var hadPending = bucket is not null;

        var alreadyFlipped = _recentlyTerminal is not null && _recentlyTerminal.Contains(transactionId);

        // Per-key backstop set: every key in committedValues that is
        // (a) NOT already covered by this leaf's pending bucket (the
        // pending-flip path will surface those values), AND
        // (b) NOT already backstopped under this transaction id by a
        // prior terminal delivery (per-key dedup, not per-saga).
        //
        // Per-key dedup is load-bearing: two terminal deliveries to the
        // same leaf can legitimately carry DIFFERENT committedValues
        // subsets - the AtomicWriteGrain direct fan-out routes by
        // current-routing per shard, while a source shard's
        // ForwardSplitTerminalAsync mirror routes by MovedAwaySlots's
        // earlier migration record. A per-saga dedup observes one
        // subset first, marks the saga backstopped, and short-circuits
        // the OTHER subset's missing keys - leaving them stuck at the
        // drained pre-saga value. The chaos pattern
        // `split (pre=5, post=11)` on the reshard fixture reproduces
        // this exactly: 5 keys (one source shard's worth) orphaned
        // because their backstop arrived after another shard's subset
        // already poisoned the txid's dedup marker.
        List<KeyValuePair<string, byte[]>>? missingKeys = null;
        var hasBackstopPayload = committed && committedValues is { Count: > 0 };
        HashSet<string>? alreadyBackstoppedKeys = null;
        if (hasBackstopPayload)
        {
            if (_backstoppedTerminals is not null)
                _backstoppedTerminals.TryGetValue(transactionId, out alreadyBackstoppedKeys);

            foreach (var kvp in committedValues!)
            {
                if (bucket is not null && bucket.ContainsKey(kvp.Key))
                    continue;
                if (alreadyBackstoppedKeys is not null && alreadyBackstoppedKeys.Contains(kvp.Key))
                    continue;
                (missingKeys ??= []).Add(kvp);
            }
        }

        // Hot-path short-circuit: a duplicate terminal delivery with
        // nothing new to do. The flip side already ran (alreadyFlipped),
        // and either there is no backstop payload, or every payload key
        // is already covered (in the bucket - which is null on the
        // alreadyFlipped path - or in the per-key backstopped set).
        if (alreadyFlipped && missingKeys is null && !hadPending)
            return;

        // Pending-flip path: drain the bucket into Entries (commit) or
        // drop it without surfacing (abort). Zero leaf I/O - the WAL is
        // the recovery source for the flipped entries. Gated on
        // !alreadyFlipped so a duplicate delivery (e.g. arriving via
        // both the direct fan-out and a split-shadow forward) does not
        // attempt to re-flip a bucket that the first delivery already
        // consumed.
        if (hadPending && !alreadyFlipped)
        {
            if (committed)
            {
                // Tick state.State.Version[ReplicaId] at the saga's
                // terminal-foreground entry point so the cache's next
                // delta(callerClock) call no longer hits the
                // sinceVersion.DominatesOrEquals(state.State.Version)
                // short-circuit, AND so callerClock at the next refresh
                // (which equals the cache's new saved Version[ReplicaId])
                // is strictly less than the drained values' re-stamped
                // Timestamps (= state.State.Clock at terminal-time, which
                // already trails Version by zero-or-positive ticks because
                // the prepare path no longer ticks Version). Foreground-
                // only - the replay path inherits the
                // ILeafProjection.Apply convention of not advancing
                // Version, so terminal replay rebuilds Entries
                // deterministically without contributing to a
                // foreground-cache version-vector view that does not
                // exist during replay.
                state.State.Version.Tick(ReplicaId);
                ApplyTxCommit(transactionId);
            }
            else
            {
                ApplyTxAbort(transactionId);
            }
        }

        // Per-key cross-migration LWW backstop. Fires on the commit
        // path for every committedValues key that the bucket did not
        // cover and the per-key dedup set did not already cover.
        // Stamp every backstop entry with the SAME Tick(state.State.Clock)
        // value: HLC.Tick guarantees strict-greater ordering against any
        // pre-saga drained value already in Entries, so LWW.Merge
        // resolves in favour of the backstop. We do NOT tick Version on
        // the pure backstop path (hadPending=false) - the cache is not
        // tracking this leaf as a pending source for this saga, and
        // ticking would race with concurrent reads. When the
        // pending-flip path above already ticked Version
        // (hadPending=true), the backstop piggybacks on that single tick.
        //
        // Each missing-key write is durably committed by appending a
        // LatticeMutation { Kind = Set, IsBackstop = true, ... } to the
        // per-shard WAL via ICommitLogWriter - the same primitive every
        // other foreground commit on this leaf uses under the
        // WAL-as-sole-commit-point invariant. The WAL append is the
        // durability point; the in-memory projection update
        // (StoreEntry) happens immediately after under the same shared
        // HLC tick so a co-located reader sees the value before the
        // next dequeue. Crash recovery rebuilds Entries from the WAL
        // via the per-shard activation-time replay path. The legacy
        // standalone state-row persist that used to follow this loop
        // is gone - every leaf foreground commit now obeys the
        // WAL-as-sole-commit-point invariant.
        if (missingKeys is { Count: > 0 })
        {
            var stamp = Primitives.HybridLogicalClock.Tick(state.State.Clock);
            var origin = LatticeOriginContext.Current;
            var vc = LatticeVectorClockContext.Current;
            var writer = ResolveCommitLogWriter();
            var treeId = state.State.TreeId ?? string.Empty;
            var shardIndex = state.State.ShardIndex ?? 0;
            var maintenance = LatticeMaintenanceContext.Current;

            foreach (var kvp in missingKeys)
            {
                if (writer is not null)
                {
                    var mutation = new LatticeMutation
                    {
                        TreeId = treeId,
                        Kind = MutationKind.Set,
                        Key = kvp.Key,
                        Value = kvp.Value,
                        Timestamp = stamp,
                        IsTombstone = false,
                        ExpiresAtTicks = 0,
                        OriginClusterId = origin,
                        VectorClock = vc,
                        TransactionId = transactionId,
                        Category = maintenance,
                        IsPrepared = false,
                        IsBackstop = true,
                        ShardIndex = shardIndex,
                    };

                    // Emit the WAL append on the LeafWriteDuration
                    // histogram tagged `kind=backstop` so operators can
                    // size cross-migration backstop traffic against
                    // ordinary writes on the same instrument. The tag
                    // dimension is additive - emissions on this
                    // histogram from the projection-checkpoint flush
                    // path carry no `kind` tag and remain
                    // distinguishable as the steady-state state-row
                    // path (now scoped to projection-checkpoint flushes
                    // only).
                    var walStartTicks = Stopwatch.GetTimestamp();
                    try
                    {
                        await writer.AppendAsync(mutation);
                    }
                    finally
                    {
                        var elapsedMs = (Stopwatch.GetTimestamp() - walStartTicks) * 1000.0 / Stopwatch.Frequency;
                        LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "backstop"));
                    }
                }

                var value = new Primitives.LwwValue<byte[]>
                {
                    Value = kvp.Value,
                    Timestamp = stamp,
                    OriginClusterId = origin,
                    VectorClock = vc,
                };
                StoreEntry(kvp.Key, value);
            }

            AdvanceProjectionClock(stamp);
            BumpLocalRevision();

            // Record the keys we just backstopped so a SUBSEQUENT
            // delivery (carrying possibly a different subset) skips
            // these via the alreadyBackstoppedKeys check above without
            // re-stamping Entries. Per-key dedup is the load-bearing
            // invariant - a per-txid marker would short-circuit a
            // legitimate sibling subset arriving later.
            _backstoppedTerminals ??= new Dictionary<Guid, HashSet<string>>();
            if (!_backstoppedTerminals.TryGetValue(transactionId, out var perTxBackstopped))
            {
                perTxBackstopped = new HashSet<string>(StringComparer.Ordinal);
                _backstoppedTerminals[transactionId] = perTxBackstopped;
            }
            foreach (var kvp in missingKeys)
                perTxBackstopped.Add(kvp.Key);
        }

        // Mark the saga's pending-flip dedup. _backstoppedTerminals is
        // populated above only when a backstop write actually landed,
        // keyed per-key so future deliveries with different subsets
        // continue to do real work for keys they haven't covered yet.
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
    }
}
