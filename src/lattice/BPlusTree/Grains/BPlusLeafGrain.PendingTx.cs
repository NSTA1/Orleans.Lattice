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
    ///     A source shard's transitive split-forward fan-out (via the
    ///     saga's <c>TerminalFanOutResolver.ResolveTransitiveAsync</c>
    ///     expansion of <c>TouchedShards</c>) reaching the same
    ///     destination with a DIFFERENT subset - the keys whose
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

#if LATTICE_DIAG
        // DIAG: prepare landed on this leaf.
        DiagSink.Write($"[DIAG prepare] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={key} " +
            $"valRound={DiagDecodeRound(incoming.Value)} " +
            $"hlc={incoming.Timestamp} origin={incoming.OriginClusterId ?? "(local)"} " +
            $"clock={state.State.Clock}");
#endif

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
#if LATTICE_DIAG
            // DIAG: commit arrived on leaf with no bucket (fast-path).
            DiagSink.Write($"[DIAG commit-empty] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} clock={state.State.Clock}");
#endif
            return;
        }

#if LATTICE_DIAG
        // DIAG: commit will drain this bucket.
        {
            var keys = string.Join(",", bucket.Keys);
            DiagSink.Write($"[DIAG commit] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} bucket=[{keys}] clock={state.State.Clock}");
            foreach (var kvp in bucket)
            {
                var hasExisting = state.State.Entries.TryGetValue(kvp.Key, out var existing);
                DiagSink.Write($"[DIAG commit-key] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={kvp.Key} " +
                    $"prepared.Hlc={kvp.Value.Timestamp} " +
                    $"existing={(hasExisting ? $"hlc={existing.Timestamp},isMig={existing.IsMigrated}" : "(none)")}");
            }
        }
#endif

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
            // monotonic clock. The bucket value carries IsMigrated=false
            // (prepared mutations are never migration imports), so the
            // merge in StoreEntry clears any stale migration marker
            // when this value wins.
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
            //
            // Cross-shard-migration LWW dominance (Fix M). Under an online
            // reshard, the destination leaf is freshly created and its
            // state.State.Clock starts near Zero, while the SOURCE leaf's
            // Entries[K] for a saga-touched key carries the HLC stamped at
            // a PRIOR saga's terminal-flip time on the source - a high
            // HLC reflecting the source leaf's cumulative tick history.
            // TreeShardSplitGrain.ForwardMovedSlotEntriesAsync ships those
            // entries verbatim via target.MergeManyAsync, so the
            // destination's Entries[K] inherits the source's high HLC
            // BEFORE the current saga's terminal drains the destination's
            // pending bucket. If we re-stamp with state.State.Clock
            // verbatim (the destination's low clock) and let StoreEntry
            // LWW-merge against the migrated value, the migrated value
            // WINS because its HLC dominates ours - silently overwriting
            // the saga's drained value with the pre-saga value the
            // migration carried. The chaos-suite "other=1" stuck-key
            // failure shape on Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard
            // reproduces this exactly: one key per reshard window stays
            // at an OLDER round's value across multiple subsequent
            // sagas because every drain on the destination loses LWW to
            // the migrated entry until the destination's clock organically
            // catches up.
            //
            // Fix: pre-scan the bucket for any existing Entries[K] whose
            // HLC dominates state.State.Clock, then Tick once past the
            // observed max. The single Tick is sufficient because the
            // migrated HLC is observed atomically here and the resulting
            // terminalStamp strictly dominates it via HLC.Tick's
            // strict-greater semantic.
            var baseTerminalStamp = state.State.Clock;
            foreach (var kvp in bucket)
            {
                if (state.State.Entries.TryGetValue(kvp.Key, out var preExisting))
                {
                    // Mirror the orphan-drain skip condition below: a key
                    // whose existing HLC dominates the prepared HLC will
                    // NOT be written, so its existing.Timestamp must not
                    // pull terminalStamp past where we need it for the
                    // keys we WILL write.
                    //
                    // Migration-provenance carve-out: a dominating
                    // preExisting whose value carries IsMigrated=true
                    // (stamped at MergeIntoState / MergeEntriesAsync
                    // import time) IS going to be written below, so
                    // its HLC MUST contribute to baseTerminalStamp -
                    // otherwise the drained stamp would lose LWW to
                    // the migrated entry's high HLC.
                    if (preExisting.Timestamp.CompareTo(kvp.Value.Timestamp) > 0
                        && !preExisting.IsMigrated)
                    {
#if LATTICE_DIAG
                        // DIAG: pre-scan skip - capture stuck-key signature.
                        DiagSink.Write($"[DIAG pre-scan-skip] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={kvp.Key} " +
                            $"existing.Hlc={preExisting.Timestamp} existing.IsMigrated={preExisting.IsMigrated} " +
                            $"existing.Origin={preExisting.OriginClusterId ?? "(local)"} " +
                            $"prepared.Hlc={kvp.Value.Timestamp} prepared.Origin={kvp.Value.OriginClusterId ?? "(local)"} " +
                            $"clock={state.State.Clock}");
#endif
                        continue;
                    }
                    if (preExisting.Timestamp.CompareTo(baseTerminalStamp) > 0)
                        baseTerminalStamp = preExisting.Timestamp;
                }
            }
            // Counter-only bump past the higher of state.State.Clock and
            // baseTerminalStamp. The bump is load-bearing for cache-delta
            // visibility: ApplyTxTerminalAsync publishes the pre-bump
            // state.State.Clock as the new Version[ReplicaId] (see the
            // comment block around the call site), and the cache's
            // GetDeltaSinceAsync filter excludes any entry whose Timestamp
            // is <= the caller's last-observed Version[ReplicaId]. A
            // strictly-greater terminalStamp is therefore required for the
            // drained entries to be delivered on the next refresh.
            //
            // Replay determinism: HybridLogicalClock.Tick is non-deterministic
            // (it reads DateTimeOffset.UtcNow.Ticks, so foreground and
            // terminal-replay produce different WallClockTicks values). A
            // counter-only bump - construct a new HLC with the same
            // WallClockTicks and Counter+1 - is bit-identical across
            // foreground and replay because the WAL's AdvanceProjectionClock
            // calls have already deterministically reconstructed
            // state.State.Clock on the replay path. Every drained entry on
            // both paths thus carries the same Timestamp, which is the
            // invariant the cross-saga LWW dominance checks rely on.
            var maxBase = baseTerminalStamp.CompareTo(state.State.Clock) > 0
                ? baseTerminalStamp
                : state.State.Clock;
            var terminalStamp = new Primitives.HybridLogicalClock
            {
                WallClockTicks = maxBase.WallClockTicks,
                Counter = maxBase.Counter + 1,
            };
            foreach (var kvp in bucket)
            {
                // Orphan-drain guard. Under an online reshard, a saga's
                // shadow-forwarded prepare can land on a destination
                // leaf AFTER the saga's terminal broadcast already
                // reached the same leaf via the cross-migration LWW
                // backstop path (which writes Entries directly with no
                // bucket to flip). A second terminal for the same saga
                // - typically a duplicate via the late-refetch loop in
                // AtomicWriteGrain.BroadcastTerminalsAsync - observes
                // the orphan bucket with alreadyFlipped=false and
                // would drain it here. Re-stamping the drained value
                // with the current state.State.Clock unconditionally
                // dominates ANY prior Entries timestamp via LWW.Merge,
                // so a strictly-later saga that has ALREADY drained
                // the same key (Entries[K] = V_{newer}) gets silently
                // overwritten by this saga's (now-stale) V_{older}.
                // The orphan's prepared HLC (kvp.Value.Timestamp) is
                // the saga's source-time stamp captured at PREPARE
                // time on the source shard - strictly less than the
                // destination's terminal-time stamp for a strictly-
                // later saga that touched the same key. So if Entries
                // already holds a timestamp dominating the prepared
                // HLC, this drain is logically obsolete and must be
                // skipped to preserve the cross-saga LWW ordering.
                // Replay determinism is preserved: the same HLC
                // comparison runs against the same Entries snapshot
                // during WAL replay, producing the same skip decision.
                // This is the write-side complement to the read-side
                // orphan-pending guard in GetWithPendingAsync; both
                // are needed because the orphan can manifest either
                // as a surviving pending bucket (read-side path) or
                // as an already-drained-but-stale Entries write
                // (write-side path).
                //
                // Migration-provenance carve-out: the inverse race
                // also exists. Under a cross-shard reshard, a saga's
                // shadow-forwarded prepare can land on a freshly-
                // created destination leaf BEFORE
                // TreeShardSplitGrain.ForwardMovedSlotEntriesAsync
                // ships the source's high-HLC entries to the
                // destination via target.MergeManyAsync. The pending
                // bucket on the destination then carries a LOW
                // prepared HLC (the destination's low clock at
                // prepare-arrival time), and migration subsequently
                // imports the source's HIGH migrated HLC into
                // Entries. When the saga's terminal arrives, this
                // guard would see `existing.Timestamp > prepared.Timestamp`
                // and skip the drain - silently discarding the
                // current saga's authoritative value in favour of
                // the pre-saga migrated value. The IsMigrated flag
                // on the existing value distinguishes the two shapes:
                // when the dominating existing entry came from a
                // migration, the drain proceeds; when it came from a
                // strictly-later sibling-saga drain (IsMigrated=false),
                // the drain is correctly skipped. See LwwValue.IsMigrated
                // for the discriminator's full semantics.
                if (state.State.Entries.TryGetValue(kvp.Key, out var existing)
                    && existing.Timestamp.CompareTo(kvp.Value.Timestamp) > 0
                    && !existing.IsMigrated)
                {
#if LATTICE_DIAG
                    // DIAG: drain-loop skip - capture stuck-key signature.
                    DiagSink.Write($"[DIAG drain-skip] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} key={kvp.Key} " +
                        $"existing.Hlc={existing.Timestamp} existing.IsMigrated={existing.IsMigrated} " +
                        $"existing.Origin={existing.OriginClusterId ?? "(local)"} " +
                        $"prepared.Hlc={kvp.Value.Timestamp} prepared.Origin={kvp.Value.OriginClusterId ?? "(local)"} " +
                        $"clock={state.State.Clock} terminalStamp={terminalStamp}");
#endif
                    continue;
                }
                // The prepared value carries IsMigrated=false (default
                // - prepared mutations are never migration imports);
                // the re-stamp preserves that, so StoreEntry's merge
                // naturally clears any stale migration marker.
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

#if LATTICE_DIAG
        // DIAG: abort entry.
        DiagSink.Write($"[DIAG abort] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} hadPending={hadPending} clock={state.State.Clock}");
#endif

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
    /// O(pending-txs); bounded by in-flight saga cardinality. When two
    /// independent sagas have prepared the same key (which can happen
    /// after a shard split's retroactive sweep installs a prepare for
    /// a saga whose terminal then arrives only at the source shard,
    /// leaving an orphan on the destination, while a later saga
    /// prepares the same key against the destination), the bucket
    /// with the strictly-greater <see cref="HybridLogicalClock"/>
    /// timestamp wins this lookup. The newest prepare always
    /// represents the saga whose terminal is most likely to be
    /// pending or recently delivered, so preferring it minimises
    /// stale-read exposure when an orphaned older prepare lingers in
    /// the pending map. Idempotent re-replays of the same
    /// <c>(txid, key)</c> use the same timestamp and produce a fixed
    /// point under this tie-break.
    /// </para>
    /// </summary>
    private bool TryFindPendingForKey(string key, out Guid txid, out LwwValue<byte[]> pendingValue)
    {
        txid = Guid.Empty;
        pendingValue = default;
        if (_pendingTx is null || _pendingTx.Count == 0)
            return false;

        var found = false;
        foreach (var (id, bucket) in _pendingTx)
        {
            if (!bucket.TryGetValue(key, out var value))
                continue;

            if (!found || value.Timestamp.CompareTo(pendingValue.Timestamp) > 0)
            {
                txid = id;
                pendingValue = value;
                found = true;
            }
        }
        return found;
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
    public Task<List<PendingMutationSnapshot>> GetPendingMutationsForSlotsAsync(int[] sortedMovedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        // Steady-state fast path: no pending bucket (the vast majority
        // of leaves never participate in a saga) or an empty
        // moved-slots array means no work to do. Return an empty list
        // without allocating any further state.
        if (_pendingTx is null || _pendingTx.Count == 0 || sortedMovedSlots.Length == 0)
            return Task.FromResult(new List<PendingMutationSnapshot>());

        var result = new List<PendingMutationSnapshot>();
        foreach (var (txid, bucket) in _pendingTx)
        {
            // Per-saga WAL offset (if any) for the snapshot's
            // WalOffset field. Foreground commits leave this map
            // untouched; the value is surfaced for diagnostics only
            // and is 0 when unstamped.
            long walOffset = 0;
            if (_pendingTxOffsets is not null
                && _pendingTxOffsets.TryGetValue(txid, out var offset))
            {
                walOffset = offset;
            }

            foreach (var (key, value) in bucket)
            {
                var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
                if (Array.BinarySearch(sortedMovedSlots, slot) < 0)
                    continue;

                result.Add(new PendingMutationSnapshot
                {
                    TransactionId = txid,
                    Key = key,
                    Value = value.IsTombstone ? null : value.Value,
                    Timestamp = value.Timestamp,
                    IsTombstone = value.IsTombstone,
                    ExpiresAtTicks = value.ExpiresAtTicks,
                    OriginClusterId = value.OriginClusterId,
                    VectorClock = value.VectorClock,
                    WalOffset = walOffset,
                });
            }
        }

        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public async Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues = null)
    {
        if (transactionId == Guid.Empty)
            return;

#if LATTICE_DIAG
        // DIAG terminal-leaf-apply: fires at the very entry of the
        // per-leaf terminal handler, BEFORE the _recentlyTerminal dedup
        // and the pending/backstop path selection. Pairs with the
        // shard-side terminal-recv emission so the saga's full fan-out
        // ordering (per-leaf wall-clock timing, dedup state, backstop
        // payload presence) can be reconstructed from the trace.
        var diagHadPending = _pendingTx is not null && _pendingTx.ContainsKey(transactionId);
        var diagAlreadyFlipped = _recentlyTerminal is not null && _recentlyTerminal.Contains(transactionId);
        DiagSink.Write($"[DIAG terminal-leaf-apply] silo={DiagSiloTag} gid={context.GrainId} tx={transactionId} committed={committed} hadPending={diagHadPending} alreadyFlipped={diagAlreadyFlipped} committedValuesCount={committedValues?.Count ?? 0} committedKeys=[{(committedValues is null ? "<null>" : string.Join(",", committedValues.Keys))}]");
#endif

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
        // current-routing per shard, while the saga's transitive
        // split-forward fan-out (TerminalFanOutResolver) reaches the
        // same destination via the source shard's earlier
        // MovedAwaySlots migration record. A per-saga dedup observes
        // one subset first, marks the saga backstopped, and short-
        // circuits the OTHER subset's missing keys - leaving them
        // stuck at the drained pre-saga value. The chaos pattern
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
        // the recovery source for the flipped entries.
        //
        // Three sub-paths based on `alreadyFlipped`:
        //
        // (1) `!alreadyFlipped, committed`: normal commit. Drain the
        //     bucket into Entries via ApplyTxCommit. Tick Version so
        //     a co-located LeafCacheGrain notices the new saga state.
        //
        // (2) `!alreadyFlipped, !committed`: normal abort. Discard the
        //     bucket via ApplyTxAbort without surfacing prepared values.
        //
        // (3) `alreadyFlipped` (either commit or abort): the saga's
        //     terminal has ALREADY landed on this leaf, having written
        //     the correct values via flip-drain or per-key backstop.
        //     A bucket present now means a TreeShardSplitGrain
        //     retroactive sweep replayed a source-leaf prepare snapshot
        //     to this destination AFTER the saga's commit broadcast had
        //     already reached it (via BFS-with-fullBackstop through the
        //     source's MovedAwaySlots). The orphan bucket carries the
        //     PREPARE-TIME value, which can be many saga rounds older
        //     than the current Entries[K] state. Draining it would
        //     stamp a stale value with a fresh HLC tick, causing
        //     readers to observe an old saga's value in place of the
        //     current one (the chaos signature `unknown-round
        //     (other=N)` reproduces this exactly). The correct action
        //     is to DISCARD the orphan bucket without surfacing - the
        //     original terminal's backstop already wrote the correct
        //     value, so the bucket is pure dead weight that would
        //     otherwise pin the pending-key read-path until the txid
        //     was evicted from the registry retention window.
        if (hadPending)
        {
            if (alreadyFlipped)
            {
                ApplyTxAbort(transactionId);
            }
            else if (committed)
            {
                // Publish Version[ReplicaId] as the *pre-drain* Clock
                // value, then let ApplyTxCommit's counter-only bump push
                // Clock (and every drained Entries[K].Timestamp) one
                // counter unit ahead. The LeafCacheGrain stores
                // Version[ReplicaId] as its saved callerClock on each
                // refresh and excludes entries whose Timestamp is <=
                // callerClock from the next delta - so Version[ReplicaId]
                // must be strictly less than every drained Timestamp for
                // the just-flipped saga's values to be delivered.
                //
                // The previous shape - state.State.Version.Tick(ReplicaId)
                // BEFORE ApplyTxCommit - read DateTimeOffset.UtcNow.Ticks
                // and pumped Version[ReplicaId] forward to wall-clock-now,
                // while state.State.Clock only advanced via
                // AdvanceProjectionClock at prepare time. After enough
                // saga commits the two clocks drifted by tens of
                // milliseconds (Version ahead of Clock), and the cache's
                // `lww.Timestamp > callerClock` filter silently dropped
                // the drained values on every refresh - manifesting as
                // the chaos-test "unknown-round (other=N)" stale-cache
                // signature on Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard.
                //
                // Foreground-only: the replay path inherits the
                // ILeafProjection.Apply convention of not advancing
                // Version, so this branch is skipped on replay and the
                // foreground/replay symmetry holds because every drained
                // Entries[K].Timestamp is reconstructed bit-identically
                // from the same counter-only bump (see the matching
                // comment block in ApplyTxCommit).
                ApplyTxCommit(transactionId);
                // Publish state.State.Clock AFTER the commit: ApplyTxCommit
                // does the counter-only bump that lifts state.State.Clock
                // (and every drained Entries[K].Timestamp) one counter unit
                // ahead of the pre-publish snapshot. Publishing the
                // post-commit Clock keeps Version[ReplicaId] equal to the
                // highest stamp any drained entry actually carries - which
                // is exactly the cache filter's reference value.
                var postCommitVersion = state.State.Clock;
                if (postCommitVersion.CompareTo(state.State.Version.GetClock(ReplicaId)) > 0)
                {
                    state.State.Version.Entries[ReplicaId] = postCommitVersion;
                }
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
        // resolves in favour of the backstop.
        //
        // After the loop we MUST publish the backstop stamp into
        // Version[ReplicaId] via PublishVersionAdvance(stamp) (see
        // below). An earlier shape skipped the publication on the
        // hadPending=false branch on the theory that "the cache is not
        // tracking this leaf as a pending source for this saga." That
        // reasoning was incorrect: the same-tree LeafCacheGrain is the
        // primary read path for every key in state.State.Entries, and
        // its RefreshAsync fast path (GetDeltaSinceAsync ->
        // DominatesOrEquals) short-circuits when the cache's saved
        // _version equals Version[ReplicaId]. If the backstop write
        // does not lift Version[ReplicaId], the cache continues
        // serving the previous value (commonly a freshly-imported
        // IsMigrated=true pre-saga snapshot) indefinitely. A backstop terminal landing on a destination
        // leaf whose only prior write was a cross-leaf migration
        // import never advanced Version, so the cache pinned the
        // migrated pre-saga value through every subsequent saga
        // round.
        //
        // Publication is safe under concurrent reads because
        // PublishVersionAdvance is a strict-greater-only conditional
        // assignment of a single dictionary entry (no allocation, no
        // structural mutation of the dictionary's shape); concurrent
        // dictionary reads on the keyed slot see either the old or
        // the new value, both of which are correctness-preserving
        // (the cache's filter is monotone in either direction).
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
            // Cross-shard-migration LWW dominance (Fix M, backstop variant).
            // See the foreground-drain branch above for the full rationale.
            // The same race that affects the pending-flip restamp affects
            // the pure-backstop path: a destination leaf whose freshly-
            // minted state.State.Clock is below the migrated value's HLC
            // would stamp the backstop write with Tick(state.State.Clock),
            // which the LWW.Merge inside StoreEntry resolves AGAINST when
            // the existing Entries[K] carries a higher HLC from migration.
            // Pre-advance baseClock past any existing entry for the
            // missing keys before Ticking so the backstop strictly
            // dominates the migrated pre-saga value.
            var baseClock = state.State.Clock;
            foreach (var kvp in missingKeys)
            {
                if (state.State.Entries.TryGetValue(kvp.Key, out var preExisting)
                    && preExisting.Timestamp.CompareTo(baseClock) > 0)
                {
                    baseClock = preExisting.Timestamp;
                }
            }
            var stamp = Primitives.HybridLogicalClock.Tick(baseClock);
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
                    var entry = new WalRecord
                    {
                        TreeId = treeId,
                        Op = MutationKind.Set,
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
                        await writer.AppendAsync(entry);
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
                // Backstop is a non-migration write: any prior
                // migration-provenance marker for this key is now
                // stale and must be cleared so a subsequent saga's
                // orphan-drain guard does not mistake the backstop
                // write for a migration import.
            }

            AdvanceProjectionClock(stamp);
            // Lift Version[ReplicaId] to the backstop stamp so the
            // co-located LeafCacheGrain's next RefreshAsync observes
            // a non-empty delta containing the just-stamped backstop
            // entries. Without this the cache's DominatesOrEquals
            // fast path returns the empty singleton and the cache
            // serves whatever value was in _cache before the backstop
            // (commonly an IsMigrated=true pre-saga snapshot from an
            // earlier cross-leaf migration import) indefinitely. See
            // the multi-paragraph rationale block at the head of the
            // backstop branch above.
            //
            // The hadPending=true commit branch already published
            // state.State.Clock post-commit; re-publishing here is
            // additive (the guard is strict-greater) and load-bearing
            // when the same terminal carries BOTH a flippable bucket
            // AND missing-key backstops (the bucket flip publishes
            // the post-flip Clock, but the backstop is stamped AFTER
            // and produces a strictly-greater stamp).
            if (stamp.CompareTo(state.State.Version.GetClock(ReplicaId)) > 0)
            {
                state.State.Version.Entries[ReplicaId] = stamp;
            }
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

        // Clear any destination-side shadow marker installed by the
        // split coordinator for this saga. Once the terminal has been
        // applied here, Entries[K] reflects the authoritative
        // post-saga state (drained pending, backstopped commit, or
        // unchanged on abort), so the migrated-entry guard in the
        // read path has nothing left to gate. The clear is unconditional
        // on the (committed, aborted) axis - both terminate the saga's
        // visibility window for this leaf, and any shadow marker
        // installed for a different saga's txid is untouched.
        ClearSagaShadow(transactionId);

        // Forward the projection-hash delta from any drained pending
        // bucket and / or per-key backstop writes to the parent
        // internal node so the chained subtree fold stays current.
        // No-op when this terminal landed on the abort path or
        // alreadyFlipped short-circuit branch (no StoreEntry calls
        // ran, so _digestDirty is still false).
        await PublishDigestUpwardAsync();
    }

    /// <summary>
    /// Destination-side shadow markers installed by the split
    /// coordinator naming, for each key whose virtual slot is
    /// migrating into this leaf, the in-flight source-side sagas
    /// whose prepared mutations touched that key. The read path
    /// consults this map whenever it is about to surface an
    /// <see cref="LwwValue{T}.IsMigrated"/>=<c>true</c> value, and
    /// raises <see cref="StaleShardRoutingException"/> for any
    /// shadowing saga that the registry has flipped to
    /// <see cref="TxStatus.Committed"/> but whose backstop terminal
    /// has not yet reached this leaf - so the
    /// <c>LatticeGrain</c> deadline-bounded retry loop re-fans once
    /// the backstop arrives. In-flight and aborted sagas are
    /// strict-isolation-correct on the migrated pre-saga value and
    /// pass through.
    /// <para>
    /// Lazily allocated - the steady-state path (no active split
    /// touching this leaf) leaves it null and incurs zero overhead
    /// in the read hot path beyond a single null check. Cleared
    /// per-saga by <see cref="ApplyTxTerminalAsync"/> when the
    /// saga's terminal lands on this leaf, so the per-saga footprint
    /// is bounded by saga lifetime.
    /// </para>
    /// </summary>
    private Dictionary<string, HashSet<Guid>>? _shadowedSagas;

    /// <inheritdoc />
    public Task MarkSagaShadowAsync(Guid transactionId, IReadOnlyList<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (transactionId == Guid.Empty)
            throw new ArgumentException("Transaction id must be non-empty.", nameof(transactionId));

        if (keys.Count == 0)
            return Task.CompletedTask;

        _shadowedSagas ??= new Dictionary<string, HashSet<Guid>>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            if (string.IsNullOrEmpty(key))
                continue;
            if (!_shadowedSagas.TryGetValue(key, out var sagas))
            {
                sagas = new HashSet<Guid>();
                _shadowedSagas[key] = sagas;
            }
            sagas.Add(transactionId);
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Removes <paramref name="transactionId"/> from every key's
    /// shadow set, prunes any empty sets, and releases the map when
    /// it falls empty. Invoked by <see cref="ApplyTxTerminalAsync"/>
    /// on every terminal application regardless of decision, so the
    /// guard has a bounded lifetime tied to saga progress.
    /// </summary>
    private void ClearSagaShadow(Guid transactionId)
    {
        if (_shadowedSagas is null || _shadowedSagas.Count == 0) return;

        List<string>? emptyKeys = null;
        foreach (var (key, sagas) in _shadowedSagas)
        {
            if (sagas.Remove(transactionId) && sagas.Count == 0)
            {
                emptyKeys ??= new List<string>();
                emptyKeys.Add(key);
            }
        }
        if (emptyKeys is not null)
        {
            foreach (var key in emptyKeys)
                _shadowedSagas.Remove(key);
        }
        if (_shadowedSagas.Count == 0)
            _shadowedSagas = null;
    }

    /// <summary>
    /// Returns <c>true</c> when a destination-side shadow marker is
    /// installed for <paramref name="key"/>, returning the captured
    /// txid set. The read path uses this signal to decide whether to
    /// consult the registry for a shadow-routing decision on an
    /// <see cref="LwwValue{T}.IsMigrated"/>=<c>true</c> value.
    /// </summary>
    private bool TryGetShadowedSagas(string key, out HashSet<Guid> sagas)
    {
        if (_shadowedSagas is not null && _shadowedSagas.TryGetValue(key, out var s) && s.Count > 0)
        {
            sagas = s;
            return true;
        }
        sagas = null!;
        return false;
    }

    /// <summary>
    /// Decides whether the read path is safe to surface an
    /// <see cref="LwwValue{T}.IsMigrated"/>=<c>true</c> value for a
    /// key that carries a destination-side shadow marker. Resolves
    /// every shadowing saga's <see cref="TxStatus"/> through the
    /// per-tree registry (or the ambient
    /// <see cref="LatticeRegistrySnapshotContext"/> snapshot when one
    /// is in scope) and applies the per-saga rule:
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="TxStatus.InFlight"/> / <see cref="TxStatus.Aborted"/>:
    ///     the migrated pre-saga value is the strict-isolation-correct
    ///     answer, the saga is safe to pass through.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="TxStatus.Committed"/> with backstop already
    ///     applied (txid in <see cref="_recentlyTerminal"/>): the
    ///     value in <c>Entries[K]</c> is now post-saga and safe to
    ///     serve.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="TxStatus.Committed"/> without backstop: serving
    ///     the migrated pre-saga value would violate atomic visibility
    ///     against any sibling leaf whose backstop has already landed.
    ///     Returns <c>false</c> so the caller raises
    ///     <see cref="StaleShardRoutingException"/>.
    ///   </description></item>
    /// </list>
    /// </summary>
    private async ValueTask<bool> IsShadowedReadSafeAsync(HashSet<Guid> sagas)
    {
        foreach (var txid in sagas)
        {
            var status = await ResolvePendingStatusAsync(txid);
            if (status != TxStatus.Committed) continue;
            // Committed: safe only if the backstop terminal has already
            // been applied here. _recentlyTerminal is set unconditionally
            // by every ApplyTxTerminalAsync exit path, so it is the
            // single source of truth for "this saga's terminal has
            // landed on this leaf".
            if (_recentlyTerminal is not null && _recentlyTerminal.Contains(txid))
                continue;
            return false;
        }
        return true;
    }

    #if LATTICE_DIAG
    /// <summary>
    /// DIAG: decode the round prefix from a chaos-test value of
    /// shape <c>v-NNN-II</c>, where <c>NNN</c> is the round and
    /// <c>II</c> is the key index. Returns <c>-1</c> for any value
    /// that doesn't match the test's format.
    /// </summary>
    private static int DiagDecodeRound(byte[]? value)
    {
        // Mirror DiagSink.DecodeRound: 'v-NNN' is 5 bytes; the
        // previous Length < 6 guard rejected every legal value.
        if (value is null || value.Length < 3) return -1;
        if (value[0] != (byte)'v' || value[1] != (byte)'-') return -1;
        int round = 0;
        for (int i = 2; i < value.Length; i++)
        {
            var c = value[i];
            if (c < (byte)'0' || c > (byte)'9') return -1;
            round = round * 10 + (c - (byte)'0');
        }
        return round;
    }
#endif
}
