using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationApplier"/> implementation. Resolves
/// the per-origin high-water-mark for the entry's
/// <c>(treeId, originClusterId)</c> pair, filters re-delivery, runs
/// the causal-plus dependency check - parking entries whose
/// declared <see cref="WalRecord.VectorClock"/> is not yet
/// dominated by the local vector clock - and routes the entry through
/// the core library's <see cref="IReplicationApplyGrain"/> seam so
/// the persisted <c>LwwValue&lt;byte[]&gt;</c> carries the remote
/// cluster's HLC and origin id verbatim. The HWM is advanced only
/// after the apply returns successfully; every advance triggers a
/// drain of the per-tree causal-apply buffer so blocked entries
/// whose deps are now satisfied wake up and apply in FIFO order.
/// </summary>
internal sealed partial class ReplicationApplier(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    LocalVectorClockCache localVectorClockCache,
    OrMapShapeRegistry? orMapShapes = null,
    ILogger<ReplicationApplier>? logger = null) : IReplicationApplier
{
    private readonly ILogger<ReplicationApplier> _logger =
        logger ?? NullLogger<ReplicationApplier>.Instance;

    /// <summary>
    /// Per-tree causal-apply buffers, lazily created on first park.
    /// Each tree's buffer is independent - there is no cross-tree
    /// coordination. The map itself is concurrent because Orleans
    /// grain calls into a singleton applier may interleave across
    /// trees; per-buffer concurrency is enforced by the buffer's
    /// internal lock.
    /// </summary>
    private readonly ConcurrentDictionary<string, CausalApplyBuffer> _buffers = new(StringComparer.Ordinal);

    /// <summary>
    /// Per-tree shadow-forward dedupe caches, lazily created on first
    /// non-range-delete entry per tree. Each cache holds a bounded
    /// FIFO of recently-applied
    /// <c>(originClusterId, timestamp, key, op)</c> identity tuples
    /// and rejects the duplicate-emit pair structural rewrites
    /// (shard split / merge / saga compensate) generate when they
    /// shadow-forward a user write into a different shard. The cache
    /// is a fast-path race-killer: under concurrent inbound
    /// delivery, both duplicate emits can otherwise observe the same
    /// pre-advance per-origin high-water-mark and both pass the HWM
    /// check before either advances it. Correctness is still bounded
    /// by the HWM - cache eviction under sustained churn cannot
    /// cause a re-merge.
    /// </summary>
    private readonly ConcurrentDictionary<string, RecentApplyCache> _dedupeCaches =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Per-<c>(treeId, originClusterId)</c> last-applied source HLC. Used
    /// to surface a transport-side regression that breaks the per-origin
    /// FIFO invariant the causal-apply pipeline relies on for occupancy
    /// bounds: under correct sender + transport behaviour every
    /// successful apply for a given origin has an HLC strictly greater
    /// than the previous successful apply for that origin (the producer's
    /// partitioned change feed yields per-shard in WAL-offset order and
    /// each shard's WAL is HLC-monotonic per origin). A violation does
    /// not change apply behaviour - the entry is still applied and the
    /// HWM is still advanced - it only increments the
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>
    /// counter so an alert on <c>rate &gt; 0</c> flags the regression.
    /// Updated on successful apply (not on park) so the invariant tracks
    /// "what has been merged" rather than "what has been observed".
    /// </summary>
    private readonly ConcurrentDictionary<(string TreeId, string Origin), HybridLogicalClock> _lastAppliedSourceHlc =
        new();

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        // Apply-duration instrumentation: record the wall-clock duration
        // of every terminal apply outcome (success / dedup / failure /
        // parked-causal-buffer) into the apply.duration histogram. The
        // stopwatch is allocation-free (a long timestamp captured via
        // Stopwatch.GetTimestamp); the outcome local is updated before
        // each return so the finally records the matching tag value.
        // A throw out of the body unwinds with the default outcome
        // (failure), which is correct because every uncaught exception
        // here represents a failed apply attempt.
        var startTimestamp = Stopwatch.GetTimestamp();
        var outcome = LatticeReplicationMetrics.OutcomeFailure;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrEmpty(entry.TreeId))
            {
                throw new ArgumentException("WalRecord.TreeId must be non-empty.", nameof(entry));
            }

            if (string.IsNullOrEmpty(entry.OriginClusterId))
            {
                throw new ArgumentException(
                    "WalRecord.OriginClusterId must be non-empty for replication apply.",
                    nameof(entry));
            }

            // Defence-in-depth: tombstone-reap envelopes
            // (`MutationKind.Tombstone`) are local structural cleanup
            // records and are filtered out at the producer boundary by
            // `ReplicationShipperGrain.ShouldShip` / `ChangeFeed.Subscribe`.
            // A receiver should therefore never see one in the steady
            // state; an older shipper or a hand-built apply call site
            // could still deliver one. Surface it as an explicit
            // dedup-shaped no-op (Applied=false, HWM unchanged) so the
            // entry is acknowledged without faulting the apply loop.
            // The category signal is not preserved through `WalRecord`
            // (no Category slot), so the guard keys on `Op` directly.
            if (entry.Op == MutationKind.Tombstone)
            {
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            var resolved = options.Get(entry.TreeId);
            if (string.Equals(entry.OriginClusterId, resolved.ClusterId, StringComparison.Ordinal))
            {
                // Defence-in-depth: a local-origin entry must never be applied
                // back onto its authoring cluster. The outbound ship loop's
                // origin filter already prevents this in the steady state, but
                // hand-built apply pipelines and tests can still hand us such
                // an entry - surface it as an explicit no-op rather than
                // silently merging into the same cluster's state.
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            // Range deletes carry HybridLogicalClock.Zero by design (the walk
            // produces many per-leaf HLCs that cannot be faithfully collapsed),
            // so per-origin HWM dedupe does not apply to them. Range applies
            // are naturally idempotent at the leaf layer.
            if (entry.Op == MutationKind.DeleteRange)
            {
                // Defence-in-depth: a DeleteRange entry tagged with
                // atomic-batch metadata (AtomicBatchSize > 0) is a
                // producer-contract violation - the producer's
                // SetManyAtomicAsync surface emits only Set/Delete, never
                // DeleteRange, so atomic-batch stamps on a range op are
                // intrinsic ambiguity (no consistent saga key, no
                // single-HLC commit point). Surface it as an explicit
                // ArgumentException so the producer fails fast rather
                // than the receiver silently applying a non-atomic range
                // delete that carries an unfulfilled atomic-batch
                // promise. The violation is producer-shaped, not
                // receiver-shaped - the wire slot remains additive on
                // every WalRecord so the receiver-side prepared-Set /
                // prepared-Delete primitive can route Set/Delete entries
                // through the per-tx pending bucket while DeleteRange
                // entries are explicitly rejected here.
                if (entry.AtomicBatchSize > 0)
                {
                    throw new ArgumentException(
                        "WalRecord.Op=DeleteRange must not carry atomic-batch metadata "
                        + "(AtomicBatchSize > 0). Atomic batches must contain only Set / Delete entries; "
                        + "range deletes are emitted by the producer's DeleteRangeAsync surface, not "
                        + "SetManyAtomicAsync.",
                        nameof(entry));
                }

                await ApplyRangeAsync(entry, cancellationToken);
                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero };
            }

            // Saga terminal-mark records (TxCommit / TxAbort) are
            // saga-id-keyed and idempotent on the receiver
            // (per-tree TxRegistry repeat-same-outcome no-op + per-leaf
            // _recentlyTerminal HashSet dedup). They bypass the
            // per-origin HWM check, the shadow-forward dedup cache, and
            // the causal-buffer parking path: those primitives are
            // per-key data-flow dedup primitives and have no defined
            // semantics on saga linearization records. The receiver-side
            // ApplyTxTerminalAsync routes the mark through the per-tree
            // ITxRegistryGrain (the linearization point readers dial
            // back through) and the addressed shard's
            // AppendTxTerminalAsync under a LatticeHlcOverrideContext
            // so the receiver's local WAL append re-stamps the source
            // cluster's terminal HLC verbatim - preserving the
            // cross-cluster ordering invariant on receiver replays.
            if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort)
            {
                await ApplyTxTerminalCoreAsync(entry, cancellationToken);
                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero };
            }

            var hwmGrain = GetHwmGrain(entry.TreeId);
            var hwm = await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);

            // Bootstrap-drain bypass: the receiver-side coordinator
            // wraps its per-entry ApplyAsync calls in a
            // <see cref="LatticeBootstrapApplyContext"/> scope. The
            // snapshot exporter walks shards and leaves in arbitrary
            // order rather than HLC order, so prepared rows for the
            // same saga across different shards can arrive with
            // non-monotonic per-origin HLCs. Applying the per-origin
            // HWM gate to those entries drops every row whose source
            // HLC is below the highest already-seen source HLC -
            // leaving the saga's per-tx pending bucket with a strict
            // subset of its keys and producing a partial-saga view
            // when the matching terminal arrives. The post-drain
            // <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
            // call atomically establishes the per-origin HWM at the
            // snapshot's AsOfHlc, so steady-state dedup is preserved
            // across the bootstrap-to-incremental handoff. Receiver-side
            // idempotency during the drain is upheld by leaf-level LWW
            // (re-delivery is a no-op), the per-leaf _recentlyTerminal
            // guard (re-arriving terminals are dropped), and the
            // per-tree ITxRegistryGrain repeat-same-outcome rule
            // (re-marking a saga is a no-op). See
            // <see cref="LatticeBootstrapApplyContext"/> for the
            // rationale and the dedup primitives that remain in force.
            var isBootstrapDrain = LatticeBootstrapApplyContext.IsActive;
            if (!isBootstrapDrain && entry.Timestamp <= hwm)
            {
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = hwm };
            }

            // Shadow-forward dedupe cache: a structural rewrite (shard
            // split / merge / saga compensate) that shadow-forwards a
            // user write into a different shard generates a duplicate
            // emit pair with identical (origin, hlc, key, op). The
            // per-origin HWM check above catches the second delivery
            // when it is sequential (first has already advanced the
            // HWM), but a concurrent inbound delivery can otherwise
            // observe the same pre-advance HWM on both deliveries and
            // both pass before either advances it. The cache is the
            // fast-path race-killer for that scenario. The check sits
            // after HWM so HWM-deduped entries do not pollute the
            // cache (which would break operator-driven re-pin
            // scenarios where a lower frontier must re-admit a
            // previously-deduped identity tuple). Range deletes bypass
            // it entirely because they carry HLC.Zero (ambiguous
            // identity).
            var cache = _dedupeCaches.GetOrAdd(
                entry.TreeId,
                static (_, capacity) => new RecentApplyCache(capacity),
                resolved.ShadowForwardDedupeCacheSize);
            if (!cache.TryAdd(entry))
            {
                outcome = LatticeReplicationMetrics.OutcomeShadowForwardDedup;
                return new ApplyResult { Applied = false, HighWaterMark = hwm };
            }

            // Roll back the cache reservation if the apply pipeline
            // throws (or is cancelled) after TryAdd succeeded. Without
            // rollback, a transient apply failure would leave a phantom
            // cache entry that suppresses the transport's retry path:
            // the retry would observe TryAdd=false (cache hit), classify
            // the call as Applied=false (shadow-forward-dedup), and the
            // dead-letter decorator's retry-counter contract would clear
            // the failure counter on what looks like a filtered call -,
            // silently dropping the entry until FIFO eviction admits a
            // future retry. The park branch returns normally inside the
            // try, so its cache reservation is correctly retained: the
            // drained entry routes through ApplyPointAsync directly,
            // bypassing the cache, and the retained reservation
            // continues to suppress duplicate-emit pairs of the parked
            // entry that arrive while it is buffered.
            try
            {
                // Causal-plus dependency check. Skip the fetch entirely
                // when the entry carries no declared dependencies - legacy
                // peers and pre-causal-plus entries decode VectorClock as null
                // and must continue to apply unconditionally on the existing
                // HWM-only path so this code is wire-compatible with the
                // additive vector-clock schema slot.
                if (HasCausalDependencies(entry))
                {
                    var localVc = await hwmGrain.GetVectorAsync(cancellationToken);
                    if (!CausalApplyBuffer.DependenciesSatisfied(entry, localVc))
                    {
                        await ParkAsync(entry, resolved, cancellationToken);
                        outcome = LatticeReplicationMetrics.OutcomeParkedCausalBuffer;
                        return new ApplyResult { Applied = false, HighWaterMark = hwm };
                    }
                }

                await ApplyPointAsync(entry);
                RecordApplyLag(entry);

                // Bootstrap-drain bypass: skip the per-origin HWM
                // advance and the per-(tree, origin) FIFO-violation
                // tracker for entries delivered through a
                // <see cref="LatticeBootstrapApplyContext"/> scope.
                // The drain produces entries whose per-origin HLCs
                // are not globally ordered (the snapshot exporter
                // visits shards/leaves in arbitrary order), so a
                // mid-drain advance can later suppress a still-pending
                // saga key with a strictly-earlier HLC and break
                // per-saga all-or-nothing visibility, and recording
                // those non-monotonic HLCs on
                // <c>_lastAppliedSourceHlc</c> would surface every
                // out-of-order shard arrival as a spurious
                // <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>
                // increment (the counter is documented as a
                // transport-side FIFO-regression signal, which is
                // expressly not what bootstrap delivery is). The
                // post-drain
                // <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
                // call installs the per-origin HWM at the snapshot
                // cut atomically; the first live-incremental entry
                // delivered after the pin seeds
                // <c>_lastAppliedSourceHlc</c> at the producer's
                // first post-pin HLC, which is the canonical FIFO
                // anchor for steady-state replication.
                if (isBootstrapDrain)
                {
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    return new ApplyResult { Applied = true, HighWaterMark = hwm };
                }

                RecordFifoState(entry);

                // Advance the HWM only after the apply commits.
                var advanced = await hwmGrain.TryAdvanceAsync(entry.OriginClusterId!, entry.Timestamp, cancellationToken);
                var newHwm = advanced
                    ? entry.Timestamp
                    : await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);

                // The advance may have unblocked entries parked by an earlier
                // delivery whose deps included this origin's diagonal. Drain
                // FIFO until the buffer reaches a fixed point - each drained
                // apply may itself advance the local vector clock, so re-fetch
                // before each pass.
                if (advanced)
                {
                    // Mirror the foreign advance into the producer-side
                    // local vector clock cache so a subsequent local emit
                    // stamps a VectorClock that reflects the just-applied
                    // foreign entry. Without this, a producer would emit
                    // a VC with the foreign origin's entry one HLC behind
                    // and a remote receiver could park the resulting
                    // entry until the next cold-start refreshes the
                    // producer's view.
                    localVectorClockCache.AdvanceForeign(
                        entry.TreeId,
                        entry.OriginClusterId!,
                        entry.Timestamp);
                    await DrainBufferAsync(entry.TreeId, hwmGrain, resolved, cancellationToken);
                }

                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = newHwm };
            }
            catch
            {
                cache.Remove(entry);
                throw;
            }
        }
        finally
        {
            RecordApplyDuration(entry.TreeId, entry.OriginClusterId ?? string.Empty, startTimestamp, outcome);
        }
    }

    /// <summary>
    /// Records a single sample on the <see cref="LatticeReplicationMetrics.ApplyDuration"/>
    /// histogram. Skipped when <paramref name="treeId"/> is empty so a
    /// validation throw on the tree-id guard does not publish a histogram
    /// sample with an empty <c>tree</c> tag (which would be unusable for
    /// per-tree alerting). The <paramref name="peerOriginClusterId"/> is
    /// emitted on the <see cref="LatticeReplicationMetrics.TagPeer"/>
    /// dimension so per-source-peer break-down is honoured by the
    /// instrument's documented schema; an empty value (e.g. when the
    /// origin guard threw before any apply work happened) is emitted
    /// verbatim rather than skipped so the histogram still records the
    /// failure outcome and operators get a stable cardinality. The
    /// duration is read via <see cref="Stopwatch.GetElapsedTime(long)"/>,
    /// which is allocation-free.
    /// </summary>
    private static void RecordApplyDuration(string treeId, string peerOriginClusterId, long startTimestamp, string outcome)
    {
        if (string.IsNullOrEmpty(treeId))
        {
            return;
        }

        var elapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;
        LatticeReplicationMetrics.ApplyDuration.Record(
            elapsedMs,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peerOriginClusterId ?? string.Empty),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome));
    }

    private static bool HasCausalDependencies(WalRecord entry) =>
        entry.VectorClock is { Entries.Count: > 0 };

    private async Task ParkAsync(
        WalRecord entry,
        LatticeReplicationOptions resolved,
        CancellationToken cancellationToken)
    {
        var buffer = _buffers.GetOrAdd(entry.TreeId, static treeId => new CausalApplyBuffer(treeId));
        var outcome = buffer.TryAdd(
            entry,
            resolved.CausalBufferMaxEntries,
            resolved.CausalBufferMaxBytes,
            out var evicted);

        if (outcome == AddOutcome.AddedWithEviction && evicted.Count > 0)
        {
            var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
            foreach (var displaced in evicted)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await dlq.EnqueueAsync(
                    displaced,
                    failureReason: "Causal-apply buffer full; evicted blocked entry to make room.",
                    retryCount: 0,
                    reasonTag: LatticeReplicationMetrics.ReasonHlcSkew,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task DrainBufferAsync(
        string treeId,
        IReplicationHighWaterMarkGrain hwmGrain,
        LatticeReplicationOptions resolved,
        CancellationToken cancellationToken)
    {
        if (!_buffers.TryGetValue(treeId, out var buffer) || buffer.Count == 0)
        {
            return;
        }

        // Iterate to a fixed point: each drained apply may advance the
        // local vector clock and unblock further entries on the next
        // pass. Bounded by the buffer's current size + any retried
        // entries that fail and re-park; in practice convergence is
        // O(n) where n is the gap between received and applied.
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var localVc = await hwmGrain.GetVectorAsync(cancellationToken);
            var ready = buffer.DrainSatisfied(localVc);
            if (ready.Count == 0)
            {
                return;
            }

            foreach (var ent in ready)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await ApplyPointAsync(ent);
                    RecordApplyLag(ent);
                    RecordFifoState(ent);
                    var advancedDrained = await hwmGrain
                        .TryAdvanceAsync(ent.OriginClusterId!, ent.Timestamp, cancellationToken)
                        .ConfigureAwait(false);
                    if (advancedDrained)
                    {
                        // Mirror the drained foreign advance into the
                        // producer-side cache so the next local emit
                        // observes it. The drain loop's next pass
                        // re-fetches localVc from the grain and may
                        // unblock further entries - the producer cache
                        // is updated independently here so a concurrent
                        // commit-time observer sees the advance even
                        // before the drain loop completes.
                        localVectorClockCache.AdvanceForeign(
                            ent.TreeId,
                            ent.OriginClusterId!,
                            ent.Timestamp);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // A drained apply has no transport-level retry
                    // path: the original delivery was already ack'd
                    // when ApplyAsync returned for the entry that
                    // unblocked this one. Route the failed drained
                    // entry to the dead-letter queue rather than
                    // dropping it silently. ArgumentException /
                    // InvalidOperationException are schema-shaped
                    // faults; everything else is unknown.
                    var reasonTag = ex is ArgumentException or InvalidOperationException
                        ? LatticeReplicationMetrics.ReasonSchema
                        : LatticeReplicationMetrics.ReasonUnknown;
                    var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(ent.TreeId);
                    await dlq.EnqueueAsync(
                        ent,
                        failureReason: ex.Message ?? "<no message>",
                        retryCount: 0,
                        reasonTag: reasonTag,
                        cancellationToken).ConfigureAwait(false);

                    // Roll back the shadow-forward cache reservation
                    // that was made when ApplyAsync originally parked
                    // this entry. Without rollback, an operator-driven
                    // retry from the DLQ would observe TryAdd=false on
                    // the cache and be classified as Applied=false
                    // (shadow-forward-dedup); the dead-letter
                    // decorator's "Applied=false clears the counter"
                    // contract would then silently drop the entry
                    // until FIFO eviction. The HWM was never advanced
                    // for this entry (apply threw before TryAdvance),
                    // so HWM dedupe will not suppress the retry - the
                    // cache rollback is the only step required.
                    if (_dedupeCaches.TryGetValue(ent.TreeId, out var cache))
                    {
                        cache.Remove(ent);
                    }
                }
            }
        }
    }

    private Task ApplyPointAsync(WalRecord entry)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);

        // Saga prepare-phase entries (IsPrepared==true) route through
        // the prepared-apply seam so the receiver leaf parks them in
        // its per-tx pending bucket rather than flipping the visible
        // projection. The terminal record arriving subsequently via
        // ApplyTxTerminalAsync is the per-shard linearization point
        // that flips pending into visible (or drops on abort). Only
        // LwwRegister mode is supported - the saga surface
        // (SetManyAtomicAsync) emits only Set/Delete in that mode;
        // CRDT modes have no saga-prepared shape on the wire.
        if (entry.IsPrepared && entry.Op is MutationKind.Set or MutationKind.Delete)
        {
            return ApplyPreparedPointAsync(apply, entry);
        }

        return entry.Op switch
        {
            // The null-Value guard applies only to LwwRegister, whose
            // Value is the canonical payload the receiver writes
            // verbatim. CRDT-mode entries carry their
            // post-merge contribution exclusively via Delta, so a
            // null Value on a CRDT-mode Set is the expected wire
            // shape (the encoder strips Value when a typed Delta is
            // present). A CRDT-mode entry that arrives with both
            // Value and Delta null is still a hard error and is
            // surfaced inside the typed-delta dispatch (each
            // ApplyTypedDeltaAsync overload validates Delta itself).
            MutationKind.Set when entry.Value is null
                && entry.Mode == LatticeMergeMode.LwwRegister
                => throw new ArgumentException(
                    "WalRecord.Value must be non-null for MutationKind.Set on LwwRegister entries.",
                    nameof(entry)),
            MutationKind.Set => entry.Mode switch
            {
                LatticeMergeMode.LwwRegister => apply.ApplySetAsync(
                    entry.Key,
                    entry.Value!,
                    entry.Timestamp,
                    entry.OriginClusterId!,
                    sourceVectorClock: null,
                    entry.ExpiresAtTicks),
                LatticeMergeMode.OrSet => ApplyTypedDeltaAsync<OrSet, OrSetDelta>(
                    entry,
                    static (state, delta) => state.MergeDelta(delta),
                    static () => new OrSet()),
                LatticeMergeMode.PnCounter => ApplyTypedDeltaAsync<PnCounter, PnCounterDelta>(
                    entry,
                    static (state, delta) => state.MergeDelta(delta),
                    static () => new PnCounter()),
                LatticeMergeMode.VersionVector => ApplyTypedDeltaAsync<VersionVector, VersionVectorDelta>(
                    entry,
                    static (state, delta) => state.MergeDelta(delta),
                    static () => new VersionVector()),
                LatticeMergeMode.MvRegister => ApplyTypedDeltaAsync<MvRegister, MvRegisterDelta>(
                    entry,
                    static (state, delta) => state.MergeDelta(delta),
                    static () => new MvRegister()),
                LatticeMergeMode.OrMap => ApplyOrMapDeltaAsync(entry),
                _ => throw new InvalidOperationException(
                    $"WalRecord on tree '{entry.TreeId}' carries unrecognised replication mode '{entry.Mode}' "
                    + "(value="
                    + ((int)entry.Mode).ToString(System.Globalization.CultureInfo.InvariantCulture)
                    + "). The receiver has no apply rule registered for this mode; in a future release such "
                    + "entries will be routed to a dead-letter queue."),
            },
            MutationKind.Delete => apply.ApplyDeleteAsync(
                entry.Key,
                entry.Timestamp,
                entry.OriginClusterId!,
                sourceVectorClock: null),
            _ => throw new InvalidOperationException(
                $"Unsupported point-apply op {entry.Op} for entry on tree '{entry.TreeId}'."),
        };
    }

    /// <summary>
    /// Routes an inbound saga prepare-phase entry through the
    /// receiver-side <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/>
    /// or <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>
    /// seam. The seam wraps the per-key write in the same ambient
    /// context stack the source saga's prepare step would have produced
    /// (<see cref="LatticePreparedContext"/>, 
    /// <see cref="LatticeOriginContext"/>, <see cref="LatticeVectorClockContext"/>, 
    /// <see cref="LatticeHlcOverrideContext"/>, 
    /// <see cref="LatticeAtomicBatchContext"/>, and the request-scope
    /// transaction id) so the receiver leaf routes the entry into its
    /// per-tx pending bucket instead of the visible projection.
    /// </summary>
    private static Task ApplyPreparedPointAsync(IReplicationApplyGrain apply, WalRecord entry)
    {
        if (entry.TransactionId == Guid.Empty)
        {
            throw new ArgumentException(
                $"WalRecord on tree '{entry.TreeId}' carries IsPrepared=true but TransactionId=Guid.Empty; "
                + "saga prepare-phase entries must stamp the saga's transaction id so the receiver can "
                + "route the entry into the correct per-leaf pending-tx bucket.",
                nameof(entry));
        }

        if (entry.Op == MutationKind.Set)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    "WalRecord.Value must be non-null for prepared MutationKind.Set.",
                    nameof(entry));
            }

            return apply.ApplyPreparedSetAsync(
                entry.Key,
                entry.Value,
                entry.Timestamp,
                entry.OriginClusterId!,
                sourceVectorClock: null,
                entry.ExpiresAtTicks,
                entry.TransactionId,
                entry.AtomicBatchSize,
                entry.AtomicBatchIndex);
        }

        return apply.ApplyPreparedDeleteAsync(
            entry.Key,
            entry.Timestamp,
            entry.OriginClusterId!,
            sourceVectorClock: null,
            entry.TransactionId,
            entry.AtomicBatchSize,
            entry.AtomicBatchIndex);
    }

    /// <summary>
    /// Routes an inbound saga terminal-mark record (TxCommit / TxAbort)
    /// through the receiver-side
    /// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/> seam.
    /// Resolves the source-side shard index from the typed
    /// <see cref="WalRecord.ShardIndex"/> slot, falling back to parsing
    /// <see cref="WalRecord.Key"/> for back-compat with pre-Option A WAL
    /// records authored before the typed slot was introduced.
    /// </summary>
    private Task ApplyTxTerminalCoreAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        var committed = entry.Op == MutationKind.TxCommit;

        int shardIndex;
        if (entry.ShardIndex > 0)
        {
            shardIndex = entry.ShardIndex;
        }
        else if (!string.IsNullOrEmpty(entry.Key)
            && int.TryParse(entry.Key, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            && parsed >= 0)
        {
            shardIndex = parsed;
        }
        else
        {
            throw new ArgumentException(
                $"WalRecord.Op={entry.Op} on tree '{entry.TreeId}' must carry a positive ShardIndex slot or a "
                + $"non-negative numeric Key (got Key='{entry.Key}', ShardIndex={entry.ShardIndex}).",
                nameof(entry));
        }

        if (entry.TransactionId == Guid.Empty)
        {
            throw new ArgumentException(
                $"WalRecord.Op={entry.Op} on tree '{entry.TreeId}' must carry a non-empty TransactionId so the "
                + "receiver can address the saga's per-tree TxRegistry mark.",
                nameof(entry));
        }

        return apply.ApplyTxTerminalAsync(
            entry.TransactionId,
            committed,
            shardIndex,
            entry.Timestamp,
            entry.OriginClusterId!,
            entry.AtomicShardCount,
            cancellationToken);
    }

    /// <summary>
    /// CAS retry budget for the read-merge-write loop used by typed CRDT
    /// delta applies (<see cref="LatticeMergeMode.OrSet"/>,
    /// <see cref="LatticeMergeMode.PnCounter"/>, <see cref="LatticeMergeMode.VersionVector"/>,
    /// <see cref="LatticeMergeMode.MvRegister"/>).
    /// Mirrors the budget the typed accessors (<see cref="OrSetAccessor.DefaultMaxAttempts"/>,
    /// <see cref="PnCounterAccessor.DefaultMaxAttempts"/>,
    /// <see cref="VersionVectorAccessor.DefaultMaxAttempts"/>,
    /// <see cref="MvRegisterAccessor{T}.DefaultMaxAttempts"/>) use for the
    /// authoring side, so a typical fan-in matches.
    /// </summary>
    private const int StateMergeMaxAttempts = 16;

    /// <summary>
    /// Routes an inbound CRDT-mode <see cref="MutationKind.Set"/> entry
    /// through the typed-delta receive path. The producer authored a
    /// public typed delta DTO (<typeparamref name="TDelta"/>) into
    /// <see cref="WalRecord.Delta"/> via the typed accessor's
    /// <see cref="LatticeDeltaContext"/> stamp. The receiver deserialises
    /// the DTO, loads the existing primitive (creating an empty one when
    /// the key is absent), folds the delta in via the primitive's
    /// instance <c>MergeDelta</c> method, and CAS-writes the merged
    /// state back. The full-state bytes in <see cref="WalRecord.Value"/>
    /// are intentionally ignored on this path; the wire contract carries
    /// them only for change-feed back-compat.
    /// </summary>
    private async Task ApplyTypedDeltaAsync<TState, TDelta>(
        WalRecord entry,
        Action<TState, TDelta> mergeDelta,
        Func<TState> emptyFactory)
        where TState : class
    {
        if (entry.Delta is null)
        {
            throw new ArgumentException(
                $"WalRecord.Delta must be non-null for {entry.Mode} typed-delta apply on tree "
                + $"'{entry.TreeId}', key '{entry.Key}'. The producer is required to stamp a typed CRDT delta "
                + "DTO into the Delta slot via the typed accessor surface; receivers cannot reconstruct the "
                + "wire-only causal information from the full-state Value bytes.",
                nameof(entry));
        }

        var lattice = grainFactory.GetGrain<ILattice>(entry.TreeId);
        var stateSerializer = JsonLatticeSerializer<TState>.Default;
        var deltaSerializer = JsonLatticeSerializer<TDelta>.Default;
        var incomingDelta = deltaSerializer.Deserialize(entry.Delta);

        // Stamp the remote origin onto the receiver-side mutation so the
        // outbound change-feed observer publishes the foreign origin and
        // the producer's outbound ship loop filters the resulting entry
        // back out (the durable, async-boundary-safe successor to the
        // legacy thread-local replay flag).
        using var scope = LatticeOriginContext.With(entry.OriginClusterId);

        for (var attempt = 0; attempt < StateMergeMaxAttempts; attempt++)
        {
            var versioned = await lattice.GetWithVersionAsync(entry.Key);
            var existing = versioned.Value is null
                ? emptyFactory()
                : stateSerializer.Deserialize(versioned.Value);
            mergeDelta(existing, incomingDelta);
            var bytes = stateSerializer.Serialize(existing);
            var ok = await lattice.SetIfVersionAsync(entry.Key, bytes, versioned.Version);
            if (ok)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"Replication typed-delta CAS budget exhausted after {StateMergeMaxAttempts} attempts on tree "
            + $"'{entry.TreeId}', key '{entry.Key}', mode '{entry.Mode}'. The receiver could not install the "
            + "merged state under optimistic concurrency; reduce contention on this key or increase the "
            + "budget in a future configuration knob.");
    }

    /// <summary>
    /// Routes an inbound <see cref="LatticeMergeMode.OrMap"/> entry through
    /// the registered <see cref="OrMapShape"/> for the entry's
    /// tree. The wire shape is generic over <c>(TKey, TValue)</c>, so the
    /// receiver cannot statically pick a deserialiser; the host registers
    /// the concrete pair via
    /// <see cref="LatticeServiceCollectionExtensions.AddOrMapShape{TKey, TValue}(ISiloBuilder, string)"/>
    /// before silo start, and this method looks the descriptor up by tree
    /// id and delegates to it. A tree configured for
    /// <see cref="LatticeMergeMode.OrMap"/> with no registered shape faults
    /// the apply so the misconfiguration is surfaced rather than silently
    /// dropping the entry.
    /// </summary>
    private async Task ApplyOrMapDeltaAsync(WalRecord entry)
    {
        if (entry.Delta is null)
        {
            throw new ArgumentException(
                $"WalRecord.Delta must be non-null for OrMap typed-delta apply on tree "
                + $"'{entry.TreeId}', key '{entry.Key}'. The producer is required to stamp a typed CRDT delta "
                + "DTO into the Delta slot via the typed accessor surface; receivers cannot reconstruct the "
                + "wire-only causal information from the full-state Value bytes.",
                nameof(entry));
        }

        var shape = orMapShapes?.TryGet(entry.TreeId)
            ?? throw new InvalidOperationException(
                $"Tree '{entry.TreeId}' is configured for LatticeMergeMode.OrMap but no "
                + "OrMapShape is registered with the receiver. Call "
                + "siloBuilder.AddOrMapShape<TKey, TValue>(\"" + entry.TreeId + "\") on the "
                + "service collection before silo start so the receiver can deserialise the generic delta.");

        var incomingDelta = shape.DeserializeDelta(entry.Delta);
        var lattice = grainFactory.GetGrain<ILattice>(entry.TreeId);

        using var scope = LatticeOriginContext.With(entry.OriginClusterId);

        for (var attempt = 0; attempt < StateMergeMaxAttempts; attempt++)
        {
            var versioned = await lattice.GetWithVersionAsync(entry.Key);
            var existing = versioned.Value is null
                ? shape.CreateEmpty()
                : shape.DeserializeState(versioned.Value);
            shape.MergeDelta(existing, incomingDelta);
            var bytes = shape.SerializeState(existing);
            var ok = await lattice.SetIfVersionAsync(entry.Key, bytes, versioned.Version);
            if (ok)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"Replication OrMap typed-delta CAS budget exhausted after {StateMergeMaxAttempts} attempts on tree "
            + $"'{entry.TreeId}', key '{entry.Key}'. The receiver could not install the merged state under "
            + "optimistic concurrency; reduce contention on this key or increase the budget in a future "
            + "configuration knob.");
    }

    private Task ApplyRangeAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (entry.EndExclusiveKey is null)
        {
            throw new ArgumentException(
                "WalRecord.EndExclusiveKey must be non-null for MutationKind.DeleteRange.",
                nameof(entry));
        }

        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        // Pass entry.Timestamp through verbatim: producers stamp the
        // range-delete issue HLC into this slot via
        // ShardRootGrain.PublishDeleteRangeAsync; receivers pin every
        // per-leaf tombstone to this HLC on the apply seam so the
        // cross-origin LWW invariant is preserved. Legacy entries
        // persisted before this invariant was enforced carry
        // HybridLogicalClock.Zero; the apply seam detects the sentinel
        // and falls back to fresh-local-HLC stamping for back-compat.
        return apply.ApplyDeleteRangeAsync(
            entry.Key,
            entry.EndExclusiveKey,
            entry.Timestamp,
            entry.OriginClusterId!,
            sourceVectorClock: null);
    }

    private IReplicationHighWaterMarkGrain GetHwmGrain(string treeId) =>
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeId);

    /// <summary>
    /// Records the receiver-side replication-lag sample for a successfully
    /// applied point operation. Lag is computed as
    /// <c>now - entry.Timestamp.WallClockTicks</c> in milliseconds and
    /// clamped at zero - a future-dated source HLC (the producing cluster's
    /// wall clock leads the receiver's) reports as <c>0</c> rather than a
    /// negative sample, which would corrupt downstream histograms.
    /// </summary>
    private static void RecordApplyLag(WalRecord entry)
    {
        var sourceTicks = entry.Timestamp.WallClockTicks;
        if (sourceTicks <= 0)
        {
            // Source HLC was never stamped (HybridLogicalClock.Zero or a
            // pathological negative). Reporting "now - 0" would publish
            // a garbage multi-decade lag value; skip the sample instead.
            return;
        }

        var deltaTicks = DateTime.UtcNow.Ticks - sourceTicks;
        if (deltaTicks < 0)
        {
            deltaTicks = 0;
        }

        var ms = deltaTicks / (double)TimeSpan.TicksPerMillisecond;
        LatticeReplicationMetrics.ApplyLag.Record(
            ms,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, entry.OriginClusterId ?? string.Empty));
    }

    /// <summary>
    /// Updates the per-<c>(treeId, originClusterId)</c> last-applied
    /// source-HLC tracker for a successfully applied point operation
    /// and increments
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/> when
    /// the entry's HLC is strictly less than the previously recorded
    /// value - surfacing a transport-side regression that broke the
    /// per-origin FIFO invariant. The recorded value is the pointwise
    /// max so a benign re-delivery (which the HWM check already filters
    /// upstream) does not silently downgrade the tracker on the rare
    /// path where it slipped through.
    /// </summary>
    private void RecordFifoState(WalRecord entry)
    {
        // Skip range deletes (carry HLC.Zero) and entries with a missing
        // origin (defensive - the caller-side guard rejects empty origins
        // before we get here).
        if (entry.Op == MutationKind.DeleteRange || string.IsNullOrEmpty(entry.OriginClusterId))
        {
            return;
        }

        var key = (entry.TreeId, entry.OriginClusterId!);
        var ts = entry.Timestamp;

        // Allocation-free CAS loop: avoids the closure that an
        // AddOrUpdate factory would capture per call. Under steady-state
        // monotonic delivery the loop runs exactly once.
        while (true)
        {
            if (!_lastAppliedSourceHlc.TryGetValue(key, out var existing))
            {
                if (_lastAppliedSourceHlc.TryAdd(key, ts))
                {
                    return;
                }
                continue;
            }

            if (ts < existing)
            {
                // Strictly out-of-order: surface the regression and
                // keep the existing (higher) recorded HLC so a future
                // monotonic delivery is compared against the true
                // pointwise max.
                LatticeReplicationMetrics.ApplyFifoViolations.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, entry.OriginClusterId!));
                return;
            }

            if (ts == existing || _lastAppliedSourceHlc.TryUpdate(key, ts, existing))
            {
                return;
            }
        }
    }
}
