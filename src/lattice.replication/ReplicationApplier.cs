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
/// the causal-plus dependency check — parking entries whose
/// declared <see cref="ReplogEntry.VectorClock"/> is not yet
/// dominated by the local vector clock — and routes the entry through
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
    ILatticeReplicationCursorRegistry? cursorRegistry = null,
    ILogger<ReplicationApplier>? logger = null) : IReplicationApplier
{
    private readonly ILogger<ReplicationApplier> _logger =
        logger ?? NullLogger<ReplicationApplier>.Instance;

    /// <summary>
    /// Per-tree causal-apply buffers, lazily created on first park.
    /// Each tree's buffer is independent — there is no cross-tree
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
    /// by the HWM — cache eviction under sustained churn cannot
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
    /// not change apply behaviour — the entry is still applied and the
    /// HWM is still advanced — it only increments the
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>
    /// counter so an alert on <c>rate &gt; 0</c> flags the regression.
    /// Updated on successful apply (not on park) so the invariant tracks
    /// "what has been merged" rather than "what has been observed".
    /// </summary>
    private readonly ConcurrentDictionary<(string TreeId, string Origin), HybridLogicalClock> _lastAppliedSourceHlc =
        new();

    /// <summary>
    /// Per-tree semaphore serialising the
    /// <see cref="ReportBlockedFloorAsync(string, IReplicationTxBufferGrain, CancellationToken)"/>
    /// helper's grain-call + registry-call pair. Concurrent applier
    /// invocations (e.g. multiple peer ship loops pushing into the
    /// same receiver tree) would otherwise interleave the
    /// <c>GetLowestStagedHlc</c> read against the
    /// <c>ReportCursorAsync</c> write and a stale snapshot from a
    /// late-arriving thread could clobber a fresher snapshot from an
    /// earlier-resolving thread (replace semantics: most-recent
    /// caller wins, but "most recent" was wall-clock arrival, not
    /// most-recent observation of buffer state). Holding the
    /// semaphore across both calls collapses the TOCTOU window so
    /// the registry's pin always reflects the latest observation in
    /// linearisable order.
    /// </summary>
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _floorReportLocks =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Last value (and observed-vs-never) the applier has published
    /// to the cursor registry for each tree's blocked-floor pin.
    /// Used to suppress redundant reports when a steady-state batch
    /// of identical-HLC admissions does not move the floor; also used
    /// by the on-drain unregister path to detect the
    /// "transition to null after a non-null" case.
    /// </summary>
    private readonly ConcurrentDictionary<string, BlockedFloorReport> _lastReportedFloor =
        new(StringComparer.Ordinal);

    private readonly record struct BlockedFloorReport(bool Reported, HybridLogicalClock? Value);

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default)
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
                throw new ArgumentException("ReplogEntry.TreeId must be non-empty.", nameof(entry));
            }

            if (string.IsNullOrEmpty(entry.OriginClusterId))
            {
                throw new ArgumentException(
                    "ReplogEntry.OriginClusterId must be non-empty for replication apply.",
                    nameof(entry));
            }

            var resolved = options.Get(entry.TreeId);
            if (string.Equals(entry.OriginClusterId, resolved.ClusterId, StringComparison.Ordinal))
            {
                // Defence-in-depth: a local-origin entry must never be applied
                // back onto its authoring cluster. The outbound ship loop's
                // origin filter already prevents this in the steady state, but
                // hand-built apply pipelines and tests can still hand us such
                // an entry — surface it as an explicit no-op rather than
                // silently merging into the same cluster's state.
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            // Range deletes carry HybridLogicalClock.Zero by design (the walk
            // produces many per-leaf HLCs that cannot be faithfully collapsed),
            // so per-origin HWM dedupe does not apply to them. Range applies
            // are naturally idempotent at the leaf layer.
            if (entry.Op == ReplogOp.DeleteRange)
            {
                // Defence-in-depth: a DeleteRange entry tagged with
                // atomic-batch metadata (AtomicBatchSize > 0) is a
                // producer-contract violation — the producer's
                // SetManyAtomicAsync surface emits only Set/Delete, never
                // DeleteRange, so atomic-batch stamps on a range op are
                // intrinsic ambiguity (no consistent saga key, no
                // single-HLC commit point). Surface it as an explicit
                // ArgumentException so the producer fails fast rather
                // than the receiver silently applying a non-atomic range
                // delete that carries an unfulfilled atomic-batch
                // promise. The check is independent of the receiver-side
                // AtomicBatchDelivery opt-in because the violation is
                // producer-shaped, not receiver-shaped.
                if (entry.AtomicBatchSize > 0)
                {
                    throw new ArgumentException(
                        "ReplogEntry.Op=DeleteRange must not carry atomic-batch metadata "
                        + "(AtomicBatchSize > 0). Atomic batches must contain only Set / Delete entries; "
                        + "range deletes are emitted by the producer's DeleteRangeAsync surface, not "
                        + "SetManyAtomicAsync.",
                        nameof(entry));
                }

                await ApplyRangeAsync(entry, cancellationToken);
                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero };
            }

            var hwmGrain = GetHwmGrain(entry.TreeId);
            var hwm = await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);
            if (entry.Timestamp <= hwm)
            {
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = hwm };
            }

            // Atomic-batch staging buffer: when the receiver
            // has opted in via LatticeReplicationOptions.AtomicBatchDelivery
            // and the entry carries a non-zero AtomicBatchSize (it is
            // part of an enclosing SetManyAtomicAsync transaction), hand
            // the entry off to the per-tree buffer grain. While the
            // batch is incomplete, return Applied=false with the
            // per-origin HWM unchanged so the producer continues to
            // re-ship until every sibling lands and the buffer dedupes
            // wire-shape re-deliveries by (origin, transactionId, index).
            // When the admission completes the enclosing batch, dispatch
            // the whole batch atomically through the source-HLC-preserving
            // IReplicationApplyGrain.ApplyManyAtomicAsync seam: every key
            // commits or rolls back together, the per-origin HWM advances
            // exactly once to the maximum HLC across the batch, and a
            // failed saga (Compensated outcome or thrown exception) routes
            // every entry to the per-tree dead-letter queue tagged
            // ReasonAtomicApplyFailure with the HWM left unchanged so the
            // producer continues to re-ship until the DLQ is recovered.
            //
            // The gate sits after HWM dedup (so a re-delivery of an
            // already-applied batch is not redundantly buffered) and
            // before the shadow-forward dedupe cache (the buffer's
            // (origin, txid, index) dedup is a stronger contract than
            // the cache's (origin, hlc, key, op) tuple).
            if (resolved.AtomicBatchDelivery && entry.AtomicBatchSize > 0)
            {
                if (entry.TransactionId == Guid.Empty)
                {
                    throw new ArgumentException(
                        "ReplogEntry.TransactionId must be non-empty when AtomicBatchSize > 0 and "
                        + "AtomicBatchDelivery is enabled. Producer must stamp a transaction id on every "
                        + "entry of an atomic batch.",
                        nameof(entry));
                }

                var txBuffer = grainFactory.GetGrain<IReplicationTxBufferGrain>(entry.TreeId);
                var admission = await txBuffer.AdmitAsync(entry, cancellationToken).ConfigureAwait(false);

                // Publish the buffer's lowest staged HLC to the
                // cursor registry so the producer-side WAL GC AND-s a
                // strict-less blocked-floor clause into its trim
                // predicate. Reported after every admit (admit may
                // have grown the buffer with a fresh batch) and after
                // every batch-completion path (admission below
                // returned BatchComplete=true; the staged entries for
                // that transaction have been removed and the floor
                // may have advanced or cleared). Failure is swallowed
                // with no observable apply-path impact - the WAL still
                // holds the entry and a subsequent admit / removal
                // re-publishes the floor.
                await ReportBlockedFloorAsync(entry.TreeId, txBuffer, cancellationToken).ConfigureAwait(false);

                if (!admission.BatchComplete)
                {
                    outcome = LatticeReplicationMetrics.OutcomeAtomicBuffered;
                    return new ApplyResult { Applied = false, HighWaterMark = hwm };
                }

                var batchResult = await ApplyCompletedAtomicBatchAsync(
                    entry,
                    admission.CompletedBatch,
                    hwmGrain,
                    hwm,
                    resolved,
                    cancellationToken).ConfigureAwait(false);

                // Re-report after the saga returns: a successful
                // commit does not mutate the buffer further (the
                // siblings were removed atomically inside the
                // BatchComplete=true branch above), but a thrown saga
                // path may have routed entries to the DLQ without
                // changing the in-memory floor. Re-publishing here is
                // a defensive sweep so the floor reflects post-saga
                // buffer state; redundant when the saga did not
                // mutate the buffer (no-op cost is one cheap grain
                // call).
                await ReportBlockedFloorAsync(entry.TreeId, txBuffer, cancellationToken).ConfigureAwait(false);

                outcome = batchResult.Applied
                    ? LatticeReplicationMetrics.OutcomeSuccess
                    : LatticeReplicationMetrics.OutcomeFailure;
                return batchResult;
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
            // the failure counter on what looks like a filtered call —
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
                // when the entry carries no declared dependencies — legacy
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
                RecordFifoState(entry);

                // Advance the HWM only after the apply commits.
                var advanced = await hwmGrain.TryAdvanceAsync(entry.OriginClusterId!, entry.Timestamp, cancellationToken);
                var newHwm = advanced
                    ? entry.Timestamp
                    : await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);

                // The advance may have unblocked entries parked by an earlier
                // delivery whose deps included this origin's diagonal. Drain
                // FIFO until the buffer reaches a fixed point — each drained
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
            RecordApplyDuration(entry.TreeId, startTimestamp, outcome);
        }
    }

    /// <summary>
    /// Records a single sample on the <see cref="LatticeReplicationMetrics.ApplyDuration"/>
    /// histogram. Skipped when <paramref name="treeId"/> is empty so a
    /// validation throw on the tree-id guard does not publish a histogram
    /// sample with an empty <c>tree</c> tag (which would be unusable for
    /// per-tree alerting). The duration is read via
    /// <see cref="Stopwatch.GetElapsedTime(long)"/>, which is allocation-free.
    /// </summary>
    private static void RecordApplyDuration(string treeId, long startTimestamp, string outcome)
    {
        if (string.IsNullOrEmpty(treeId))
        {
            return;
        }

        var elapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;
        LatticeReplicationMetrics.ApplyDuration.Record(
            elapsedMs,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome));
    }

    private static bool HasCausalDependencies(ReplogEntry entry) =>
        entry.VectorClock is { Entries.Count: > 0 };

    /// <summary>
    /// Consumer id under which the receiver-side applier
    /// registers its atomic-batch staging buffer pin in the
    /// <see cref="ILatticeReplicationCursorRegistry"/>. The applier
    /// is a blocked-floor-only consumer (cursor=Zero) so its
    /// registration does not pollute the GC's HLC <c>min(cursor)</c>
    /// branch — the per-origin HWM grain and the leaf cursor reporter
    /// already cover that side. Per-tree scoping is achieved by the
    /// registry's <c>(treeName, consumerId)</c> key shape so the same
    /// constant is safely reused across every replicated tree.
    /// </summary>
    private const string AtomicBatchApplierConsumerId = "applier:atomic-batch";

    /// <summary>
    /// Reads the per-tree atomic-batch staging buffer's
    /// lowest staged HLC and publishes it to the
    /// <see cref="ILatticeReplicationCursorRegistry"/> as the
    /// applier's blocked-floor pin. Called after every admit and
    /// every batch-completion event so the producer-side WAL GC sees
    /// the current floor across silos. No-op when the registry seam
    /// was not supplied (test-only constructor path); production DI
    /// always injects the registered singleton.
    /// </summary>
    /// <remarks>
    /// Failures are intentionally swallowed: the WAL retains the
    /// authoritative copy of every staged entry and a subsequent
    /// admit / removal call against the buffer re-publishes the
    /// floor. Surfacing a registry exception out of the apply hot
    /// path would convert a diagnostic-side outage into an apply-
    /// path failure, which is the wrong trade-off (the GC is the
    /// only consumer of the registry's blocked-floor and a stale
    /// frontier merely defers a trim, never breaks correctness).
    /// Swallowed exceptions are logged at <see cref="LogLevel.Warning"/>
    /// so an operator dashboard can still surface a sustained
    /// registry outage.
    /// </remarks>
    private async Task ReportBlockedFloorAsync(
        string treeName,
        IReplicationTxBufferGrain txBuffer,
        CancellationToken cancellationToken)
    {
        if (cursorRegistry is null)
        {
            return;
        }

        var lockTaken = false;
        var semaphore = _floorReportLocks.GetOrAdd(treeName, static _ => new SemaphoreSlim(1, 1));
        try
        {
            // Serialise across both the GetLowestStagedHlc read + ReportCursorAsync
            // write so a stale snapshot from a late-arriving thread does not clobber
            // a fresher snapshot from an earlier-resolving thread (replace semantics:
            // most-recent caller wins, but "most recent" was wall-clock arrival, not
            // most-recent observation of buffer state).
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            lockTaken = true;

            var floor = await txBuffer.GetLowestStagedHlcAsync(cancellationToken).ConfigureAwait(false);

            // Suppress duplicate reports: the steady-state hot path
            // through the helper observes an unchanged floor on every
            // call (admit raises buffer count but does not always
            // change the minimum HLC; the post-saga defensive sweep
            // re-publishes the same value). The cache eliminates the
            // redundant grain hops without affecting correctness.
            var previous = _lastReportedFloor.TryGetValue(treeName, out var existing)
                ? existing
                : default;
            if (previous.Reported && Nullable.Equals(previous.Value, floor))
            {
                return;
            }

            // Drain transition: when the buffer has fully drained
            // (floor == null) AND we previously had a non-null pin
            // registered, unregister the consumer entirely instead
            // of holding a (cursor=Zero, blockedAtHlc=null) row that
            // contributes nothing to either GC predicate branch but
            // still surfaces in SnapshotAsync output. A subsequent
            // non-null admit re-registers cleanly.
            if (floor is null && previous.Reported && previous.Value is not null)
            {
                await cursorRegistry.UnregisterAsync(
                    treeName,
                    AtomicBatchApplierConsumerId,
                    cancellationToken).ConfigureAwait(false);
                _lastReportedFloor[treeName] = new BlockedFloorReport(true, null);
                return;
            }

            await cursorRegistry.ReportCursorAsync(
                treeName,
                AtomicBatchApplierConsumerId,
                HybridLogicalClock.Zero,
                floor,
                cancellationToken).ConfigureAwait(false);
            _lastReportedFloor[treeName] = new BlockedFloorReport(true, floor);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Swallow: see <remarks/> on this method. The WAL retains
            // the staged entry and a subsequent admit / removal call
            // re-publishes the floor. Logged at Warning so a
            // sustained outage is visible on the dashboard.
            _logger.LogWarning(ex,
                "Replication blocked-floor registry report failed for tree {Tree}; "
                + "the WAL retains the staged entry and a subsequent admit / removal "
                + "will re-publish the floor.",
                treeName);
        }
        finally
        {
            if (lockTaken)
            {
                semaphore.Release();
            }
        }
    }

    private async Task ParkAsync(
        ReplogEntry entry,
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
                        // unblock further entries — the producer cache
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
                    // so HWM dedupe will not suppress the retry — the
                    // cache rollback is the only step required.
                    if (_dedupeCaches.TryGetValue(ent.TreeId, out var cache))
                    {
                        cache.Remove(ent);
                    }
                }
            }
        }
    }

    private Task ApplyPointAsync(ReplogEntry entry)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        return entry.Op switch
        {
            ReplogOp.Set when entry.Value is null
                => throw new ArgumentException(
                    "ReplogEntry.Value must be non-null for ReplogOp.Set.",
                    nameof(entry)),
            ReplogOp.Set => entry.Mode switch
            {
                ReplicationMode.LwwRegister => apply.ApplySetAsync(
                    entry.Key,
                    entry.Value!,
                    entry.Timestamp,
                    entry.OriginClusterId!,
                    sourceVectorClock: null,
                    entry.ExpiresAtTicks),
                ReplicationMode.OrSet => ApplyStateMergeAsync<OrSet>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new OrSet()),
                ReplicationMode.PnCounter => ApplyStateMergeAsync<PnCounter>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new PnCounter()),
                ReplicationMode.VersionVector => ApplyStateMergeAsync<VersionVector>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new VersionVector()),
                _ => throw new InvalidOperationException(
                    $"ReplogEntry on tree '{entry.TreeId}' carries unrecognised replication mode '{entry.Mode}' "
                    + "(value="
                    + ((int)entry.Mode).ToString(System.Globalization.CultureInfo.InvariantCulture)
                    + "). The receiver has no apply rule registered for this mode; in a future release such "
                    + "entries will be routed to a dead-letter queue."),
            },
            ReplogOp.Delete => apply.ApplyDeleteAsync(
                entry.Key,
                entry.Timestamp,
                entry.OriginClusterId!,
                sourceVectorClock: null),
            _ => throw new InvalidOperationException(
                $"Unsupported point-apply op {entry.Op} for entry on tree '{entry.TreeId}'."),
        };
    }

    /// <summary>
    /// CAS retry budget for the read-merge-write loop used by typed CRDT
    /// state-merge applies (<see cref="ReplicationMode.OrSet"/>,
    /// <see cref="ReplicationMode.PnCounter"/>, <see cref="ReplicationMode.VersionVector"/>).
    /// Mirrors the budget the typed accessors (<see cref="OrSetAccessor.DefaultMaxAttempts"/>,
    /// <see cref="PnCounterAccessor.DefaultMaxAttempts"/>,
    /// <see cref="VersionVectorAccessor.DefaultMaxAttempts"/>) use for the
    /// authoring side, so a typical fan-in matches.
    /// </summary>
    private const int StateMergeMaxAttempts = 16;

    private async Task ApplyStateMergeAsync<TState>(
        ReplogEntry entry,
        Action<TState, TState> merge,
        Func<TState> emptyFactory)
        where TState : class
    {
        if (entry.Value is null)
        {
            throw new ArgumentException(
                $"ReplogEntry.Value must be non-null for {entry.Mode} state-merge apply.",
                nameof(entry));
        }

        var lattice = grainFactory.GetGrain<ILattice>(entry.TreeId);
        var serializer = JsonLatticeSerializer<TState>.Default;
        var incoming = serializer.Deserialize(entry.Value);

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
                : serializer.Deserialize(versioned.Value);
            merge(existing, incoming);
            var bytes = serializer.Serialize(existing);
            var ok = await lattice.SetIfVersionAsync(entry.Key, bytes, versioned.Version);
            if (ok)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"Replication state-merge CAS budget exhausted after {StateMergeMaxAttempts} attempts on tree "
            + $"'{entry.TreeId}', key '{entry.Key}', mode '{entry.Mode}'. The receiver could not install the "
            + "merged state under optimistic concurrency; reduce contention on this key or increase the "
            + "budget in a future configuration knob.");
    }

    private Task ApplyRangeAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (entry.EndExclusiveKey is null)
        {
            throw new ArgumentException(
                "ReplogEntry.EndExclusiveKey must be non-null for ReplogOp.DeleteRange.",
                nameof(entry));
        }

        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        return apply.ApplyDeleteRangeAsync(entry.Key, entry.EndExclusiveKey, entry.OriginClusterId!, sourceVectorClock: null);
    }

    private IReplicationHighWaterMarkGrain GetHwmGrain(string treeId) =>
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeId);

    /// <summary>
    /// Dispatches a completed atomic batch surfaced from the per-tree
    /// <see cref="IReplicationTxBufferGrain"/> through the source-HLC-preserving
    /// <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/> seam,
    /// advances the per-origin high-water-mark exactly once to the
    /// maximum HLC across the batch on
    /// <see cref="AtomicApplyOutcome.Committed"/>, and routes every
    /// entry to the per-tree dead-letter queue tagged
    /// <see cref="LatticeReplicationMetrics.ReasonAtomicApplyFailure"/>
    /// on <see cref="AtomicApplyOutcome.Compensated"/> or a thrown
    /// non-cancellation exception. The HWM advance is critical for the
    /// atomic-visibility contract: a concurrent reader of the per-origin
    /// HWM never observes an intermediate value where some-but-not-all
    /// keys in the batch have been applied. On failure the HWM is left
    /// unchanged so the producer continues to re-ship until the DLQ is
    /// recovered or discarded.
    /// </summary>
    private async Task<ApplyResult> ApplyCompletedAtomicBatchAsync(
        ReplogEntry trigger,
        IReadOnlyList<TxStagedEntry> completedBatch,
        IReplicationHighWaterMarkGrain hwmGrain,
        HybridLogicalClock currentHwm,
        LatticeReplicationOptions resolved,
        CancellationToken cancellationToken)
    {
        var (committed, maxHlc) = await RunAtomicSagaAsync(
            trigger, completedBatch, currentHwm, cancellationToken).ConfigureAwait(false);

        if (!committed)
        {
            return new ApplyResult { Applied = false, HighWaterMark = currentHwm };
        }

        // Committed: advance the per-origin HWM exactly once to the
        // maximum HLC across the batch. Doing this in a single grain
        // call (instead of per-entry) is the load-bearing invariant for
        // cross-cluster atomic visibility: a concurrent reader of the
        // per-origin HWM never sees an intermediate value where some
        // but not all keys are visible.
        var advanced = await hwmGrain
            .TryAdvanceAsync(trigger.OriginClusterId!, maxHlc, cancellationToken)
            .ConfigureAwait(false);
        var newHwm = advanced
            ? maxHlc
            : await hwmGrain.GetAsync(trigger.OriginClusterId!, cancellationToken).ConfigureAwait(false);

        if (advanced)
        {
            // Mirror the foreign advance into the producer-side local
            // vector clock cache so a subsequent local emit observes
            // the just-applied foreign frontier. Mirrors the per-entry
            // success path's AdvanceForeign call site.
            localVectorClockCache.AdvanceForeign(
                trigger.TreeId,
                trigger.OriginClusterId!,
                maxHlc);
            await DrainBufferAsync(trigger.TreeId, hwmGrain, resolved, cancellationToken)
                .ConfigureAwait(false);
        }

        return new ApplyResult { Applied = true, HighWaterMark = newHwm };
    }

    /// <summary>
    /// Dispatches the atomic batch through the source-HLC-preserving
    /// saga seam and routes failures to the per-tree dead-letter
    /// queue. Returns the saga outcome alongside the maximum HLC across
    /// the batch on success — callers integrate the HLC into their own
    /// high-water-mark advance step (the per-entry path advances
    /// directly via the HWM grain; the batch path defers the advance
    /// to the end-of-run flush). On a non-committed outcome the
    /// returned HLC is the supplied <paramref name="baselineHlc"/>
    /// because no advance is warranted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The saga-wide <see cref="VersionVector"/> passed to the apply
    /// seam is read from the first staged entry. Per the producer-side
    /// atomic-batch capture contract, every entry in an atomic batch
    /// shares the same saga-wide vector-clock frontier (single capture
    /// at the start of the saga's first <c>Prepare</c>, stamped on every
    /// per-key emit), so every staged entry carries an identical
    /// <see cref="ReplogEntry.VectorClock"/> and reading from index 0
    /// is canonical.
    /// </para>
    /// <para>
    /// The <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/>
    /// seam is itself idempotent on
    /// <see cref="ReplogEntry.TransactionId"/> — the underlying
    /// <c>AtomicWriteGrain</c> activation is keyed
    /// <c>(treeId, transactionId)</c> and replays its persisted saga
    /// state on re-entry — so a producer that re-ships the batch after
    /// a transient receiver failure observes the same terminal outcome
    /// on the second attempt.
    /// </para>
    /// </remarks>
    private async Task<(bool Committed, HybridLogicalClock MaxHlc)> RunAtomicSagaAsync(
        ReplogEntry trigger,
        IReadOnlyList<TxStagedEntry> completedBatch,
        HybridLogicalClock baselineHlc,
        CancellationToken cancellationToken)
    {
        // Defence-in-depth: the buffer-grain contract guarantees a
        // non-empty CompletedBatch on BatchComplete=true. A zero-entry
        // admission would index [0] below for the saga-wide VC capture
        // and trip an opaque IndexOutOfRangeException; surface a typed
        // contract violation instead so an operator inspecting the
        // failure sees the buffer-grain contract that was broken.
        if (completedBatch.Count == 0)
        {
            throw new InvalidOperationException(
                $"Atomic batch on tree '{trigger.TreeId}' admitted with zero staged entries — "
                + "buffer-grain contract violation (BatchComplete=true requires a non-empty CompletedBatch).");
        }

        // Map TxStagedEntry → AtomicApplyEntry. ReplogOp.Set becomes a
        // non-tombstone item with its committed value bytes and
        // ExpiresAtTicks; ReplogOp.Delete becomes a tombstone item with
        // a null Value and ExpiresAtTicks=0 (the contract on
        // AtomicApplyEntry forbids non-zero expiry on tombstones).
        // Range-delete entries are not part of an atomic batch by
        // construction (the producer's SetManyAtomicAsync surface only
        // emits Set/Delete) so they do not appear here; if a malformed
        // entry slips through we surface it as the same
        // InvalidOperationException the canonical apply path raises.
        var applyEntries = new AtomicApplyEntry[completedBatch.Count];
        var maxHlc = baselineHlc;
        for (var i = 0; i < completedBatch.Count; i++)
        {
            var staged = completedBatch[i].Entry;
            applyEntries[i] = MapStagedToAtomicApplyEntry(staged, trigger.TreeId);

            if (staged.Timestamp.CompareTo(maxHlc) > 0)
            {
                maxHlc = staged.Timestamp;
            }
        }

        // Saga-wide VC: per the producer-side capture contract every
        // entry in the batch shares the same frontier, so reading from
        // index 0 is canonical.
        var sagaVc = completedBatch[0].Entry.VectorClock;

        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(trigger.TreeId);
        AtomicApplyResult sagaResult;
        try
        {
            sagaResult = await apply.ApplyManyAtomicAsync(
                applyEntries,
                trigger.TransactionId,
                trigger.OriginClusterId!,
                sagaVc,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation is not a saga failure — propagate to the
            // caller so the cooperative cancellation contract is
            // preserved (no DLQ park, no HWM advance, the producer's
            // next pump cycle redelivers the trigger entry).
            throw;
        }
        catch (Exception ex)
        {
            await RouteAtomicBatchToDlqAsync(
                trigger.TreeId,
                completedBatch,
                ex.Message ?? "Atomic apply saga threw with no message.",
                cancellationToken).ConfigureAwait(false);
            return (false, baselineHlc);
        }

        if (sagaResult.Outcome == AtomicApplyOutcome.Compensated)
        {
            await RouteAtomicBatchToDlqAsync(
                trigger.TreeId,
                completedBatch,
                sagaResult.FailureReason ?? "Atomic apply saga compensated.",
                cancellationToken).ConfigureAwait(false);
            return (false, baselineHlc);
        }

        // Defence-in-depth: a Committed outcome must have applied
        // every entry in the batch — the saga is all-or-nothing by
        // construction. A mismatch indicates a saga-contract violation
        // (e.g. a future RunSagaAsync refactor that silently drops a
        // per-key write); surface it as a typed exception so the
        // per-origin HWM is not advanced past entries that never
        // landed and the producer redelivers on the next pump cycle.
        if (sagaResult.AppliedCount != completedBatch.Count)
        {
            throw new InvalidOperationException(
                $"Atomic apply saga on tree '{trigger.TreeId}' returned Committed with "
                + $"AppliedCount={sagaResult.AppliedCount} but BatchSize={completedBatch.Count} — "
                + "saga contract violation (Committed implies every entry applied).");
        }

        return (true, maxHlc);
    }

    private static AtomicApplyEntry MapStagedToAtomicApplyEntry(ReplogEntry staged, string treeId) =>
        staged.Op switch
        {
            ReplogOp.Set when staged.Value is null
                => throw new ArgumentException(
                    "Staged ReplogEntry.Value must be non-null for ReplogOp.Set in an atomic batch.",
                    nameof(staged)),
            ReplogOp.Set => new AtomicApplyEntry
            {
                Key = staged.Key,
                Value = staged.Value,
                Timestamp = staged.Timestamp,
                ExpiresAtTicks = staged.ExpiresAtTicks,
                VectorClock = staged.VectorClock,
                IsTombstone = false,
            },
            ReplogOp.Delete => new AtomicApplyEntry
            {
                Key = staged.Key,
                Value = null,
                Timestamp = staged.Timestamp,
                ExpiresAtTicks = 0,
                VectorClock = staged.VectorClock,
                IsTombstone = true,
            },
            _ => throw new InvalidOperationException(
                $"Atomic batch on tree '{treeId}' carries unsupported op '{staged.Op}' "
                + "for key '" + staged.Key + "'. Atomic batches must contain only Set / Delete entries."),
        };

    /// <summary>
    /// Routes every entry in a failed atomic batch to the per-tree
    /// dead-letter queue tagged
    /// <see cref="LatticeReplicationMetrics.ReasonAtomicApplyFailure"/>.
    /// Every entry in the batch is parked under the same reason and
    /// transaction id so an operator inspecting the DLQ sees the whole
    /// batch as a unit. DLQ enqueue failures are swallowed because the
    /// per-origin high-water-mark was not advanced for this batch — the
    /// producer continues to re-ship until the DLQ is recovered, so a
    /// transient DLQ failure does not block apply progress on a
    /// deterministically-failing DLQ.
    /// </summary>
    private async Task RouteAtomicBatchToDlqAsync(
        string treeId,
        IReadOnlyList<TxStagedEntry> completedBatch,
        string failureReason,
        CancellationToken cancellationToken)
    {
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(treeId);
        for (var i = 0; i < completedBatch.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await dlq.EnqueueAsync(
                    completedBatch[i].Entry,
                    failureReason,
                    retryCount: 0,
                    reasonTag: LatticeReplicationMetrics.ReasonAtomicApplyFailure,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // Best-effort routing — see method summary.
            }
        }
    }

    /// <summary>
    /// Records the receiver-side replication-lag sample for a successfully
    /// applied point operation. Lag is computed as
    /// <c>now - entry.Timestamp.WallClockTicks</c> in milliseconds and
    /// clamped at zero — a future-dated source HLC (the producing cluster's
    /// wall clock leads the receiver's) reports as <c>0</c> rather than a
    /// negative sample, which would corrupt downstream histograms.
    /// </summary>
    private static void RecordApplyLag(ReplogEntry entry)
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
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId));
    }

    /// <summary>
    /// Updates the per-<c>(treeId, originClusterId)</c> last-applied
    /// source-HLC tracker for a successfully applied point operation
    /// and increments
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/> when
    /// the entry's HLC is strictly less than the previously recorded
    /// value — surfacing a transport-side regression that broke the
    /// per-origin FIFO invariant. The recorded value is the pointwise
    /// max so a benign re-delivery (which the HWM check already filters
    /// upstream) does not silently downgrade the tracker on the rare
    /// path where it slipped through.
    /// </summary>
    private void RecordFifoState(ReplogEntry entry)
    {
        // Skip range deletes (carry HLC.Zero) and entries with a missing
        // origin (defensive — the caller-side guard rejects empty origins
        // before we get here).
        if (entry.Op == ReplogOp.DeleteRange || string.IsNullOrEmpty(entry.OriginClusterId))
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
