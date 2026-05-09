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
/// the causal-plus dependency check — parking entries whose
/// declared <see cref="WalRecord.VectorClock"/> is not yet
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
            if (entry.Op == MutationKind.DeleteRange)
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
                // promise. The check is unconditional (the
                // receiver-side AtomicBatchDelivery opt-in that this
                // check used to gate against was retired by the WAL
                // repivot) because the violation is producer-shaped,
                // not receiver-shaped — the wire slot remains additive
                // on every WalRecord so a future receiver-side
                // primitive can re-consume it.
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

            var hwmGrain = GetHwmGrain(entry.TreeId);
            var hwm = await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);
            if (entry.Timestamp <= hwm)
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
            // the failure counter on what looks like a filtered call —,
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

    private Task ApplyPointAsync(WalRecord entry)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        return entry.Op switch
        {
            MutationKind.Set when entry.Value is null
                => throw new ArgumentException(
                    "WalRecord.Value must be non-null for MutationKind.Set.",
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
                LatticeMergeMode.OrSet => ApplyStateMergeAsync<OrSet>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new OrSet()),
                LatticeMergeMode.PnCounter => ApplyStateMergeAsync<PnCounter>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new PnCounter()),
                LatticeMergeMode.VersionVector => ApplyStateMergeAsync<VersionVector>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new VersionVector()),
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
    /// CAS retry budget for the read-merge-write loop used by typed CRDT
    /// state-merge applies (<see cref="LatticeMergeMode.OrSet"/>,
    /// <see cref="LatticeMergeMode.PnCounter"/>, <see cref="LatticeMergeMode.VersionVector"/>).
    /// Mirrors the budget the typed accessors (<see cref="OrSetAccessor.DefaultMaxAttempts"/>,
    /// <see cref="PnCounterAccessor.DefaultMaxAttempts"/>,
    /// <see cref="VersionVectorAccessor.DefaultMaxAttempts"/>) use for the
    /// authoring side, so a typical fan-in matches.
    /// </summary>
    private const int StateMergeMaxAttempts = 16;

    private async Task ApplyStateMergeAsync<TState>(
        WalRecord entry,
        Action<TState, TState> merge,
        Func<TState> emptyFactory)
        where TState : class
    {
        if (entry.Value is null)
        {
            throw new ArgumentException(
                $"WalRecord.Value must be non-null for {entry.Mode} state-merge apply.",
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
        return apply.ApplyDeleteRangeAsync(entry.Key, entry.EndExclusiveKey, entry.OriginClusterId!, sourceVectorClock: null);
    }

    private IReplicationHighWaterMarkGrain GetHwmGrain(string treeId) =>
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeId);

    /// <summary>
    /// Records the receiver-side replication-lag sample for a successfully
    /// applied point operation. Lag is computed as
    /// <c>now - entry.Timestamp.WallClockTicks</c> in milliseconds and
    /// clamped at zero — a future-dated source HLC (the producing cluster's
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
    private void RecordFifoState(WalRecord entry)
    {
        // Skip range deletes (carry HLC.Zero) and entries with a missing
        // origin (defensive — the caller-side guard rejects empty origins
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
