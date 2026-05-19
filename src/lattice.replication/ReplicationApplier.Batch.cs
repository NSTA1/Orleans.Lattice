using Orleans.Lattice.BPlusTree.Grains;
using System.Diagnostics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Optimised batch-apply path for <see cref="ReplicationApplier"/>.
/// Groups the inbound batch into contiguous same-<c>(treeId, originClusterId)</c>
/// runs and collapses the per-entry per-origin high-water-mark
/// round-trips to a single
/// <see cref="IReplicationHighWaterMarkGrain.GetAsync"/> at the start
/// of each run plus a single
/// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/> at the
/// end. The causal-apply buffer is drained once at the end of each
/// run that advanced the persisted HWM rather than after every
/// successful apply, and the local vector clock is fetched at most
/// once per run on demand (only when the first causal-dep entry is
/// seen) and re-fetched only when an apply has happened since.
/// </summary>
internal sealed partial class ReplicationApplier
{
    /// <inheritdoc />
    public async Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        if (entries.Count == 0)
        {
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        // Single-entry: defer to the per-entry path so behaviour is
        // bit-identical with the legacy receiver. The per-entry path
        // already covers every classification (range delete, local-origin
        // defence, dedup, causal-park, success).
        if (entries.Count == 1)
        {
            return await ApplyAsync(entries[0], cancellationToken).ConfigureAwait(false);
        }

        // Walk contiguous same-(treeId, origin) runs. The receiver
        // protocol guarantees the inbound batch is shipped from a
        // single producer in WAL order so a 256-entry inbound batch
        // from one origin collapses to a single run.
        var anyApplied = false;
        var highest = HybridLogicalClock.Zero;
        var i = 0;
        while (i < entries.Count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var startTreeId = entries[i].TreeId;
            var startOrigin = entries[i].OriginClusterId;
            var j = i + 1;
            while (j < entries.Count
                && string.Equals(entries[j].TreeId, startTreeId, StringComparison.Ordinal)
                && string.Equals(entries[j].OriginClusterId, startOrigin, StringComparison.Ordinal))
            {
                j++;
            }

            var runResult = await ApplyOriginRunAsync(entries, i, j, cancellationToken).ConfigureAwait(false);
            if (runResult.Applied)
            {
                anyApplied = true;
            }
            if (runResult.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = runResult.HighWaterMark;
            }
            i = j;
        }

        return new ApplyResult { Applied = anyApplied, HighWaterMark = highest };
    }

    /// <summary>
    /// Applies a contiguous run of entries that share the same
    /// <c>(treeId, originClusterId)</c> tuple. The run is identified
    /// by half-open indices <paramref name="startInclusive"/> and
    /// <paramref name="endExclusive"/>.
    /// </summary>
    /// <remarks>
    /// <para>The per-entry classification is preserved exactly:</para>
    /// <list type="bullet">
    ///   <item><description>Range-delete entries bypass HWM dedup and
    ///   apply unconditionally (they carry <see cref="HybridLogicalClock.Zero"/>
    ///   by design).</description></item>
    ///   <item><description>The first entry's <see cref="WalRecord.Timestamp"/>
    ///   is checked against the persisted HWM (single
    ///   <see cref="IReplicationHighWaterMarkGrain.GetAsync"/>);
    ///   subsequent entries are checked against an in-memory
    ///   <c>runningHwm</c> that tracks the highest applied HLC in
    ///   this run, saving N-1 redundant HWM round-trips.</description></item>
    ///   <item><description>The local vector clock is fetched on
    ///   demand the first time a causal-dep entry is seen, then
    ///   reused until an apply mutates it (a "dirty" flag re-fetches
    ///   on next causal-dep check).</description></item>
    ///   <item><description>The HWM advance is deferred to the end of
    ///   the run (single <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/>),
    ///   and the causal-apply buffer is drained once per advanced
    ///   run (single <c>DrainBufferAsync</c>).</description></item>
    /// </list>
    /// <para>Per-entry instrumentation
    /// (<see cref="LatticeReplicationMetrics.ApplyDuration"/>,
    /// <see cref="LatticeReplicationMetrics.ApplyLag"/>,
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>) is
    /// recorded inside the loop so per-entry observability is
    /// preserved.</para>
    /// </remarks>
    private async Task<ApplyResult> ApplyOriginRunAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        CancellationToken cancellationToken)
    {
        var first = entries[startInclusive];
        var treeId = first.TreeId;
        var origin = first.OriginClusterId;

        // Defensive: an empty tree-id or empty origin must surface as
        // the same ArgumentException the per-entry path raises. Falling
        // back to per-entry preserves the exact validation message and
        // keeps the local-origin defence consistent.
        if (string.IsNullOrEmpty(treeId) || string.IsNullOrEmpty(origin))
        {
            return await ApplyRunPerEntryAsync(entries, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
        }

        var resolved = options.Get(treeId);
        if (string.Equals(origin, resolved.ClusterId, StringComparison.Ordinal))
        {
            // Local-origin defence: the per-entry path classifies each
            // entry as Dedup with HighWaterMark=Zero. Replay the same
            // classification (and per-entry duration sample) here.
            for (var k = startInclusive; k < endExclusive; k++)
            {
                var startTs = Stopwatch.GetTimestamp();
                RecordApplyDuration(treeId, origin!, startTs, LatticeReplicationMetrics.OutcomeDedup);
            }
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        var hwmGrain = GetHwmGrain(treeId);
        var hwm = await hwmGrain.GetAsync(origin!, cancellationToken).ConfigureAwait(false);

        // Bootstrap-drain mode: receiver-side bootstrap replay opens a
        // <see cref="LatticeBootstrapApplyContext"/> scope around the
        // entire drain. While that scope is active the per-origin HWM
        // gate must be suppressed and the end-of-run HWM advance must
        // be skipped, mirroring the per-entry path's bypass at
        // <see cref="ApplyAsync"/>. The snapshot exporter visits
        // shards / leaves in arbitrary order, so applying steady-state
        // HWM dedup during bootstrap replay can drop a still-pending
        // saga key with a strictly-earlier source HLC and break
        // per-saga all-or-nothing visibility on the bootstrapped peer.
        // The post-drain
        // <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
        // installs the HWM at the snapshot's AsOfHlc atomically. The
        // current bootstrap coordinator routes through the per-entry
        // path, so this branch is defence-in-depth for any future
        // drainer that batches; the matching test fixture covers it
        // verbatim.
        var bootstrapMode = LatticeBootstrapApplyContext.IsActive;

        // Per-tree shadow-forward dedupe cache (see ApplyAsync for the
        // race scenario it closes). The cache instance is fetched once
        // per run; per-entry TryAdd is performed after the runningHwm
        // dedupe so HWM-deduped entries do not pollute the cache (which
        // would break operator-driven re-pin recovery, where lowering
        // the per-origin frontier must re-admit previously-deduped
        // identity tuples). On apply failure the reservation is rolled
        // back via Remove so the transport's retry path is not silently
        // suppressed.
        var dedupeCache = _dedupeCaches.GetOrAdd(
            treeId,
            static (_, capacity) => new RecentApplyCache(capacity),
            resolved.ShadowForwardDedupeCacheSize);

        // runningHwm tracks the highest applied HLC in this run so
        // subsequent entries can be deduped without a fresh GetAsync
        // round trip. Within a single inbound run the producer
        // guarantees per-origin HLC monotonicity, so this is strictly
        // equivalent to per-entry GetAsync followed by an in-storage
        // dedup check.
        var runningHwm = hwm;
        var anyApplied = false;
        var advancedAtAll = false;
        var highestApplied = hwm;

        // Lazy local vector clock: only the first causal-dep entry
        // pays the GetVectorAsync round trip; later entries reuse it
        // until an apply mutates it (which may have moved the local
        // VC), at which point we mark it dirty and re-fetch on the
        // next causal-dep check.
        VersionVector? cachedLocalVc = null;
        var localVcDirty = false;

        // Pending batched LWW Set/Delete items. Items pass classification
        // (not range delete, not dedup'd, not causally parked) and are
        // deferred into a single ApplyMergeManyAsync at end of run rather
        // than issuing one shard RPC per item. State changes
        // (runningHwm, highestApplied, anyApplied, advancedAtAll,
        // localVcDirty) and per-entry instrumentation
        // (ApplyDuration, ApplyLag, FifoState) are deferred until the
        // flush succeeds, mirroring the per-entry path's semantics under
        // partial-batch failure.
        List<ApplyMergeItem>? pendingItems = null;
        List<(int EntryIndex, long StartTs)>? pendingApplies = null;
        IReplicationApplyGrain? applyGrain = null;

        async Task FlushPendingAsync()
        {
            if (pendingItems is null || pendingItems.Count == 0)
            {
                return;
            }

            applyGrain ??= grainFactory.GetGrain<IReplicationApplyGrain>(treeId);

            // Hand the list off to the apply call by reference and
            // immediately null the locals - NSubstitute and other mocks
            // capture the reference for late argument matching, so a
            // subsequent .Clear() would mutate the captured snapshot
            // out from under the assertion. Production code paths read
            // the list synchronously inside ApplyMergeManyAsync, so
            // ownership transfer is safe.
            var dispatchItems = pendingItems;
            var dispatchApplies = pendingApplies!;
            pendingItems = null;
            pendingApplies = null;

            try
            {
                await applyGrain.ApplyMergeManyAsync(dispatchItems).ConfigureAwait(false);
            }
            catch
            {
                // Mirror the per-entry path: a throw records OutcomeFailure
                // for each deferred entry and rolls back their
                // shadow-forward cache reservations so the transport's
                // retry path admits them again. Without the rollback the
                // dead-letter decorator's "Applied=false clears the
                // counter" rule would silently drop the entry until
                // FIFO eviction.
                foreach (var (deferredIdx, deferredStartTs) in dispatchApplies)
                {
                    RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeFailure);
                    dedupeCache.Remove(entries[deferredIdx]);
                }
                throw;
            }

            // Flush succeeded: advance state and emit per-entry
            // observability.
            for (var p = 0; p < dispatchApplies.Count; p++)
            {
                var (deferredIdx, deferredStartTs) = dispatchApplies[p];
                var deferredEntry = entries[deferredIdx];
                RecordApplyLag(deferredEntry);
                if (!bootstrapMode)
                {
                    // Bootstrap drain is intentionally non-monotonic
                    // per (tree, origin) - see the bootstrapMode
                    // comment at run entry - so the steady-state FIFO
                    // regression counter must stay silent.
                    RecordFifoState(deferredEntry);
                }
                RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeSuccess);

                if (deferredEntry.Timestamp.CompareTo(runningHwm) > 0)
                {
                    runningHwm = deferredEntry.Timestamp;
                }
                if (deferredEntry.Timestamp.CompareTo(highestApplied) > 0)
                {
                    highestApplied = deferredEntry.Timestamp;
                }
            }

            anyApplied = true;
            advancedAtAll = true;
            localVcDirty = true;
        }

        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entry = entries[k];
            var startTs = Stopwatch.GetTimestamp();
            var outcome = LatticeReplicationMetrics.OutcomeFailure;
            var deferred = false;
            // Tracks whether the current iteration owns a live
            // shadow-forward cache reservation that must be rolled back
            // if an exception escapes. Cleared when ownership transfers
            // (deferral to pendingApplies, successful inline apply) or
            // when the park branch returns normally - in which case the
            // reservation is intentionally retained so duplicate-emit
            // pairs of the parked entry are suppressed while it is
            // buffered.
            var cacheReservedForCurrent = false;
            try
            {
                if (entry.Op == MutationKind.DeleteRange)
                {
                    // Range delete forces the pending LWW batch to flush
                    // first - the producer ordered the WAL such that
                    // entries before the range delete must observe their
                    // effect after, and any deferred LWW work must be
                    // visible before the range walk starts.
                    await FlushPendingAsync().ConfigureAwait(false);
                    await ApplyRangeAsync(entry, cancellationToken).ConfigureAwait(false);
                    anyApplied = true;
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    continue;
                }

                if (!bootstrapMode && entry.Timestamp.CompareTo(runningHwm) <= 0)
                {
                    outcome = LatticeReplicationMetrics.OutcomeDedup;
                    continue;
                }

                // Shadow-forward dedupe cache: suppress the duplicate-emit
                // pair that structural rewrites (split / merge / saga
                // compensate) generate when they shadow-forward a user
                // write into a different shard. See ApplyAsync for the
                // detailed race scenario. The check sits after the
                // runningHwm dedupe so HWM-deduped entries do not
                // pollute the cache.
                if (!dedupeCache.TryAdd(entry))
                {
                    outcome = LatticeReplicationMetrics.OutcomeShadowForwardDedup;
                    continue;
                }
                cacheReservedForCurrent = true;

                if (HasCausalDependencies(entry))
                {
                    if (cachedLocalVc is null || localVcDirty)
                    {
                        cachedLocalVc = await hwmGrain.GetVectorAsync(cancellationToken).ConfigureAwait(false);
                        localVcDirty = false;
                    }
                    if (!CausalApplyBuffer.DependenciesSatisfied(entry, cachedLocalVc))
                    {
                        await ParkAsync(entry, resolved, cancellationToken).ConfigureAwait(false);
                        // Park retains the cache reservation (mirroring
                        // ApplyAsync's park branch): the parked entry,
                        // when drained, routes via ApplyPointAsync
                        // directly and bypasses the cache, so the
                        // retained reservation continues to suppress
                        // duplicate-emit pairs of the parked entry that
                        // arrive while it is buffered. Release local
                        // rollback responsibility so the catch below
                        // does not undo the intentional retention.
                        cacheReservedForCurrent = false;
                        outcome = LatticeReplicationMetrics.OutcomeParkedCausalBuffer;
                        continue;
                    }
                }

                // Classify: only LWW-register Set/Delete entries are
                // batchable. Typed-CRDT modes (OrSet, PnCounter,
                // VersionVector) need per-entry CAS loops on the
                // shard-root and stay on the per-entry path.
                //
                // Saga prepare-phase entries (IsPrepared==true) are
                // explicitly excluded from the batched LWW path: the
                // batched path collapses the per-entry route through
                // ApplyMergeManyAsync, which calls into the shard-root's
                // generic LWW merge primitive without honouring
                // IsPrepared / TransactionId. Routing a prepared
                // record through that primitive applies it directly
                // into the visible projection, bypassing the per-tx
                // pending bucket on the receiver leaf - the same
                // failure mode the producer-side prepare path exists
                // to prevent, manifesting on the wire instead of in
                // memory. Cross-cluster atomic-visibility of a saga
                // collapses to ad-hoc per-key arrival order: keys
                // whose prepares are batched land as visible writes
                // before the terminal arrives, and a receiver reader
                // that scans the batch mid-flight observes a strict
                // subset of the saga's keys. Forcing prepared entries
                // back onto the per-entry path routes them through
                // ApplyPointAsync's IsPrepared branch, which calls
                // ApplyPreparedSetAsync / ApplyPreparedDeleteAsync on
                // the receiver and parks them in the leaf's per-tx
                // pending bucket until the matching terminal arrives.
                var batchable = entry.Mode == LatticeMergeMode.LwwRegister
                    && (entry.Op == MutationKind.Set || entry.Op == MutationKind.Delete)
                    && !entry.IsPrepared;

                if (!batchable)
                {
                    await FlushPendingAsync().ConfigureAwait(false);
                    await ApplyPointAsync(entry).ConfigureAwait(false);
                    // Successful apply: clear local rollback
                    // responsibility. The cache reservation is retained
                    // in the steady-state cache (it is the desired
                    // outcome for non-failure paths).
                    cacheReservedForCurrent = false;
                    RecordApplyLag(entry);
                    if (!bootstrapMode)
                    {
                        // Bootstrap drain suppresses FIFO state tracking
                        // for the same reason the deferred-apply branch
                        // does above.
                        RecordFifoState(entry);
                    }

                    if (entry.Timestamp.CompareTo(runningHwm) > 0)
                    {
                        runningHwm = entry.Timestamp;
                    }
                    if (entry.Timestamp.CompareTo(highestApplied) > 0)
                    {
                        highestApplied = entry.Timestamp;
                    }
                    anyApplied = true;
                    advancedAtAll = true;
                    localVcDirty = true;
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    continue;
                }

                // Batched path. Validate Set's value-non-null contract
                // here so the ArgumentException surface matches the
                // per-entry path (ApplyPointAsync raises the same).
                if (entry.Op == MutationKind.Set && entry.Value is null)
                {
                    throw new ArgumentException(
                        "WalRecord.Value must be non-null for MutationKind.Set.",
                        nameof(entries));
                }

                pendingItems ??= new List<ApplyMergeItem>();
                pendingApplies ??= new List<(int, long)>();
                pendingItems.Add(new ApplyMergeItem
                {
                    Key = entry.Key,
                    Value = entry.Op == MutationKind.Set ? entry.Value : null,
                    SourceHlc = entry.Timestamp,
                    OriginClusterId = entry.OriginClusterId!,
                    SourceVectorClock = null,
                    ExpiresAtTicks = entry.Op == MutationKind.Set ? entry.ExpiresAtTicks : 0,
                    IsTombstone = entry.Op == MutationKind.Delete,
                });
                pendingApplies.Add((k, startTs));
                // Ownership of the cache reservation transfers to
                // pendingApplies; FlushPendingAsync's failure path
                // rolls it back if the eventual flush throws.
                cacheReservedForCurrent = false;
                deferred = true;
            }
            catch
            {
                // Roll back the current iteration's reservation if it
                // was held but neither applied, parked, nor deferred.
                // Hit by ApplyPointAsync / ParkAsync / GetVectorAsync
                // throws and by the contract-violation ArgumentException
                // for batchable Set with null Value.
                if (cacheReservedForCurrent)
                {
                    dedupeCache.Remove(entry);
                }

                // An exception escaping the loop body would otherwise
                // leave any previously-deferred entries with a captured
                // start timestamp but no recorded outcome, producing
                // phantom started-never-completed samples in the apply
                // duration histogram. Record OutcomeFailure for every
                // deferred entry now (the throwing entry's own failure
                // is recorded by the finally below). Cold path only -
                // hit by contract violations (Set with null Value),
                // mid-loop cancellation, and FlushPendingAsync re-throws
                // when the throw originates somewhere other than inside
                // FlushPendingAsync (which nulls pendingApplies before
                // its own await and so leaves this branch a no-op).
                // Each deferred entry's cache reservation is rolled
                // back here for the same dead-letter-retry reason
                // FlushPendingAsync rolls back its own dispatched set.
                if (pendingApplies is { Count: > 0 })
                {
                    foreach (var (deferredIdx, deferredStartTs) in pendingApplies)
                    {
                        RecordApplyDuration(treeId, origin!, deferredStartTs, LatticeReplicationMetrics.OutcomeFailure);
                        dedupeCache.Remove(entries[deferredIdx]);
                    }
                    pendingItems = null;
                    pendingApplies = null;
                }
                throw;
            }
            finally
            {
                if (!deferred)
                {
                    RecordApplyDuration(treeId, origin!, startTs, outcome);
                }
            }
        }

        // End-of-run flush of any remaining deferred items.
        await FlushPendingAsync().ConfigureAwait(false);

        if (advancedAtAll && !bootstrapMode)
        {
            var advanced = await hwmGrain.TryAdvanceAsync(origin!, highestApplied, cancellationToken)
                .ConfigureAwait(false);
            var newHwm = advanced
                ? highestApplied
                : await hwmGrain.GetAsync(origin!, cancellationToken).ConfigureAwait(false);

            if (advanced)
            {
                // Mirror the foreign advance into the producer-side
                // local vector clock cache. Mirrors the per-entry path's
                // AdvanceForeign call site post-TryAdvanceAsync so a
                // batch-applied run keeps the producer view in sync
                // with the receiver-side HWM grain.
                localVectorClockCache.AdvanceForeign(treeId, origin!, highestApplied);
                await DrainBufferAsync(treeId, hwmGrain, resolved, cancellationToken).ConfigureAwait(false);
            }

            return new ApplyResult { Applied = anyApplied, HighWaterMark = newHwm };
        }

        // Bootstrap mode: the per-origin HWM is pinned atomically at
        // the snapshot's AsOfHlc by
        // <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
        // after the drain completes; advancing it mid-drain would
        // suppress still-pending saga keys with strictly-earlier source
        // HLCs. Surface the pre-drain HWM so callers observe the
        // canonical pre-pin frontier.
        return new ApplyResult { Applied = anyApplied, HighWaterMark = hwm };
    }

    /// <summary>
    /// Fallback per-entry walk for runs whose first entry has an empty
    /// tree-id or origin. Routes through <see cref="ApplyAsync"/> so
    /// the per-entry validation guards surface the correct
    /// <see cref="ArgumentException"/> path.
    /// </summary>
    private async Task<ApplyResult> ApplyRunPerEntryAsync(
        IReadOnlyList<WalRecord> entries,
        int startInclusive,
        int endExclusive,
        CancellationToken cancellationToken)
    {
        var anyApplied = false;
        var highest = HybridLogicalClock.Zero;
        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var r = await ApplyAsync(entries[k], cancellationToken).ConfigureAwait(false);
            if (r.Applied)
            {
                anyApplied = true;
            }
            if (r.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = r.HighWaterMark;
            }
        }
        return new ApplyResult { Applied = anyApplied, HighWaterMark = highest };
    }
}
