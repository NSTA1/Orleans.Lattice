using System.Diagnostics;
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
        IReadOnlyList<ReplogEntry> entries,
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
    ///   <item><description>The first entry's <see cref="ReplogEntry.Timestamp"/>
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
        IReadOnlyList<ReplogEntry> entries,
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
                RecordApplyDuration(treeId, startTs, LatticeReplicationMetrics.OutcomeDedup);
            }
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        var hwmGrain = GetHwmGrain(treeId);
        var hwm = await hwmGrain.GetAsync(origin!, cancellationToken).ConfigureAwait(false);

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
        // until an apply has occurred (which may have moved the local
        // VC), at which point we mark it dirty and re-fetch on the
        // next causal-dep check.
        VersionVector? cachedLocalVc = null;
        var localVcDirty = false;

        for (var k = startInclusive; k < endExclusive; k++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entry = entries[k];
            var startTs = Stopwatch.GetTimestamp();
            var outcome = LatticeReplicationMetrics.OutcomeFailure;
            try
            {
                if (entry.Op == ReplogOp.DeleteRange)
                {
                    await ApplyRangeAsync(entry, cancellationToken).ConfigureAwait(false);
                    anyApplied = true;
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    continue;
                }

                if (entry.Timestamp.CompareTo(runningHwm) <= 0)
                {
                    outcome = LatticeReplicationMetrics.OutcomeDedup;
                    continue;
                }

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
                        outcome = LatticeReplicationMetrics.OutcomeParkedCausalBuffer;
                        continue;
                    }
                }

                await ApplyPointAsync(entry).ConfigureAwait(false);
                RecordApplyLag(entry);
                RecordFifoState(entry);

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
            }
            finally
            {
                RecordApplyDuration(treeId, startTs, outcome);
            }
        }

        if (advancedAtAll)
        {
            var advanced = await hwmGrain.TryAdvanceAsync(origin!, highestApplied, cancellationToken)
                .ConfigureAwait(false);
            var newHwm = advanced
                ? highestApplied
                : await hwmGrain.GetAsync(origin!, cancellationToken).ConfigureAwait(false);

            if (advanced)
            {
                await DrainBufferAsync(treeId, hwmGrain, resolved, cancellationToken).ConfigureAwait(false);
            }

            return new ApplyResult { Applied = anyApplied, HighWaterMark = newHwm };
        }

        return new ApplyResult { Applied = anyApplied, HighWaterMark = hwm };
    }

    /// <summary>
    /// Fallback per-entry walk for runs whose first entry has an empty
    /// tree-id or origin. Routes through <see cref="ApplyAsync"/> so
    /// the per-entry validation guards surface the correct
    /// <see cref="ArgumentException"/> path.
    /// </summary>
    private async Task<ApplyResult> ApplyRunPerEntryAsync(
        IReadOnlyList<ReplogEntry> entries,
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
