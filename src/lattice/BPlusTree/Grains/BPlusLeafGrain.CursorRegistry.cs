using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cursor-registry integration partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.
/// Reports the leaf's highest applied <see cref="HybridLogicalClock"/> to
/// the silo-scoped <see cref="ILeafCursorReporter"/> after every successful
/// projection-checkpoint persist so the per-shard WAL GC pins its trim
/// point under the slowest local consumer (the leaf-as-materialiser).
/// <para>
/// Lazy and zero-cost when nothing drives the projection: the helper is a
/// branch check + early return when <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.Clock"/> is
/// still <see cref="HybridLogicalClock.Zero"/>. The
/// <see cref="ILeafCursorReporter"/> is registered by default (an in-memory
/// reporter wired by <c>AddLattice</c>), so the leaf reports its applied
/// frontier into the always-on cursor registry on every host and the WAL
/// saturation sampler's drain-lag back-pressure is live for every write
/// workload. The durable-pin mirror on
/// <see cref="ILeafCursorReporter.NoteDurableMaterialiserFrontier"/> is a
/// no-op until the host opts into <c>AddWalCursorRegistry</c> (directly or
/// via the WAL GC / views / replication / storage packages), which swaps in
/// the durable-pin-aware reporter; until then the in-memory report still
/// flows and the WAL GC behaves identically to its pre-promotion baseline.
/// </para>
/// <para>
/// Failures to advance the cursor are logged-and-swallowed at warning
/// level: the cursor is monotonic by construction so the next successful
/// flush catches up, and a transient registry hiccup must not stall the
/// foreground write path or block the leaf's own checkpoint advance.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Cached <see cref="ILeafCursorReporter"/> resolved from
    /// <see cref="IGrainContext.ActivationServices"/> on first use.
    /// Normally non-<c>null</c>: <c>AddLattice</c> registers an in-memory
    /// reporter by default. <c>null</c> only on a host that has stripped
    /// even that default registration, in which case the cursor-report
    /// path is a no-op.
    /// </summary>
    private ILeafCursorReporter? _cursorReporter;

    /// <summary>
    /// <see langword="true"/> once the lazy resolution of
    /// <see cref="_cursorReporter"/> has run. The resolution is a single
    /// dictionary lookup; caching the outcome (including the
    /// <c>null</c> result) avoids paying it on every successful flush.
    /// </summary>
    private bool _cursorReporterResolved;

    /// <summary>
    /// Cached consumer id template of the form
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}</c> for the
    /// single-partition shape, or the partition-suffixed form
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}_{partition}</c>
    /// when <see cref="LatticeOptions.WalPartitions"/> > 1. Computed
    /// once on first use; <c>null</c> when <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.TreeId"/>
    /// is unset (system-tree leaves and tests that bypass tree
    /// initialisation), in which case the cursor-report path is a
    /// no-op.
    /// </summary>
    private string? _cachedConsumerIdBase;

    /// <summary>
    /// Reports the leaf's current projection HLC to the registered
    /// <see cref="ILeafCursorReporter"/>, lazy-gated on
    /// <c>state.State.Clock &gt; HybridLogicalClock.Zero</c>. Called from
    /// <see cref="FlushPendingCheckpointAsync"/> after every successful
    /// persist; never throws. Under multi-partition WAL the leaf reports
    /// one cursor per partition so the per-shard WAL GC trims each
    /// partition independently against its own slowest consumer.
    /// </summary>
    private async Task ReportCursorIfActiveAsync()
    {
        var clock = state.State.Clock;
        if (clock <= HybridLogicalClock.Zero)
        {
            return;
        }

        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);

        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            try
            {
                await reporter.ReportAsync(treeId, consumerId, clock, CancellationToken.None);
                // Mirror the frontier into the durable pin store so the WAL
                // GC's trim floor survives a full restart. Fire-and-forget
                // and coalesced inside the reporter - no synchronous durable
                // write is added to the checkpoint path here.
                reporter.NoteDurableMaterialiserFrontier(treeId, consumerId, clock);
            }
            catch (Exception ex)
            {
                var logger = context.ActivationServices?
                    .GetService<ILoggerFactory>()?
                    .CreateLogger<BPlusLeafGrain>();
                logger?.LogWarning(
                    ex,
                    "Failed to report leaf cursor for tree {TreeId} consumer {ConsumerId} at HLC {Cursor}; will retry on next checkpoint flush.",
                    treeId,
                    consumerId,
                    clock);
            }
        }
    }

    /// <summary>
    /// Seeds the durable WAL materialiser pin store with this leaf's
    /// persisted checkpoint frontier (which may be
    /// <see cref="HybridLogicalClock.Zero"/> for a leaf that has activated
    /// but never checkpointed). Unlike <see cref="ReportCursorIfActiveAsync"/>
    /// this is <b>not</b> gated on <c>Clock &gt; Zero</c>: a Zero seed is a
    /// deliberate "block" pin that keeps the WAL head retained for a
    /// never-checkpointed leaf across a restart, closing the edge where a
    /// leaf has applied and shipped a write but never produced a checkpoint.
    /// Reports to the in-memory registry are skipped for the Zero case (a
    /// Zero cursor would either be rejected or pin the trim point at offset
    /// zero); only the durable, GC-floor-only pin is seeded. The seed uses
    /// the same per-partition consumer-id shape as
    /// <see cref="ReportCursorIfActiveAsync"/> so a later real-frontier
    /// report advances the same key rather than orphaning the Zero seed.
    /// Never throws.
    /// </summary>
    private async Task SeedDurableMaterialiserFrontierAsync()
    {
        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;
        var clock = state.State.Clock;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            reporter.NoteDurableMaterialiserFrontier(treeId, consumerId, clock);
        }
    }

    /// <summary>
    /// Durably seeds a <see cref="HybridLogicalClock.Zero"/> "block" pin for
    /// this leaf and <b>awaits</b> the write, closing the window in which a
    /// freshly-born data-capable leaf can have its WAL trimmed past its
    /// un-materialised frontier before it registers any pin. Called at the two
    /// tree-id birth seams - a split sibling's
    /// <see cref="InitializeSiblingAsync"/> and a root/bulk-load leaf's
    /// <see cref="SetTreeIdAsync"/> - <em>before</em> the inherited/routed
    /// writes that follow (<see cref="MergeEntriesAsync"/>) make the leaf's data
    /// reachable in the WAL. Seeds <see cref="HybridLogicalClock.Zero"/> (not the
    /// current clock) because the leaf has checkpointed none of its data yet, so
    /// its entire range is un-materialised; a Zero pin disables the WAL GC's
    /// cursor-trim branch for the tree until the leaf produces its first durable
    /// checkpoint and advances the pin past Zero. Idempotent and never throws
    /// (the awaited seam swallows transient failures); a no-op when the host has
    /// no cursor reporter (pre-WAL) or the tree id is unset.
    /// </summary>
    private async Task SeedDurableMaterialiserBlockPinAsync()
    {
        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        var reports = new MaterialiserPinReport[partitionCount];
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            reports[partition] = new MaterialiserPinReport(consumerId, HybridLogicalClock.Zero);
        }

        await reporter.SeedDurableMaterialiserBlockManyAsync(
            treeId, reports, CancellationToken.None);
    }

    private ILeafCursorReporter? ResolveCursorReporter()
    {
        if (_cursorReporterResolved)
            return _cursorReporter;

        _cursorReporterResolved = true;
        _cursorReporter = context.ActivationServices?.GetService<ILeafCursorReporter>();
        return _cursorReporter;
    }

    private string? ResolveConsumerIdBase()
    {
        if (_cachedConsumerIdBase is not null)
            return _cachedConsumerIdBase;

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return null;

        _cachedConsumerIdBase = $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{context.GrainId}";
        return _cachedConsumerIdBase;
    }

    /// <summary>
    /// Builds the per-partition consumer id. The single-partition shape
    /// (<paramref name="partitionCount"/> == 1) returns the legacy
    /// unsuffixed form for wire compatibility with hosts that have
    /// never enabled multi-partition WAL replay; the multi-partition
    /// shape suffixes <c>_{partition}</c> so each partition's cursor
    /// is tracked independently.
    /// </summary>
    private static string BuildConsumerId(string idBase, int partition, int partitionCount)
        => partitionCount == 1 ? idBase : $"{idBase}_{partition}";
}