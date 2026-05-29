using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cursor-registry integration partial for <see cref="BPlusLeafGrain"/>.
/// Reports the leaf's highest applied <see cref="HybridLogicalClock"/> to
/// the silo-scoped <see cref="ILeafCursorReporter"/> after every successful
/// projection-checkpoint persist so the per-shard WAL GC pins its trim
/// point under the slowest local consumer (the leaf-as-materialiser).
/// <para>
/// Lazy and zero-cost when nothing drives the projection: the helper is a
/// branch check + early return when <see cref="LeafNodeState.Clock"/> is
/// still <see cref="HybridLogicalClock.Zero"/>, and the
/// <see cref="ILeafCursorReporter"/> resolution itself is short-circuited
/// when the host has not added <c>Orleans.Lattice.Replication</c>. Pre-WAL
/// hosts therefore never register a cursor and the WAL GC behaves
/// identically to its pre-promotion baseline.
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
    /// <c>null</c> when the host has not added the replication package
    /// (the registration is absent), in which case the cursor-report
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
    /// once on first use; <c>null</c> when <see cref="LeafNodeState.TreeId"/>
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