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
    /// Cached consumer id of the form
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}</c>. Computed once
    /// on first use; <c>null</c> when <see cref="LeafNodeState.TreeId"/>
    /// is unset (system-tree leaves and tests that bypass tree
    /// initialisation), in which case the cursor-report path is a
    /// no-op.
    /// </summary>
    private string? _cachedConsumerId;

    /// <summary>
    /// Reports the leaf's current projection HLC to the registered
    /// <see cref="ILeafCursorReporter"/>, lazy-gated on
    /// <c>state.State.Clock &gt; HybridLogicalClock.Zero</c>. Called from
    /// <see cref="FlushPendingCheckpointAsync"/> after every successful
    /// persist; never throws.
    /// </summary>
    private async Task ReportCursorIfActiveAsync()
    {
        var clock = state.State.Clock;
        if (clock <= HybridLogicalClock.Zero)
        {
            // No Apply has run, the projection is
            // empty, and registering at Zero would pin the WAL at offset
            // zero indefinitely. Skip the report entirely so a host that
            // has not yet flipped to the WAL-as-sole-commit-point
            // promotion never registers a cursor.
            return;
        }

        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            // Host has not added Orleans.Lattice.Replication; nothing to
            // report against and the per-shard WAL GC predicate falls
            // back to its pre-integration baseline.
            return;
        }

        var consumerId = ResolveConsumerId();
        if (consumerId is null)
        {
            // Tree id has not been seeded yet (system trees, isolated
            // unit tests). The leaf is not eligible for cursor
            // registration in this state.
            return;
        }

        var treeId = state.State.TreeId!; // non-null when consumerId resolved.

        try
        {
            await reporter.ReportAsync(treeId, consumerId, clock, CancellationToken.None);
        }
        catch (Exception ex)
        {
            // Monotonic cursor: the next successful flush will catch up,
            // so a transient registry failure must not stall the
            // foreground write path or block our checkpoint advance.
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

    private ILeafCursorReporter? ResolveCursorReporter()
    {
        if (_cursorReporterResolved)
            return _cursorReporter;

        _cursorReporterResolved = true;
        _cursorReporter = context.ActivationServices?.GetService<ILeafCursorReporter>();
        return _cursorReporter;
    }

    private string? ResolveConsumerId()
    {
        if (_cachedConsumerId is not null)
            return _cachedConsumerId;

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return null;

        // Stable, deterministic consumer id pinned
        // to the leaf grain so each leaf advances its own cursor
        // independently and the registry's per-shard min predicate
        // (replication R-083) trims under the slowest local consumer.
        _cachedConsumerId = $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{context.GrainId}";
        return _cachedConsumerId;
    }
}