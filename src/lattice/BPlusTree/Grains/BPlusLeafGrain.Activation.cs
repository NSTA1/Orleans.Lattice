using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-hook partial for <see cref="BPlusLeafGrain"/>. Implements
/// <see cref="IGrainBase.OnActivateAsync"/> as an eager cursor-registration
/// step so a quiescent leaf becomes visible to the per-shard WAL GC the
/// moment its activation completes, rather than waiting for the next
/// foreground write to drive a checkpoint flush via the lazy-on-flush cursor path.
/// <para>
/// The eager step is gated on three preconditions: the commit-log adapter
/// (<see cref="ICommitLogReader"/>) must be registered (i.e. the host has
/// added <c>Orleans.Lattice.Replication</c>), the persisted projection
/// clock must have advanced past <see cref="HybridLogicalClock.Zero"/>
/// (otherwise registering at zero would pin the WAL trim point at offset
/// zero indefinitely on a leaf that has never seen a write), and the
/// tree id must have been seeded (system-tree leaves and pre-init
/// activations are skipped). Pre-WAL hosts that have not added the
/// replication package take an early-return no-op path and the WAL GC
/// behaves identically to its pre-promotion baseline.
/// </para>
/// <para>
/// All exceptions are swallowed (and logged at warning level) so a
/// transient registry failure during activation cannot block the
/// grain coming online — the next foreground flush will retry via the
/// existing lazy-on-flush cursor path, and the cursor is monotonic so a missed
/// initial registration is recoverable.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Eagerly publishes the leaf's persisted projection HLC to the
    /// silo-scoped <see cref="ILeafCursorReporter"/> on activation when
    /// the commit-log adapter is present. No-op when the host has not
    /// added the replication package, when the projection clock is
    /// still <see cref="HybridLogicalClock.Zero"/>, or when the tree id
    /// has not been seeded.
    /// </summary>
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Eager registration is conditional on the commit-log
            // adapter being present: when no replication package has
            // been added, the lazy-on-flush cursor path is the right
            // fallback and an eager registration at activation time
            // would be unnecessary work.
            var commitLogReader = context.ActivationServices?.GetService<ICommitLogReader>();
            if (commitLogReader is null)
                return;

            // Skip leaves whose projection has never advanced
            // (registering at HLC zero would pin the WAL trim point at
            // offset zero forever on a leaf that has never seen a
            // write).
            var clock = state.State.Clock;
            if (clock <= HybridLogicalClock.Zero)
                return;

            // Defer to the same gating logic as the lazy-on-flush path so
            // the consumer-id format and reporter resolution stay in
            // exactly one place. ReportCursorIfActiveAsync is itself
            // gated on Clock > Zero (already checked above) and
            // non-null reporter / tree id / consumer id.
            await ReportCursorIfActiveAsync();
        }
        catch (Exception ex)
        {
            // Activation must not fail because of a registry hiccup.
            // The cursor is monotonic so the next successful flush
            // catches up via the lazy-on-flush cursor path.
            var logger = context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>();
            logger?.LogWarning(
                ex,
                "Eager cursor registration failed during activation for leaf {GrainId}; will retry on next checkpoint flush.",
                context.GrainId);
        }
    }
}
