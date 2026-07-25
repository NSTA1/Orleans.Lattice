using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Per-view maintainer grain, keyed by the view name. A single cluster-wide
/// activation tails every source WAL partition from the durable checkpoint,
/// projects each user mutation through the view's
/// <see cref="ILatticeViewProjection"/>, coalesces repeated writes to the same
/// view key (last-writer-wins on the source HLC), applies the survivors to the
/// <c>view-{name}</c> tree, advances and persists the checkpoint, and reports its
/// applied cursor to the WAL garbage collector.
/// </summary>
[Alias(TypeAliases.IViewMaintainerGrain)]
internal interface IViewMaintainerGrain : IGrainWithStringKey
{
    /// <summary>
    /// Idempotently brings the maintainer online: verifies the persisted
    /// projection version (rebuilding the view if it changed) and starts the
    /// background drain. Safe to call repeatedly.
    /// </summary>
    Task EnsureActiveAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs a single drain pass synchronously: reads up to the configured batch
    /// size from each source partition, applies the coalesced view writes, and
    /// checkpoints. Returns the number of view writes applied. Exposed so callers
    /// (and tests) can drive convergence deterministically without waiting on the
    /// background timer.
    /// </summary>
    Task<int> DrainAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the view's apply lag: the number of committed-but-unapplied source
    /// WAL entries summed across every source partition. Zero means the view has
    /// caught up to the source head as of this call.
    /// </summary>
    Task<long> GetLagAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rebuilds the view from current source state using a shadow-swap: builds a
    /// complete new generation tree, then atomically flips the active generation
    /// (and the resume checkpoint) over in a single durable commit, so readers
    /// never observe a half-built view. Used on a fall-off-log condition or a
    /// projection-version change.
    /// </summary>
    Task RebuildAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// View anti-entropy for a locally-derived view: builds the expected view from
    /// current source state into a shadow generation, compares it against the live
    /// view via a <see cref="ViewDigest"/>, and swaps the shadow in (repairing the
    /// view) only when they diverge. Returns <see langword="true"/> when drift was
    /// detected and repaired, <see langword="false"/> when the view already
    /// matched the source.
    /// </summary>
    Task<bool> ReconcileAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Computes a deterministic, order-independent <see cref="ViewDigest"/> over
    /// the active generation's materialised (key, value) entries (excluding any
    /// reserved aggregation internal rows).
    /// </summary>
    Task<ViewDigest> ComputeViewDigestAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the grain id of the view tree currently serving reads: the
    /// generation-addressed id for the durable active generation. The read handle
    /// caches this to resolve queries without a grain hop per read.
    /// </summary>
    Task<string> GetActiveTreeIdAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Read-your-writes barrier: drives drains until the highest applied source
    /// HLC reaches <paramref name="target"/>, or throws
    /// <see cref="TimeoutException"/> once <paramref name="timeout"/> elapses.
    /// </summary>
    Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current source head HLC: the highest committed source HLC
    /// across every source WAL partition, or <see cref="HybridLogicalClock.Zero"/>
    /// when the source is empty. Used to capture a write-then-wait target.
    /// </summary>
    Task<HybridLogicalClock> CaptureSourceHeadHlcAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Read-your-writes barrier against the current source head: captures the
    /// source head HLC and drives drains until the view has applied up to it, or
    /// throws <see cref="TimeoutException"/> once <paramref name="timeout"/>
    /// elapses. Combines the capture and the wait into a single maintainer call
    /// so the read handle does not pay two sequential grain round-trips per
    /// barrier; the capture and wait run in-process on the one activation.
    /// </summary>
    Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Tears the view down: unregisters the keepalive reminder so the grain stops
    /// being kept alive, releases the source WAL cursor pin, deletes every backing
    /// view-tree generation through the standard tree-deletion machinery, and
    /// clears the durable checkpoint state. Idempotent - safe to call on a view
    /// that was never activated or has already been decommissioned. The caller
    /// (the factory) removes the catalog entry and the durable runtime
    /// registration after this completes.
    /// </summary>
    Task DecommissionAsync(CancellationToken cancellationToken = default);
}
