using Orleans.Concurrency;
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
    /// Event-driven rebind entry point. Invoked by the core tree-registry alias
    /// observer (<see cref="ITreeAliasObserver"/>) when the maintainer's source
    /// tree has its physical-identity alias swapped (shadow-cutover restore,
    /// resize, reshard). The maintainer marks its source binding stale so its next
    /// drain re-resolves the source physical id and heals (unpins the retired WAL,
    /// resets offsets, rebuilds from the new source) instead of waiting out the
    /// coarse <see cref="LatticeViewOptions.SourceIdentityBackstopInterval"/> poll.
    /// Best-effort: a missed notification still heals via that backstop, so the
    /// call never needs to block on the rebuild itself. <paramref name="newPhysicalTreeId"/>
    /// is the registry's post-swap effective physical id, carried for diagnostics;
    /// the authoritative re-resolve happens on the next drain.
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/>: the registry sends it from
    /// inside its own non-reentrant alias turn, after committing the new row. A
    /// drain that has already seen that row may be rebuilding into a fresh
    /// generation tree whose registration queues behind the same turn, so a push
    /// that waited for the drain would deadlock the registry (#5279). It only
    /// records the request, so interleaving it into a drain is safe.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task NotifySourceIdentityChangedAsync(string newPhysicalTreeId, CancellationToken cancellationToken = default);

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
    /// Throws <see cref="InvalidOperationException"/> for a
    /// <see cref="LatticeViewReplicationMode.ShipView"/> consumer.
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
    /// <see cref="RebuildAsync"/> as a tracked grain call: reports its scan,
    /// projection and swap progress to the coordinated operation
    /// <paramref name="ticket"/> names, and stops when that operation is cancelled.
    /// </summary>
    /// <param name="ticket">The operation to report to. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    [ResponseTimeout(Operations.LatticeMaintenanceProgress.TrackedCallResponseTimeout)]
    Task RebuildTrackedAsync(Operations.LatticeOperationTicket ticket, CancellationToken cancellationToken = default);

    /// <summary>
    /// <see cref="ReconcileAsync"/> as a tracked grain call: reports its digest,
    /// scan, projection, comparison and swap progress to the coordinated operation
    /// <paramref name="ticket"/> names, and stops when that operation is cancelled.
    /// </summary>
    /// <param name="ticket">The operation to report to. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the reconcile.</param>
    /// <returns><see langword="true"/> when drift was detected and repaired.</returns>
    [ResponseTimeout(Operations.LatticeMaintenanceProgress.TrackedCallResponseTimeout)]
    Task<bool> ReconcileTrackedAsync(Operations.LatticeOperationTicket ticket, CancellationToken cancellationToken = default);

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
    /// Returns <see cref="GetLagAsync"/> and then <see cref="GetActiveTreeIdAsync"/>
    /// from one turn, so a status read pays one maintainer call rather than two.
    /// Throws exactly as <see cref="GetLagAsync"/> would.
    /// </summary>
    Task<ViewMaintainerStatus> GetStatusAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Read-your-writes barrier: drives drains until the highest applied source
    /// HLC reaches <paramref name="target"/>, or throws
    /// <see cref="TimeoutException"/> once <paramref name="timeout"/> elapses.
    /// Throws <see cref="InvalidOperationException"/> for a
    /// <see cref="LatticeViewReplicationMode.ShipView"/> consumer.
    /// </summary>
    Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current source head HLC: the highest committed source HLC
    /// across every source WAL partition, or <see cref="HybridLogicalClock.Zero"/>
    /// when the source is empty. Used to capture a write-then-wait target.
    /// Throws <see cref="InvalidOperationException"/> for a
    /// <see cref="LatticeViewReplicationMode.ShipView"/> consumer.
    /// </summary>
    Task<HybridLogicalClock> CaptureSourceHeadHlcAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Read-your-writes barrier against the current source head: resolves the
    /// source's current physical tree, captures the head offset of each of its
    /// WAL partitions, and drives drains (forcing a rebind when the view is bound
    /// to another physical tree) until the view is bound to that tree and has
    /// consumed every entry below each captured head, or throws
    /// <see cref="TimeoutException"/> once <paramref name="timeout"/> elapses.
    /// Positional rather than an HLC watermark, so it also covers writes that keep
    /// an older HLC and a source cutover onto a new physical tree. Runs as a single
    /// maintainer call so the read handle pays one grain round-trip per barrier.
    /// Throws <see cref="InvalidOperationException"/> for a
    /// <see cref="LatticeViewReplicationMode.ShipView"/> consumer.
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
