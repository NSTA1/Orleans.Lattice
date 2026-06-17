using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Per-view maintainer grain, keyed by the view name. A single cluster-wide
/// activation tails every source WAL partition from the durable checkpoint,
/// projects each user mutation through the view's
/// <see cref="ILatticeViewProjection"/>, coalesces repeated writes to the same
/// view key (last-writer-wins on the source HLC), applies the survivors to the
/// <c>view-{name}</c> tree, advances and persists the checkpoint, and reports its
/// applied cursor to the WAL garbage collector.
/// </summary>
[Alias(ReplicationTypeAliases.IViewMaintainerGrain)]
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
    /// Rebuilds the view in place from the current source state and resumes
    /// tailing from the captured source head. Used on a fall-off-log condition or
    /// a projection-version change.
    /// </summary>
    Task RebuildAsync(CancellationToken cancellationToken = default);

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
}
