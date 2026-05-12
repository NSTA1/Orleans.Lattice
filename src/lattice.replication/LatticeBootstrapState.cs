namespace Orleans.Lattice.Replication;

/// <summary>
/// State of the receiver-side snapshot/bootstrap state machine for a
/// single tree. Drives transitions
/// <see cref="Idle"/> → <see cref="RequestingSnapshot"/>
/// → <see cref="ApplyingSnapshot"/>
/// → <see cref="IncrementalHandoff"/>
/// → <see cref="LiveIncremental"/>. A failure at any stage
/// transitions to <see cref="Failed"/>; the state machine is then
/// idempotently restartable by re-invoking
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>.
/// </summary>
public enum LatticeBootstrapState
{
    /// <summary>No bootstrap has been started for this tree on this receiver.</summary>
    Idle = 0,

    /// <summary>
    /// The coordinator has called
    /// <see cref="ISnapshotProvider.ExportAsync"/> and is waiting for
    /// the producer to return the <see cref="SnapshotStream"/>
    /// metadata + entry stream handle.
    /// </summary>
    RequestingSnapshot = 1,

    /// <summary>
    /// The snapshot stream is open and the coordinator is draining
    /// <see cref="SnapshotStream.Entries"/> through the local
    /// apply seam, preserving each entry's commit-time
    /// <see cref="Primitives.HybridLogicalClock"/> and stamping the
    /// supplied source cluster id as the origin.
    /// </summary>
    ApplyingSnapshot = 2,

    /// <summary>
    /// Snapshot drain is complete; the coordinator is pinning the
    /// snapshot's <see cref="SnapshotStream.AsOfHlc"/> +
    /// <see cref="SnapshotStream.CausalStableFrontier"/> on the per-tree
    /// <see cref="Grains.IReplicationHighWaterMarkGrain"/> so the first
    /// incremental entry runs through the per-origin HWM dedupe and
    /// the causal-plus dependency check from a non-empty frontier.
    /// </summary>
    IncrementalHandoff = 3,

    /// <summary>
    /// Bootstrap is complete. Incremental replication is live; the
    /// receiver consumes new entries through the normal apply path
    /// from the pinned frontier.
    /// </summary>
    LiveIncremental = 4,

    /// <summary>
    /// A previous bootstrap attempt threw an exception. The state
    /// machine is reentrant - calling
    /// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> again
    /// restarts the cycle from <see cref="RequestingSnapshot"/>.
    /// </summary>
    Failed = 5,
}
