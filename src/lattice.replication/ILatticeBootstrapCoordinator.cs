namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side snapshot/bootstrap coordinator. Drives the state
/// machine that seeds a tree from an
/// <see cref="ISnapshotProvider"/> export, applies every snapshot
/// entry through the local apply seam preserving the source HLC, and
/// pins the snapshot's causal-stable frontier on the per-tree
/// high-water-mark grain so the first incremental entry arriving after the
/// snapshot sees a non-empty frontier and a snapshot-pinned floor.
/// <para>
/// Triggered by the fall-off detector when the maintenance pass
/// finds the receiver behind the source cluster's retained WAL,
/// and by operator-driven re-seed flows. The coordinator itself is
/// transport-agnostic: it consumes whichever
/// <see cref="ISnapshotProvider"/> is registered in DI, so a host can
/// front it with a transport-aware fetcher without changing this
/// surface.
/// </para>
/// <para>
/// The default implementation is a thin façade over a per-tree
/// internal grain whose cluster-wide single activation makes the
/// bootstrap mutually exclusive across every silo in the receiver
/// cluster. Two silos that concurrently call
/// <see cref="BootstrapAsync"/> for the same tree will both route to
/// one grain activation. A same-source kickoff is absorbed as an
/// idempotent no-op; a different-source kickoff receives an
/// <see cref="InvalidOperationException"/> indicating an in-progress
/// bootstrap. Different trees bootstrap on different activations and
/// proceed in parallel.
/// </para>
/// </summary>
public interface ILatticeBootstrapCoordinator
{
    /// <summary>
    /// Returns the current <see cref="LatticeBootstrapState"/> for
    /// <paramref name="treeName"/>, or
    /// <see cref="LatticeBootstrapState.Idle"/> when no bootstrap has
    /// been started for that tree on the receiver cluster. Bootstrap
    /// phase is persisted, so an activation restart resumes from the
    /// stored state instead of resetting it to idle. The read is a single grain RPC and may
    /// observe a transient state while a bootstrap is in progress.
    /// </summary>
    /// <param name="treeName">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeBootstrapState> GetStateAsync(string treeName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current <see cref="BootstrapCoordinatorStatus"/>
    /// for <paramref name="treeName"/>: the phase plus the source
    /// cluster id of any in-flight bootstrap. Reports
    /// <see cref="LatticeBootstrapState.Idle"/> with a
    /// <see langword="null"/>
    /// <see cref="BootstrapCoordinatorStatus.SourceClusterId"/> when
    /// no bootstrap has been started for the tree on this receiver
    /// cluster. Bootstrap phase is persisted, so an activation restart
    /// resumes from the stored state instead of resetting it to idle.
    /// The read is a single grain RPC and may observe a transient state while a bootstrap is in
    /// progress. Used by
    /// <see cref="ILatticeFallOffLogDetector.CheckAndTriggerAsync"/>
    /// to suppress duplicate alerting on probes that arrive while the
    /// coordinator is already draining a snapshot from the same
    /// source cluster.
    /// </summary>
    /// <param name="treeName">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BootstrapCoordinatorStatus> GetStatusAsync(string treeName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Bootstraps <paramref name="treeName"/> from the snapshot
    /// produced by the configured <see cref="ISnapshotProvider"/>.
    /// Drives the state machine through
    /// <see cref="LatticeBootstrapState.RequestingSnapshot"/> -
    /// <see cref="LatticeBootstrapState.ApplyingSnapshot"/> -
    /// <see cref="LatticeBootstrapState.IncrementalHandoff"/> -
    /// <see cref="LatticeBootstrapState.LiveIncremental"/>. The call
    /// persists the kickoff and schedules the background phase pump;
    /// kickoff-time failures propagate to the caller, while drain-time
    /// failures are recorded in persisted state as
    /// <see cref="LatticeBootstrapState.Failed"/> for later polling or
    /// restart.
    /// <para>
    /// Only one bootstrap may run per tree at a time, enforced
    /// cluster-wide by the underlying grain activation. A concurrent
    /// same-source invocation is an idempotent no-op; a concurrent
    /// different-source invocation throws
    /// <see cref="InvalidOperationException"/> immediately rather
    /// than queueing - including when the second invocation
    /// originates on a different silo from the first.
    /// </para>
    /// </summary>
    /// <param name="treeName">The logical tree id to bootstrap. Must be non-null and non-empty.</param>
    /// <param name="sourceClusterId">
    /// The id of the cluster the snapshot was produced on. Stamped
    /// onto every applied entry as its origin id so the per-origin
    /// HWM dedupe can recognise the snapshot/incremental boundary.
    /// Must be non-null and non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed at every state transition and on every yielded snapshot entry.</param>
    Task BootstrapAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default);
}
