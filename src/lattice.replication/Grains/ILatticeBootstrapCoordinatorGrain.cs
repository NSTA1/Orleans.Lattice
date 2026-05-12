namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree bootstrap coordinator grain. Cluster-wide single
/// activation per tree id is provided by Orleans' grain placement,
/// so concurrent bootstraps for the same tree across silos all route
/// to one activation and the in-progress gate inside the grain
/// becomes the cluster-wide mutual exclusion primitive - no
/// distributed lock or external coordination is required.
/// <para>
/// Grain key format: <c>{treeName}</c>. The public
/// <see cref="ILatticeBootstrapCoordinator"/> façade resolves this
/// grain by tree name and forwards every call; callers never observe
/// the grain interface directly because it is <c>internal</c>.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.ILatticeBootstrapCoordinatorGrain)]
internal interface ILatticeBootstrapCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the current <see cref="LatticeBootstrapState"/>. A
    /// freshly-activated grain reports
    /// <see cref="LatticeBootstrapState.Idle"/>; the field lives
    /// in-memory only, so a silo restart resets every tree's state
    /// to <see cref="LatticeBootstrapState.Idle"/> until the next
    /// <see cref="BootstrapAsync"/> call.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeBootstrapState> GetStateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Drives the bootstrap state machine through
    /// <see cref="LatticeBootstrapState.RequestingSnapshot"/> →
    /// <see cref="LatticeBootstrapState.ApplyingSnapshot"/> →
    /// <see cref="LatticeBootstrapState.IncrementalHandoff"/> →
    /// <see cref="LatticeBootstrapState.LiveIncremental"/>. On any
    /// thrown exception the state transitions to
    /// <see cref="LatticeBootstrapState.Failed"/> and the exception
    /// propagates; a subsequent call restarts the cycle.
    /// </summary>
    /// <param name="sourceClusterId">
    /// The id of the cluster that produced the snapshot. Stamped onto
    /// every applied entry so the per-origin HWM dedupe recognises
    /// the snapshot/incremental boundary. Must be non-null and
    /// non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed at every state transition and on every yielded snapshot entry.</param>
    /// <exception cref="InvalidOperationException">
    /// A bootstrap is already in progress on this activation. Raised
    /// fast (without queueing) so concurrent operator-driven and
    /// auto-bootstrap triggers across silos surface as an immediate
    /// error rather than a hung second call.
    /// </exception>
    Task BootstrapAsync(string sourceClusterId, CancellationToken cancellationToken = default);
}
