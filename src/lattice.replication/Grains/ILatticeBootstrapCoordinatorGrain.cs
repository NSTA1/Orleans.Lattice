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
    [Orleans.Concurrency.AlwaysInterleave]
    Task<LatticeBootstrapState> GetStateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current <see cref="BootstrapCoordinatorStatus"/>:
    /// the phase plus the persisted
    /// <see cref="BootstrapCoordinatorState.SourceClusterId"/>
    /// projected as <see langword="null"/> when no bootstrap is in
    /// flight (i.e. when
    /// <see cref="BootstrapCoordinatorState.InProgress"/> is
    /// <see langword="false"/> or the persisted source string is
    /// empty). Surfaces the in-progress source identity to the
    /// receiver-side fall-off detector so it can absorb duplicate
    /// probes the coordinator would otherwise quietly no-op. Also carries the
    /// read-fence and drain-progress fields an operator watches during a drain
    /// (issue #4526), so it interleaves with a running drain rather than
    /// waiting behind it.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<BootstrapCoordinatorStatus> GetStatusAsync(CancellationToken cancellationToken = default);

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
    /// every applied entry so the pinned causal frontier
    /// recognises the snapshot/incremental boundary. Must be non-null and
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

    /// <summary>
    /// Returns the export epoch of the last full bootstrap from
    /// <paramref name="sourceClusterId"/> that reached
    /// <see cref="LatticeBootstrapState.LiveIncremental"/>, or
    /// <see langword="null"/> when none has (issue #4534). Interleaves with a
    /// running bootstrap so the receive path never queues behind one.
    /// </summary>
    /// <param name="sourceClusterId">The sending cluster.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<long?> GetCompletedExportEpochAsync(string sourceClusterId);

    /// <summary>
    /// Operator override (issue #4526): lifts the read fence a failed bootstrap
    /// left up over a partial import and stops its automatic re-drive, so reads
    /// may observe the partial import until a later bootstrap completes. Refused
    /// with <see cref="InvalidOperationException"/> while a drain is running.
    /// Fails closed: a shard that cannot be lifted leaves the fence recorded as
    /// armed and the call throws.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when a fence was lifted; <see langword="false"/> when none was armed.</returns>
    Task<bool> ForceLiftReadFenceAsync(CancellationToken cancellationToken = default);
}
