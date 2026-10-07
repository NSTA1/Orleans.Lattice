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
    /// Re-enters the normal full bootstrap path when a prior delete reconcile
    /// skipped on an unstable source generation and recorded durable owed work.
    /// No-op when no owed retry exists for <paramref name="sourceClusterId"/>.
    /// </summary>
    Task RetryOwedReconcileAsync(string sourceClusterId, CancellationToken cancellationToken = default);

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
    /// Records that <paramref name="sourceClusterId"/> withholds saga records
    /// until this receiver re-seeds from an export after
    /// <paramref name="reseedAfterEpoch"/> (issues #4533 / #4534), then, when
    /// <paramref name="start"/> is set, starts a bootstrap from it unless one
    /// from the same source is already running. With automatic bootstrap off
    /// the request is still recorded, so the operator's bootstrap consumes it.
    /// A drain whose export is after the recorded epoch also clears the
    /// receiver's stale pending buckets from that source: each one whose saga
    /// the export carries neither as a prepared row nor as a decision row,
    /// which the source therefore decided and purged, is decided aborted
    /// locally. Its committed values, if any, arrived as committed rows.
    /// </summary>
    /// <param name="sourceClusterId">The re-seeding sender's cluster id. Must be non-null and non-empty.</param>
    /// <param name="reseedAfterEpoch">The sender's recorded re-seed epoch.</param>
    /// <param name="start">Whether to start the bootstrap, or only record the request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="InvalidOperationException">A bootstrap from a different source is running.</exception>
    Task BootstrapForReseedAsync(string sourceClusterId, long reseedAfterEpoch, bool start, CancellationToken cancellationToken = default);

    /// <summary>
    /// <see langword="true"/> while a re-seed request from
    /// <paramref name="sourceClusterId"/> is recorded and no drain has
    /// consumed it - from the request until the drain's stale-bucket clear has
    /// finished (issue #4533). The sender withholds every saga record for that
    /// whole window, so a saga record from it that arrives meanwhile is a
    /// straggler pushed before its re-seed marker, and the receiver refuses it.
    /// </summary>
    /// <param name="sourceClusterId">The sender's cluster id.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<bool> IsReseedPendingAsync(string sourceClusterId);

    /// <summary>
    /// The source lineage of the last whole-tree export from
    /// <paramref name="sourceClusterId"/> this receiver began draining, with the
    /// tree frontier epoch it began in, or <see langword="null"/> when none was
    /// recorded (issue #4673). A pushed batch the source stamped with another
    /// lineage, or arriving once the frontier epoch has moved on, is refused.
    /// </summary>
    /// <param name="sourceClusterId">The sender's cluster id.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<ReplicationDrainedLineage?> GetDrainedLineageAsync(string sourceClusterId);

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
    /// Returns the export epoch of the last snapshot from
    /// <paramref name="sourceClusterId"/> whose drain applied every entry -
    /// including one whose bootstrap still holds its read fence - or
    /// <see langword="null"/> when none has (issue #4684). A drain records the
    /// arrival of every cross-tree sub-saga it settles at its barrier before it
    /// ends, so a sibling import that waits on this one is served by the drain,
    /// not by the fence lifting: two imports that wait on each other's
    /// completion would never complete.
    /// </summary>
    /// <param name="sourceClusterId">The sending cluster.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<long?> GetDrainedExportEpochAsync(string sourceClusterId);

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

    /// <summary>
    /// Called by a decommission of <paramref name="sourceClusterId"/> before it
    /// abandons that source's cross-tree barriers (issue #4742). When this
    /// tree's import from that source still holds its cross-tree read fence,
    /// durably records that the fence must stay up until the source is re-added:
    /// an abandoned barrier no longer counts as undecided, so without this the
    /// next phase tick would lift the fence over an import its siblings never
    /// matched. Returns whether the fence is now held for the decommission.
    /// </summary>
    /// <param name="sourceClusterId">The decommissioned source cluster.</param>
    Task<bool> HoldFenceForDecommissionedSourceAsync(string sourceClusterId);

    /// <summary>
    /// Called when <paramref name="sourceClusterId"/> is re-added after a
    /// decommission (issue #4742). When this tree's import from that source is
    /// holding its fence for the decommission, re-drives the import from a
    /// fresh export with the fence still armed, so the fresh drain decides
    /// whether the fence lifts. Returns whether a re-drive was started.
    /// </summary>
    /// <param name="sourceClusterId">The re-added source cluster.</param>
    Task<bool> RedriveForReAddedSourceAsync(string sourceClusterId);
}
