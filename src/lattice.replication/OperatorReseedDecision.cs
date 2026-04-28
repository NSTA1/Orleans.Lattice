namespace Orleans.Lattice.Replication;

/// <summary>
/// Result of a single
/// <see cref="ILatticeReplicationAdmin.RequestSnapshotAsync"/> call.
/// Diagnostic only; the durable side effect of an honoured request
/// is the bootstrap coordinator kickoff that propagates through the
/// receiver-side state machine.
/// </summary>
/// <param name="Triggered">
/// <see langword="true"/> when the admin seam invoked
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> as a
/// result of the call. <see langword="false"/> when the request was
/// rejected by the per-<c>(tree, sourceClusterId)</c> rate limit
/// (governed by
/// <see cref="LatticeReplicationOptions.OperatorReseedMinInterval"/>)
/// because a previous request was honoured too recently.
/// </param>
/// <param name="LastRequestedAt">
/// The wall-clock timestamp of the last honoured request observed
/// by the admin seam for this <c>(tree, sourceClusterId)</c> pair, or
/// <see langword="null"/> when no prior request has been honoured
/// since the seam was activated. The default admin implementation
/// stores these timestamps in process memory only - a silo restart
/// resets the rate-limit window for every pair.
/// </param>
/// <param name="RetryAfter">
/// When <see cref="Triggered"/> is <see langword="false"/>, the
/// remaining time before another request for the same
/// <c>(tree, sourceClusterId)</c> pair would be honoured under the
/// configured
/// <see cref="LatticeReplicationOptions.OperatorReseedMinInterval"/>.
/// <see langword="null"/> when <see cref="Triggered"/> is
/// <see langword="true"/>.
/// </param>
public readonly record struct OperatorReseedDecision(
    bool Triggered,
    DateTimeOffset? LastRequestedAt,
    TimeSpan? RetryAfter);
