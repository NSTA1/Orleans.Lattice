namespace Orleans.Lattice.Replication;

/// <summary>
/// Observable status snapshot of the receiver-side bootstrap
/// coordinator for a single tree. Returned by
/// <see cref="ILatticeBootstrapCoordinator.GetStatusAsync"/>.
/// <para>
/// Distinct from <see cref="LatticeBootstrapState"/> in that it also
/// carries the <see cref="SourceClusterId"/> of any in-flight
/// bootstrap, so callers (notably
/// <see cref="ILatticeFallOffLogDetector"/>) can distinguish "no
/// bootstrap in flight" from "bootstrap already in flight from the
/// same source cluster" without consulting the internal grain state
/// directly.
/// </para>
/// </summary>
/// <param name="Phase">
/// The current observable phase of the bootstrap. Reports
/// <see cref="LatticeBootstrapState.Idle"/> when no bootstrap has
/// been started for the tree on the receiver cluster (or when the
/// silo hosting the activation restarted and the in-memory state
/// reset).
/// </param>
/// <param name="SourceClusterId">
/// The id of the cluster the in-flight bootstrap is draining from,
/// or <see langword="null"/> when no bootstrap is in flight. Empty
/// string is normalised to <see langword="null"/> by the coordinator
/// façade.
/// </param>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.BootstrapCoordinatorStatus)]
[Immutable]
public readonly record struct BootstrapCoordinatorStatus(
    [property: Id(0)] LatticeBootstrapState Phase,
    [property: Id(1)] string? SourceClusterId)
{
    /// <summary>
    /// Whether the tree's reads are refused because a snapshot drain is applying
    /// an import, or a failed drain left a partial import behind (issue #4526).
    /// While <see langword="true"/>, reads of the tree throw
    /// <see cref="LatticeTreeBootstrappingException"/>.
    /// </summary>
    [Id(2)] public bool ReadFenced { get; init; }

    /// <summary>
    /// Snapshot entries the current drain attempt has applied so far. Progress
    /// reporting for an operator watching a drain; reset when an attempt starts.
    /// </summary>
    [Id(3)] public long EntriesApplied { get; init; }

    /// <summary>
    /// Automatic re-drives of a bootstrap that failed after applying part of an
    /// import. Non-zero means the tree has been held read-fenced across at least
    /// one failed attempt.
    /// </summary>
    [Id(4)] public int RedriveAttempts { get; init; }
}
