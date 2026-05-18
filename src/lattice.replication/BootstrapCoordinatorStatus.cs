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
    [property: Id(1)] string? SourceClusterId);
