namespace Orleans.Lattice.Replication;

/// <summary>
/// A point-in-time snapshot of a cross-cluster saga, read through
/// <see cref="Grains.ICrossClusterSagaCoordinatorGrain.GetStatusAsync"/> by a
/// caller that follows the saga to its end instead of awaiting it inside one
/// grain call (issue #5506).
/// </summary>
/// <param name="Phase">The coordinator's current phase.</param>
/// <param name="Outcome">
/// The recorded outcome once the global decision is made; <c>null</c> while the
/// saga is still preparing or has not started.
/// </param>
/// <param name="IsDriving">
/// <c>true</c> while the coordinator is advancing the saga in a turn of its own.
/// <c>false</c> when nothing is driving it: a caller that needs the saga to move
/// on asks for another drive.
/// </param>
/// <param name="RequiresRepair">
/// <c>true</c> when a participant refused the recorded decision, so the saga is
/// split across clusters and will not complete without operator repair.
/// </param>
/// <param name="Detail">
/// The failure detail the coordinator recorded, or the fault of its most recent
/// drive; <c>null</c> when there is none.
/// </param>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaStatus)]
[Immutable]
internal readonly record struct CrossClusterSagaStatus(
    [property: Id(0)] CrossClusterSagaPhase Phase,
    [property: Id(1)] CrossClusterSagaOutcome? Outcome,
    [property: Id(2)] bool IsDriving,
    [property: Id(3)] bool RequiresRepair,
    [property: Id(4)] string? Detail)
{
    /// <summary>
    /// <c>true</c> once the coordinator has completed the saga: the decision is
    /// recorded and every prepared participant has applied it.
    /// </summary>
    public bool IsComplete => Phase == CrossClusterSagaPhase.Completed;
}
