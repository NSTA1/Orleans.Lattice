namespace Orleans.Lattice.Replication;

/// <summary>
/// The cluster-wide version of the runtime replication-configuration tree
/// (<see cref="LatticeSystemTreeNames.ReplicationConfig"/>) that every silo's
/// <see cref="CompiledReplicationConfigSnapshotMaintainer"/> is measured against.
/// Held in memory by the <see cref="Grains.IReplicationConfigEpochGrain"/> and
/// advanced after every committed write to the config tree.
/// </summary>
/// <remarks>
/// <see cref="Incarnation"/> is minted afresh by every activation of the epoch
/// grain, so a restarted grain can never report a version a silo has already
/// compiled: a silo treats any change of incarnation as a configuration change
/// and rebuilds. A lost counter can therefore only make a silo rebuild
/// unnecessarily, never make a stale silo look current.
/// </remarks>
/// <param name="Incarnation">The identity of the epoch-grain activation that minted this epoch.</param>
/// <param name="Version">The monotonic version within <paramref name="Incarnation"/>.</param>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationConfigEpoch)]
internal readonly record struct ReplicationConfigEpoch(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] long Version);
