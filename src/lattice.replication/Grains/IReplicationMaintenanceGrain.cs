namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree replication maintenance grain. Schedules the WAL garbage
/// collector and the fall-off-the-log detector for the named tree on
/// independent cadences (LatticeReplicationOptions.MaintenanceGcInterval
/// / MaintenanceFallOffCheckInterval), driving the dormant
/// LatticeReplicationMetrics.WalEntriesTrimmed and
/// LatticeReplicationMetrics.PeerFellOffLog instruments.
/// <para>
/// Grain key format: tree name verbatim. One activation per tree;
/// silo loss triggers automatic migration via the standard Orleans
/// cluster-singleton model.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationMaintenanceGrain)]
internal interface IReplicationMaintenanceGrain : IGrainWithStringKey
{
    /// <summary>
    /// Activates the grain and registers its keepalive reminder so
    /// it runs forever (until the host is shut down). Idempotent.
    /// </summary>
    Task EnsureActiveAsync(CancellationToken cancellationToken);
}