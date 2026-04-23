namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// Cluster-wide singleton grain that aggregates replication activity
/// counters and timestamps for the local cluster. Grain key is the
/// cluster name (e.g. <c>"forge"</c>), so both silos in the cluster
/// route to the same activation — a UI tab on silo A sees ships
/// performed by silo B in real time, and the numbers survive a full
/// cluster restart (persisted to <c>msmfgGrainState</c>).
/// </summary>
/// <remarks>
/// Write-heavy paths (<see cref="RecordSentAsync"/>,
/// <see cref="RecordReceivedAsync"/>) are invoked fire-and-forget
/// from the HTTP client and inbound endpoint, so a slow storage write
/// never back-pressures the replication path.
/// </remarks>
internal interface IClusterReplicationStatsGrain : IGrainWithStringKey
{
    /// <summary>Records a successful outbound batch ship.</summary>
    Task RecordSentAsync(string peer, int rows);

    /// <summary>Records a failed outbound batch ship.</summary>
    Task RecordSendErrorAsync();

    /// <summary>Records an applied inbound batch.</summary>
    Task RecordReceivedAsync(string peer, int rows);

    /// <summary>Returns an immutable snapshot for UI rendering.</summary>
    Task<ReplicationActivitySnapshot> GetSnapshotAsync();
}
