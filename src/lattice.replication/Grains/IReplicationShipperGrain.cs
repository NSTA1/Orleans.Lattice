namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-(tree, peer) outbound replication shipper grain. Drains the
/// per-tree change feed from the per-peer cursor, applies the
/// configured key filter and the durable origin-based cycle-break
/// (skip entries whose OriginClusterId matches the peer''s own
/// cluster id), encodes the captured entries via the registered
/// IReplicationBatchEncoder, and ships them through
/// IReplicationTransport.SendAsync. On a positive ReplicationAck
/// the shipper advances the per-peer cursor through
/// ILatticeReplicationCursorRegistry.ReportCursorAsync.
/// <para>
/// Grain key format: {treeName}/{peerClusterId}. Cluster-singleton
/// placement gives auto-migration on silo loss with no leader
/// election; per-peer back-pressure isolation falls out naturally.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationShipperGrain)]
internal interface IReplicationShipperGrain : IGrainWithStringKey
{
    /// <summary>
    /// Activates the grain and registers its keepalive reminder so
    /// it runs forever (until the host is shut down). Idempotent.
    /// </summary>
    Task EnsureActiveAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Best-effort writer-side doorbell signalling that
    /// ShardedReplogSink just appended a fresh ReplogEntry. The
    /// shipper short-circuits its next steady-state timer wait and
    /// pumps immediately. Idempotent and non-blocking.
    /// </summary>
    Task OnDoorbellAsync(CancellationToken cancellationToken);
}