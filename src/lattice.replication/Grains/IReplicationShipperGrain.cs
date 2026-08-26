using Orleans.Lattice.BPlusTree.Grains;
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
/// IWalCursorRegistry.ReportCursorAsync.
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
    /// ShardedReplogSink just appended a fresh WalRecord. The
    /// shipper short-circuits its next steady-state timer wait and
    /// pumps immediately. Idempotent and non-blocking.
    /// </summary>
    Task OnDoorbellAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Durably pauses this shipper for the cross-cluster saga
    /// <paramref name="sagaId"/>. While paused the pump tick short-circuits
    /// before any send, so no post-cut entry leaves the cluster for the saga's
    /// duration; the durable ship cursor is never advanced, so shipping resumes
    /// from the same resume point when the pause is lifted. Idempotent: a
    /// re-pause for the same saga is a no-op; a pause for a different saga while
    /// one is already engaged overwrites the owner (last engage wins) so a
    /// re-issued cutover can re-take a stale pause.
    /// </summary>
    /// <param name="sagaId">Identifier of the saga engaging the pause.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PauseShippingAsync(string sagaId, CancellationToken cancellationToken);

    /// <summary>
    /// Lifts a shipping pause previously engaged for <paramref name="sagaId"/>
    /// and immediately re-arms the pump. Idempotent: resuming an un-paused
    /// shipper, or one paused by a different saga, is a no-op so a late resume
    /// cannot clear a newer pause.
    /// </summary>
    /// <param name="sagaId">Identifier of the saga whose pause to lift.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ResumeShippingAsync(string sagaId, CancellationToken cancellationToken);

    /// <summary>
    /// Reports whether this shipper is currently administratively paused for a
    /// saga cutover. Exposed for diagnostics and tests.
    /// </summary>
    Task<bool> IsShippingPausedAsync();

    /// <summary>
    /// Event-driven notification that the tree registry has swapped the source
    /// tree's logical alias to a new physical identity
    /// <paramref name="newPhysicalTreeId"/> (shadow-cutover restore, resize,
    /// reshard). The shipper rebinds to the new physical WAL immediately - resets
    /// its per-partition resume cursors and drops cached shard-grain references so
    /// its next pump tick tails the new source log from its start - without
    /// reading the registry. This is the primary rebind path; the shipper's
    /// backstop re-resolve
    /// (<see cref="LatticeReplicationOptions.ShipSourceIdentityBackstopInterval"/>)
    /// only heals a lost notification. Idempotent: a notification whose physical
    /// id equals the current binding is a no-op.
    /// </summary>
    /// <param name="newPhysicalTreeId">The source tree's new physical identity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task NotifySourceIdentityChangedAsync(string newPhysicalTreeId, CancellationToken cancellationToken);
}