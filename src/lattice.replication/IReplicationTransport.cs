namespace Orleans.Lattice.Replication;

/// <summary>
/// Pluggable seam for shipping replication payloads between clusters.
/// Implementations frame the supplied <see cref="ReplicationBatch"/> onto
/// a concrete wire (HTTP, gRPC streaming, in-process loopback, custom
/// transport), deliver it to the receiving cluster, and return a
/// <see cref="ReplicationAck"/> describing the receiver-side
/// high-water-mark the sender should advance its per-peer cursor to.
/// <para>
/// The default DI registration is a no-op transport so the rest of the
/// replication pipeline can be wired up in isolation; production hosts
/// replace it via
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// (for example, a binary-framed gRPC streaming push transport).
/// </para>
/// <para>Implementations are expected to:</para>
/// <list type="bullet">
///   <item>
///     <description>
///       Validate that <see cref="ReplicationBatch.TargetClusterId"/>,
///       <see cref="ReplicationBatch.TreeName"/>, and
///       <see cref="ReplicationBatch.OriginClusterId"/> are non-empty,
///       throwing <see cref="ArgumentException"/> when they are not.
///     </description>
///   </item>
///   <item>
///     <description>
///       Be idempotent at the batch boundary. Receivers de-duplicate
///       re-deliveries by the per-origin
///       <c>(TreeName, OriginClusterId, hlc)</c> high-water-mark, so a
///       transport that retries a batch on transient failure must not
///       cause double-apply.
///     </description>
///   </item>
///   <item>
///     <description>
///       Be safe for concurrent invocation across distinct
///       <c>(TargetClusterId, TreeName)</c> pairs, and - when used with
///       sender-side pipelining
///       (<see cref="LatticeReplicationOptions.ShipMaxInFlight"/> &gt; 1) -
///       for concurrent invocation against the same pair. The shipper
///       consumes acks in FIFO order and never relies on the transport
///       preserving wire ordering, but it does keep up to
///       <see cref="LatticeReplicationOptions.ShipMaxInFlight"/> calls
///       outstanding per pair; a transport that cannot tolerate that
///       must be paired with the default window of <c>1</c>, under which
///       the shipper serialises calls per pair.
///     </description>
///   </item>
/// </list>
/// </summary>
public interface IReplicationTransport
{
    /// <summary>
    /// Sends a replication payload to the cluster identified by
    /// <see cref="ReplicationBatch.TargetClusterId"/> and returns the
    /// receiver-side acknowledgement. The sender advances its per-peer
    /// cursor strictly to <see cref="ReplicationAck.HighestAppliedHlc"/>
    /// when <see cref="ReplicationAck.Accepted"/> is <see langword="true"/>;
    /// otherwise the cursor stays put and the sender retries.
    /// </summary>
    /// <param name="batch">Routing metadata and opaque framed payload.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken);
}
