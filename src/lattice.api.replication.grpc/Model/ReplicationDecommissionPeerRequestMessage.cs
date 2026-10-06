namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire request for the <c>DecommissionPeer</c> RPC. Carries the peer cluster id
/// to permanently decommission.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationDecommissionPeerRequestMessage)]
[Immutable]
public sealed record ReplicationDecommissionPeerRequestMessage
{
    /// <summary>The peer cluster id to decommission.</summary>
    [Id(0)] public required string PeerClusterId { get; init; }
}
