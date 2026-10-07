namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire response for the <c>DecommissionPeer</c> RPC. Reports the decommissioned
/// peer cluster id, how many replicated trees its enrolment was removed from,
/// and whether the call was an idempotent no-op.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationDecommissionPeerResponse)]
[Immutable]
public sealed record ReplicationDecommissionPeerResponse
{
    /// <summary>The decommissioned peer cluster id.</summary>
    [Id(0)] public required string PeerClusterId { get; init; }

    /// <summary>The number of replicated trees whose enrolment the peer was removed from.</summary>
    [Id(1)] public int TreeCount { get; init; }

    /// <summary>
    /// <c>true</c> when the peer was already decommissioned and the call was an
    /// idempotent no-op.
    /// </summary>
    [Id(2)] public bool AlreadyDecommissioned { get; init; }
}
