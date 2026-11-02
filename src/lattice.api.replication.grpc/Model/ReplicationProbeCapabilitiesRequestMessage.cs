namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire request for the <c>ProbeCapabilities</c> RPC. Carries the tree id whose
/// replication capability the caller is asking about.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationProbeCapabilitiesRequestMessage)]
[Immutable]
public sealed record ReplicationProbeCapabilitiesRequestMessage
{
    /// <summary>The tree id to probe.</summary>
    [Id(0)] public required string TreeId { get; init; }
}
