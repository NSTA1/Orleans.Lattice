namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire request for the <c>DisableReplication</c> RPC. Carries the target tree
/// id whose replication should be paused.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationDisableRequestMessage)]
[Immutable]
public sealed record ReplicationDisableRequestMessage
{
    /// <summary>The target tree id to disable.</summary>
    [Id(0)] public required string TreeId { get; init; }
}
