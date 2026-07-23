namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire response for the <c>GetReplicationConfig</c> RPC: the permission-scoped
/// set of per-tree replication config entries the caller is authorized to see.
/// Empty when the caller is authorized to see no configured tree.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationConfigResponse)]
[Immutable]
public sealed record ReplicationConfigResponse
{
    /// <summary>The per-tree replication config entries visible to the caller.</summary>
    [Id(0)] public IReadOnlyList<ReplicationTreeConfigMessage> Trees { get; init; } =
        Array.Empty<ReplicationTreeConfigMessage>();
}
