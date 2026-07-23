namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire response for the <c>DisableReplication</c> RPC. Reports the target tree
/// id and whether the tree was already disabled (an idempotent no-op).
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationDisableResponse)]
[Immutable]
public sealed record ReplicationDisableResponse
{
    /// <summary>The target tree id the disable was authored for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree was already disabled (or was never
    /// configured) and the call was an idempotent no-op.
    /// </summary>
    [Id(1)] public bool AlreadyDisabled { get; init; }
}
