namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire response for the <c>ProbeCapabilities</c> RPC. Reports the effective tree id
/// the probe was evaluated over and whether the caller may configure its replication.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationCapabilitiesResponse)]
[Immutable]
public sealed record ReplicationCapabilitiesResponse
{
    /// <summary>The effective tree id the probe was evaluated over.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller holds the whole-tree
    /// <see cref="LatticeOperation.Replication"/> capability over the tree.
    /// </summary>
    [Id(1)] public bool CanConfigure { get; init; }
}
