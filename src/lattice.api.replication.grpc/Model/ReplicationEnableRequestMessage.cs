using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire request for the <c>EnableReplication</c> RPC. Carries the target tree
/// id, the wire merge mode to fix for the tree when first enabled, and an
/// optional bootstrap source cluster id used to seed a peer with the tree's
/// pre-existing data.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationEnableRequestMessage)]
[Immutable]
public sealed record ReplicationEnableRequestMessage
{
    /// <summary>The target tree id to enable.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The wire merge mode to fix for the tree when first enabled.</summary>
    [Id(1)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// Optional id of the cluster to pull an initial snapshot from when the tree
    /// already holds data. <see langword="null"/> or empty skips the bootstrap.
    /// </summary>
    [Id(2)] public string? BootstrapSourceClusterId { get; init; }
}
