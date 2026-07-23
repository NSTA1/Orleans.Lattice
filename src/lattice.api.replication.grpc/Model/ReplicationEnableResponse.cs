using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire response for the <c>EnableReplication</c> RPC. Reports the fixed merge
/// mode the tree is now enabled under, whether the request was an idempotent
/// no-op because the tree was already enabled under the same mode, and whether a
/// snapshot bootstrap was requested to seed a peer with the tree's pre-existing
/// data.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationEnableResponse)]
[Immutable]
public sealed record ReplicationEnableResponse
{
    /// <summary>The target tree id the enable was authored for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The wire merge mode the tree is enabled under.</summary>
    [Id(1)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree was already enabled under
    /// <see cref="Mode"/> and the call was an idempotent no-op.
    /// </summary>
    [Id(2)] public bool AlreadyEnabled { get; init; }

    /// <summary>
    /// <see langword="true"/> when the engine requested a snapshot bootstrap so a
    /// peer converges on the tree's pre-existing data.
    /// </summary>
    [Id(3)] public bool BootstrapRequested { get; init; }
}
