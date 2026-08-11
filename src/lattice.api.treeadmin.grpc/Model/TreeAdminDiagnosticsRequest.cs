namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying a tree id and a <see cref="Deep"/> flag, used by the
/// tree-administration control-API diagnostics RPC that can run either the cheap
/// shard-root projection (the default) or an expensive leaf-walk.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminDiagnosticsRequest)]
[Immutable]
public sealed record TreeAdminDiagnosticsRequest
{
    /// <summary>The tree id the call targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// When <see langword="true"/>, walk leaf state for authoritative counts;
    /// otherwise use the cheap shard-root projection.
    /// </summary>
    [Id(1)] public bool Deep { get; init; }
}
