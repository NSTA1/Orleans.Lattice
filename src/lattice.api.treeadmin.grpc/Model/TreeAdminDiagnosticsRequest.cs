namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying a tree id and a <see cref="Deep"/> flag, used by the
/// tree-administration control-API diagnostics RPC, which pages through every
/// shard's leaf chain counting either live keys only (the default) or live keys plus
/// tombstoned and expired entries.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminDiagnosticsRequest)]
[Immutable]
public sealed record TreeAdminDiagnosticsRequest
{
    /// <summary>The tree id the call targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// When <see langword="true"/>, each leaf also counts its tombstoned and expired
    /// entries; otherwise the walk counts live keys only.
    /// </summary>
    [Id(1)] public bool Deep { get; init; }
}
