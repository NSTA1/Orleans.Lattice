namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the per-tree configuration lifecycle RPC: the tree id to
/// configure and the partial <see cref="TreeConfigurationUpdate"/> to apply. The
/// nested update carries its own apply-flags so a single dimension can be changed
/// without disturbing the others.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminSetConfigRequest)]
[Immutable]
public sealed record TreeAdminSetConfigRequest
{
    /// <summary>The tree id to configure.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The partial per-tree configuration update to apply.</summary>
    [Id(1)] public required TreeConfigurationUpdate Update { get; init; }
}
