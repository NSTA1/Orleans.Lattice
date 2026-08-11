namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the tree-alias lifecycle RPC: the logical tree id to alias and
/// the physical tree id it should resolve to. Only a single level of indirection is
/// allowed - the physical target must not itself be aliased.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminSetAliasRequest)]
[Immutable]
public sealed record TreeAdminSetAliasRequest
{
    /// <summary>The logical tree id to alias.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The physical tree id the logical tree should resolve to.</summary>
    [Id(1)] public required string PhysicalTreeId { get; init; }
}
