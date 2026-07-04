namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the point-delete RPC. A serializable mirror of the
/// <c>(treeId, key)</c> argument pair routed onto
/// <c>ILatticeDataApi.DeleteAsync</c>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataDeleteRequest)]
[Immutable]
public sealed record DataDeleteRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The entry key to delete.</summary>
    [Id(1)] public required string Key { get; init; }
}
