namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the point-get RPC. A serializable mirror of the
/// <c>(treeId, key)</c> argument pair routed onto
/// <c>ILatticeDataApi.GetAsync</c>. The response reuses the facade's
/// <see cref="DataReadResult"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataGetRequest)]
[Immutable]
public sealed record DataGetRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The entry key to read.</summary>
    [Id(1)] public required string Key { get; init; }
}
