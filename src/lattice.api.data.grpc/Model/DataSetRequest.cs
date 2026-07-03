namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the point-set RPC. A serializable mirror of the
/// <c>(treeId, key, value)</c> argument triple routed onto
/// <c>ILatticeDataApi.SetAsync</c>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetRequest)]
public sealed record DataSetRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The entry key to write.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>The value bytes to store.</summary>
    [Id(2)] public byte[] Value { get; init; } = Array.Empty<byte>();
}
