namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the point-delete RPC. <see cref="Removed"/> reports whether
/// a live value existed and was removed (mirroring the boolean result of
/// <c>ILatticeDataApi.DeleteAsync</c>). A denied delete never reaches this
/// response - it is carried out-of-band as a gRPC <c>PermissionDenied</c> status.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataDeleteResponse)]
[Immutable]
public sealed record DataDeleteResponse
{
    /// <summary><see langword="true"/> when a live value existed and was removed.</summary>
    [Id(0)] public bool Removed { get; init; }
}
