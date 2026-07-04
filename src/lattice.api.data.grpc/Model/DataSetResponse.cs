namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the point-set RPC. Set has no payload result; an empty
/// success response acknowledges the write committed. Denial and failure are
/// carried out-of-band as a gRPC status, not as a field on this record.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetResponse)]
[Immutable]
public sealed record DataSetResponse
{
}
