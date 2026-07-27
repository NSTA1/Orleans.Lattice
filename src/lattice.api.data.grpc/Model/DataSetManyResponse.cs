namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the non-atomic bulk-write RPC. The batch has no payload
/// result; an empty success response acknowledges the fan-out completed. A
/// per-key partial failure or an authorization denial is carried out-of-band as
/// a gRPC status, not as a field on this record.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetManyResponse)]
[Immutable]
public sealed record DataSetManyResponse
{
}
