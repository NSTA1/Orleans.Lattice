namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the single-tree atomic-batch RPC. The batch has no payload
/// result; an empty success response acknowledges the batch committed
/// all-or-nothing. A denied leg aborts the whole batch and surfaces as a gRPC
/// <c>PermissionDenied</c> status rather than a field on this record.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataAtomicResponse)]
[Immutable]
public sealed record DataAtomicResponse
{
}
