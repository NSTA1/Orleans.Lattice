namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the unified typed-CRDT write RPC. A CRDT write returns no
/// payload; an empty success response acknowledges the delta was authorized and
/// applied. A denial or a mode / shape fault is carried out-of-band as a gRPC
/// status, not as a field on this record.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtWriteResponse)]
[Immutable]
public sealed record CrdtWriteResponse
{
}
