namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Empty wire acknowledgement for the schema control-API RPCs that return no
/// payload (set policy, set version config).
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaAckResponse)]
[Immutable]
public sealed record SchemaAckResponse;
