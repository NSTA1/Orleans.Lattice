namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request naming one tracked compliance-scan operation, for the
/// <c>GetComplianceScanStatus</c> and <c>CancelComplianceScan</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaComplianceOperationRequest)]
[Immutable]
public sealed record SchemaComplianceOperationRequest
{
    /// <summary>The operation id.</summary>
    [Id(0)] public required string OperationId { get; init; }
}
