using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response wrapping a <see cref="LatticeSchemaComplianceReport"/> value
/// type for the <c>ScanCompliance</c> RPC (gRPC message types must be reference
/// types).
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaComplianceReportResponse)]
[Immutable]
public sealed record SchemaComplianceReportResponse
{
    /// <summary>The compliance audit report.</summary>
    [Id(0)] public required LatticeSchemaComplianceReport Report { get; init; }
}
