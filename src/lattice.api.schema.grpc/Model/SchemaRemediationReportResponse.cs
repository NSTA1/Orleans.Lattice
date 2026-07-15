using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response wrapping a <see cref="LatticeSchemaRemediationReport"/> value
/// type for the remediation and migration RPCs (<c>AdvanceAndMigrate</c>,
/// <c>MigrateToTargetVersion</c>, <c>Remediate</c>, <c>GetRemediationStatus</c>);
/// gRPC message types must be reference types.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaRemediationReportResponse)]
[Immutable]
public sealed record SchemaRemediationReportResponse
{
    /// <summary>The remediation / migration report.</summary>
    [Id(0)] public required LatticeSchemaRemediationReport Report { get; init; }
}
