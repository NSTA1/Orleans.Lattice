using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response of the <c>GetComplianceScanStatus</c> and
/// <c>CancelComplianceScan</c> RPCs: the operation's status, or
/// <see langword="null"/> when no such operation is visible to the caller.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaComplianceOperationStatusResponse)]
[Immutable]
public sealed record SchemaComplianceOperationStatusResponse
{
    /// <summary>The status, or <see langword="null"/> when not found.</summary>
    [Id(0)] public LatticeOperationStatus? Status { get; init; }
}
