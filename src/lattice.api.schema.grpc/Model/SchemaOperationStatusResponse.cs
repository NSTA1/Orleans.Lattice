using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response of the <c>GetSchemaOperationStatus</c> and
/// <c>CancelSchemaOperation</c> RPCs: the operation's status, or
/// <see langword="null"/> when no such operation is visible to the caller.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaOperationStatusResponse)]
[Immutable]
public sealed record SchemaOperationStatusResponse
{
    /// <summary>The status, or <see langword="null"/> when not found.</summary>
    [Id(0)] public LatticeOperationStatus? Status { get; init; }
}
