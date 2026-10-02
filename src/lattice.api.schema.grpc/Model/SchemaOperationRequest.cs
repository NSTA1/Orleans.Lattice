namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request naming one tracked schema operation, for the
/// <c>GetSchemaOperationStatus</c> and <c>CancelSchemaOperation</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaOperationRequest)]
[Immutable]
public sealed record SchemaOperationRequest
{
    /// <summary>The operation id.</summary>
    [Id(0)] public required string OperationId { get; init; }
}
