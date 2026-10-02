namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request naming one tracked tree-administration operation, for the
/// <c>GetTreeAdminOperationStatus</c> and <c>CancelTreeAdminOperation</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminOperationRequest)]
[Immutable]
public sealed record TreeAdminOperationRequest
{
    /// <summary>The operation id.</summary>
    [Id(0)] public required string OperationId { get; init; }
}
