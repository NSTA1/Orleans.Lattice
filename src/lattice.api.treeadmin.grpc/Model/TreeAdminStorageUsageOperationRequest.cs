namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request naming one tracked storage-usage refresh, for the
/// <c>GetStorageUsageRefreshStatus</c> and <c>CancelStorageUsageRefresh</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminStorageUsageOperationRequest)]
[Immutable]
public sealed record TreeAdminStorageUsageOperationRequest
{
    /// <summary>The operation id.</summary>
    [Id(0)] public required string OperationId { get; init; }
}