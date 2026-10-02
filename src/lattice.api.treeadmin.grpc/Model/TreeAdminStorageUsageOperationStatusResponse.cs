using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire response of the <c>GetStorageUsageRefreshStatus</c> and
/// <c>CancelStorageUsageRefresh</c> RPCs: the operation's status, or
/// <see langword="null"/> when no such operation is visible to the caller.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminStorageUsageOperationStatusResponse)]
[Immutable]
public sealed record TreeAdminStorageUsageOperationStatusResponse
{
    /// <summary>The status, or <see langword="null"/> when not found.</summary>
    [Id(0)] public LatticeOperationStatus? Status { get; init; }
}