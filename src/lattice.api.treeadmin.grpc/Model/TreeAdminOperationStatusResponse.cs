using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire response of the <c>GetTreeAdminOperationStatus</c> and
/// <c>CancelTreeAdminOperation</c> RPCs: the operation's status, or
/// <see langword="null"/> when no such operation is visible to the caller.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminOperationStatusResponse)]
[Immutable]
public sealed record TreeAdminOperationStatusResponse
{
    /// <summary>The status, or <see langword="null"/> when not found.</summary>
    [Id(0)] public LatticeOperationStatus? Status { get; init; }
}
