using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response of the <c>GetBackupOperationStatus</c> and
/// <c>CancelBackupOperation</c> RPCs: the operation's status, or
/// <see langword="null"/> when no such operation is visible to the caller.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupOperationStatusResponse)]
[Immutable]
public sealed record BackupOperationStatusResponse
{
    /// <summary>The status, or <see langword="null"/> when not found.</summary>
    [Id(0)] public LatticeOperationStatus? Status { get; init; }
}
