using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the scope-status RPC: the backup scope whose schedule and
/// last-run status should be described.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupScopeStatusRequestMessage)]
[Immutable]
public sealed record BackupScopeStatusRequestMessage
{
    /// <summary>The scope to describe.</summary>
    [Id(0)] public required BackupScopeSelector Scope { get; init; }
}
