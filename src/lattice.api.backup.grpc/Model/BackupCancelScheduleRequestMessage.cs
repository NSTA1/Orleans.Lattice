using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the cancel-schedule RPC: the scope whose runtime schedule is
/// removed and the schedule kind to remove.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCancelScheduleRequestMessage)]
[Immutable]
public sealed record BackupCancelScheduleRequestMessage
{
    /// <summary>The scope whose runtime schedule should be removed.</summary>
    [Id(0)] public required BackupScopeSelector Scope { get; init; }

    /// <summary>Whether to remove the incremental schedule rather than the full schedule.</summary>
    [Id(1)] public bool Incremental { get; init; }
}
