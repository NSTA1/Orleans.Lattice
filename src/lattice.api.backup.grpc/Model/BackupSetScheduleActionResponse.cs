namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the cancel, remove and run-now backup-set-schedule RPCs.
/// <see cref="Result"/> carries the facade's boolean answer: whether the schedule
/// existed (cancel), was removed (remove), or a run was started (run-now).
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleActionResponse)]
[Immutable]
public sealed record BackupSetScheduleActionResponse
{
    /// <summary>The facade's boolean answer for the action.</summary>
    [Id(0)] public bool Result { get; init; }
}
