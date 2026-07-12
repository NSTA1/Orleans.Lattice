namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the schedule-backup RPC. <see cref="Scheduled"/> is
/// <see langword="true"/> once the recurring schedule reminder has been
/// registered (or updated). <see cref="EffectiveIntervalTicks"/> reports the
/// cadence actually registered in <see cref="System.DateTime.Ticks"/>, which may
/// be larger than the requested interval when the request was below the
/// scheduler minimum and was clamped up.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupScheduleResponse)]
[Immutable]
public sealed record BackupScheduleResponse
{
    /// <summary>Whether the recurring schedule was registered.</summary>
    [Id(0)] public bool Scheduled { get; init; }

    /// <summary>The cadence actually registered, in <see cref="System.DateTime.Ticks"/>.</summary>
    [Id(1)] public long EffectiveIntervalTicks { get; init; }
}
