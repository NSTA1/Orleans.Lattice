using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the schedule-backup RPC: the scope to schedule, whether each
/// scheduled cycle captures an incremental (rather than full) backup, and the
/// cadence between captures expressed as <see cref="System.DateTime.Ticks"/> so
/// the interval survives the wire without a <c>TimeSpan</c> surrogate.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupScheduleRequestMessage)]
[Immutable]
public sealed record BackupScheduleRequestMessage
{
    /// <summary>The scope each scheduled cycle captures.</summary>
    [Id(0)] public required BackupScopeSelector Scope { get; init; }

    /// <summary>Whether each scheduled cycle captures an incremental backup rather than a full one.</summary>
    [Id(1)] public bool Incremental { get; init; }

    /// <summary>The cadence between scheduled captures, in <see cref="System.DateTime.Ticks"/>.</summary>
    [Id(2)] public long IntervalTicks { get; init; }
}
