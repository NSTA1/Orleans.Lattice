namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the configure-backup-health RPC: the per-backup
/// health-monitor override to persist - whether the periodic monitor verifies the
/// backup and the minimum interval between verifications (in ticks so the
/// <see cref="TimeSpan"/> crosses the wire as a stable integer).
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthConfigureRequestMessage)]
[Immutable]
public sealed record BackupHealthConfigureRequestMessage
{
    /// <summary>The backup id to configure.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>Whether the periodic monitor verifies this backup.</summary>
    [Id(1)] public bool MonitoringEnabled { get; init; }

    /// <summary>The minimum interval between verifications of this backup, in ticks.</summary>
    [Id(2)] public long IntervalTicks { get; init; }
}
