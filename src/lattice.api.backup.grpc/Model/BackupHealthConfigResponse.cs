using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response carrying a backup's effective health-monitor configuration. An
/// unknown backup is represented by <see cref="Found"/> set to
/// <see langword="false"/> and a <see langword="null"/> <see cref="Config"/>;
/// otherwise <see cref="Config"/> is the stored override, or the deployment default
/// when none was stored.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthConfigResponse)]
[Immutable]
public sealed record BackupHealthConfigResponse
{
    /// <summary>Whether the backup exists.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>The backup's effective monitor configuration, or <see langword="null"/> when the backup does not exist.</summary>
    [Id(1)] public BackupHealthConfig? Config { get; init; }
}
