using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the full- and incremental-capture RPCs. A serializable
/// mirror of <see cref="LatticeBackupCaptureResult"/> (a plain,
/// non-serializable facade record): the content-addressed
/// <see cref="BackupId"/> and the captured <see cref="Manifest"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCaptureResponse)]
[Immutable]
public sealed record BackupCaptureResponse
{
    /// <summary>The content-addressed backup id.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>The manifest written for the backup.</summary>
    [Id(1)] public required BackupManifest Manifest { get; init; }
}
