using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the backup-set-capture RPC. A serializable mirror of
/// <see cref="LatticeBackupSetCaptureResult"/> (a plain, non-serializable facade
/// record): the <see cref="SetManifest"/> tying the members together and the
/// per-tree <see cref="Members"/> results in scope order (each the same
/// <see cref="BackupCaptureResponse"/> the single-capture RPC returns).
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetCaptureResponse)]
[Immutable]
public sealed record BackupSetCaptureResponse
{
    /// <summary>The set manifest tying the members together.</summary>
    [Id(0)] public required BackupSetManifest SetManifest { get; init; }

    /// <summary>The per-tree member results, in scope order.</summary>
    [Id(1)] public required IReadOnlyList<BackupCaptureResponse> Members { get; init; }
}
