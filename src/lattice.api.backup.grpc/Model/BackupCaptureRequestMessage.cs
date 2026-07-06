using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the full-capture RPC. A serializable mirror of
/// <see cref="LatticeBackupCaptureRequest"/> (which is a plain, non-serializable
/// facade record): the human-readable <see cref="Name"/>, the
/// <see cref="Scope"/> to capture, and the raw-entry drain
/// <see cref="PageSize"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCaptureRequestMessage)]
[Immutable]
public sealed record BackupCaptureRequestMessage
{
    /// <summary>The human-readable backup name recorded on the manifest.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>The region of the tree the backup captures.</summary>
    [Id(1)] public required BackupScopeSelector Scope { get; init; }

    /// <summary>
    /// The number of raw entries to drain from the snapshot cursor per
    /// round-trip. Defaults to <see cref="LatticeBackupCaptureRequest.DefaultPageSize"/>.
    /// </summary>
    [Id(2)] public int PageSize { get; init; } = LatticeBackupCaptureRequest.DefaultPageSize;
}
