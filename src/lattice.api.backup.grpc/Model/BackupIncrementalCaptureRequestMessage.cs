using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the incremental-capture RPC. A serializable mirror of
/// <see cref="LatticeBackupIncrementalCaptureRequest"/>: the same shape as
/// <see cref="BackupCaptureRequestMessage"/> plus the
/// <see cref="BaseBackupId"/> of the backup this increment is layered on.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupIncrementalCaptureRequestMessage)]
[Immutable]
public sealed record BackupIncrementalCaptureRequestMessage
{
    /// <summary>The human-readable backup name recorded on the manifest.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>The region of the tree the backup captures.</summary>
    [Id(1)] public required BackupScopeSelector Scope { get; init; }

    /// <summary>The id of the base backup this increment is layered on.</summary>
    [Id(2)] public required string BaseBackupId { get; init; }

    /// <summary>
    /// The number of raw entries to drain from the snapshot cursor per
    /// round-trip. Defaults to <see cref="LatticeBackupCaptureRequest.DefaultPageSize"/>.
    /// </summary>
    [Id(3)] public int PageSize { get; init; } = LatticeBackupCaptureRequest.DefaultPageSize;
}
