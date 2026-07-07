using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the backup-set-capture RPC. A serializable mirror of
/// <see cref="LatticeBackupSetCaptureRequest"/> (which is a plain,
/// non-serializable facade record): the human-readable <see cref="Name"/>, the
/// per-tree <see cref="Scopes"/> to capture, the <see cref="CrossTreeConsistent"/>
/// fence flag, and the raw-entry drain <see cref="PageSize"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetCaptureRequestMessage)]
[Immutable]
public sealed record BackupSetCaptureRequestMessage
{
    /// <summary>The human-readable set name recorded on every member and the set manifest.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>The per-tree scopes to capture, one per distinct tree.</summary>
    [Id(1)] public required IReadOnlyList<BackupScopeSelector> Scopes { get; init; }

    /// <summary>Whether to capture every tree at a single cross-tree causal fence.</summary>
    [Id(2)] public bool CrossTreeConsistent { get; init; }

    /// <summary>
    /// The number of raw entries to drain from each snapshot cursor per
    /// round-trip. Defaults to <see cref="LatticeBackupCaptureRequest.DefaultPageSize"/>.
    /// </summary>
    [Id(3)] public int PageSize { get; init; } = LatticeBackupCaptureRequest.DefaultPageSize;
}
