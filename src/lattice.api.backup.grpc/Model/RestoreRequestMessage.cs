using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the restore RPC. A serializable mirror of
/// <see cref="LatticeRestoreRequest"/>: the <see cref="BackupId"/> to restore
/// to, an optional <see cref="TargetTreeId"/> and sub-region <see cref="Scope"/>,
/// the restore <see cref="Mode"/>, an optional idempotency
/// <see cref="OperationId"/>, and the per-round-trip <see cref="ApplyBatchSize"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.RestoreRequestMessage)]
[Immutable]
public sealed record RestoreRequestMessage
{
    /// <summary>The content-addressed id of the backup to restore to.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>The tree to restore into, or <see langword="null"/> to restore into the captured tree.</summary>
    [Id(1)] public string? TargetTreeId { get; init; }

    /// <summary>The sub-region of the backup to restore, or <see langword="null"/> for the whole captured scope.</summary>
    [Id(2)] public BackupScopeSelector? Scope { get; init; }

    /// <summary>The restore mode.</summary>
    [Id(3)] public LatticeRestoreMode Mode { get; init; } = LatticeRestoreMode.InPlace;

    /// <summary>The idempotency key, or <see langword="null"/> to derive one from the request.</summary>
    [Id(4)] public string? OperationId { get; init; }

    /// <summary>The maximum number of entries applied to a single shard per round-trip.</summary>
    [Id(5)] public int ApplyBatchSize { get; init; } = LatticeRestoreRequest.DefaultApplyBatchSize;
}
