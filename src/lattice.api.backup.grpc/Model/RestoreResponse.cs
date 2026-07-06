using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the restore RPC, and the request payload for the revert
/// RPC. A serializable mirror of <see cref="LatticeRestoreResult"/> (a plain,
/// non-serializable facade record) that carries every field needed to reconstruct
/// the result - so a client can hand it straight back to
/// <c>RevertRestore</c> to undo a shadow-cutover restore.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.RestoreResponse)]
[Immutable]
public sealed record RestoreResponse
{
    /// <summary>The backup id restored.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>The tree restored into.</summary>
    [Id(1)] public required string TargetTreeId { get; init; }

    /// <summary>The restore mode applied.</summary>
    [Id(2)] public LatticeRestoreMode Mode { get; init; }

    /// <summary>The resolved idempotency key.</summary>
    [Id(3)] public required string OperationId { get; init; }

    /// <summary>The base-first ordered chain of backup ids that were replayed.</summary>
    [Id(4)] public IReadOnlyList<string> ManifestChain { get; init; } = Array.Empty<string>();

    /// <summary>The number of entries installed.</summary>
    [Id(5)] public long EntriesApplied { get; init; }

    /// <summary>The physical tree id the alias now resolves to (shadow-cutover only).</summary>
    [Id(6)] public string? ShadowPhysicalTreeId { get; init; }

    /// <summary>The physical tree id retained for revert (shadow-cutover only).</summary>
    [Id(7)] public string? PreviousPhysicalTreeId { get; init; }
}
