using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the describe-backup RPC. A serializable envelope over the
/// nullable <see cref="BackupChainDescription"/> the facade returns:
/// <see cref="Found"/> is <see langword="false"/> (and <see cref="Manifest"/> /
/// <see cref="ChainBackupIds"/> are empty) when no backup with the requested id
/// exists, distinguishing an absent backup from one with an empty chain.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupChainResponse)]
[Immutable]
public sealed record BackupChainResponse
{
    /// <summary>Whether a backup with the requested id was found.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>The described backup's manifest when <see cref="Found"/>; otherwise <see langword="null"/>.</summary>
    [Id(1)] public BackupManifest? Manifest { get; init; }

    /// <summary>
    /// The base-first ordered chain of backup ids replayed to restore the
    /// backup, ending with the described backup's own id. Empty when not found.
    /// </summary>
    [Id(2)] public IReadOnlyList<string> ChainBackupIds { get; init; } = Array.Empty<string>();
}
