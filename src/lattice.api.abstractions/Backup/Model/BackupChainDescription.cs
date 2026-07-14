using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Describes a single backup and its restore chain: the target backup's
/// <see cref="Manifest"/> plus the base-first ordered list of backup ids that
/// must be replayed to restore it (<see cref="ChainBackupIds"/>). For a full
/// backup the chain is a single element (the backup itself); for an incremental
/// it is the full base backup followed by each increment up to and including the
/// described backup, walked from <see cref="BackupManifest.BaseBackupId"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiBackupTypeAliases.BackupChainDescription)]
[Immutable]
public sealed record BackupChainDescription
{
    /// <summary>Initializes a new <see cref="BackupChainDescription"/>.</summary>
    /// <param name="manifest">The described backup's manifest. Must not be <c>null</c>.</param>
    /// <param name="chainBackupIds">
    /// The base-first ordered chain of backup ids replayed to restore the
    /// backup, ending with the described backup's own id. Must not be
    /// <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public BackupChainDescription(BackupManifest manifest, IReadOnlyList<string> chainBackupIds)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(chainBackupIds);
        Manifest = manifest;
        ChainBackupIds = chainBackupIds;
    }

    /// <summary>The described backup's manifest.</summary>
    [Id(0)] public BackupManifest Manifest { get; init; }

    /// <summary>
    /// The base-first ordered chain of backup ids replayed to restore the
    /// backup, ending with the described backup's own id.
    /// </summary>
    [Id(1)] public IReadOnlyList<string> ChainBackupIds { get; init; }
}
