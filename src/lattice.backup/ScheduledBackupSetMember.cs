namespace Orleans.Lattice.Backup;

/// <summary>
/// One member of a <see cref="ScheduledBackupSet"/>: the scope it captured, the
/// tree it restores, and the backup that holds it.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.ScheduledBackupSetMember)]
[Immutable]
public sealed record ScheduledBackupSetMember
{
    /// <summary>Initializes a new <see cref="ScheduledBackupSetMember"/>.</summary>
    /// <param name="scope">The (effective) scope the member captured. Must not be <c>null</c>.</param>
    /// <param name="backupId">The content-addressed id of the member backup. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    public ScheduledBackupSetMember(BackupScopeSelector scope, string backupId)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        Scope = scope;
        BackupId = backupId;
    }

    /// <summary>The (effective) scope the member captured.</summary>
    [Id(0)]
    public BackupScopeSelector Scope { get; init; }

    /// <summary>The content-addressed id of the member backup.</summary>
    [Id(1)]
    public string BackupId { get; init; }

    /// <summary>The (effective) tree the member restores.</summary>
    public string TreeId => Scope.TreeId;
}
