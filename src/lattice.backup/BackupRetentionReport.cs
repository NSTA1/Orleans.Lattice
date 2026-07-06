namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome of a retention pass over one backup scope: how many backups were
/// retained, and the ids of the backups that were pruned (their manifests and
/// artifacts removed through the sink and their catalog entries deleted). A
/// backup whose base chain is still referenced by a retained increment is never
/// pruned, so the returned counts always describe a chain-integrity-preserving
/// prune.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupRetentionReport)]
[Immutable]
public sealed record BackupRetentionReport
{
    /// <summary>Initializes a new <see cref="BackupRetentionReport"/>.</summary>
    /// <param name="retainedCount">The number of backups retained. Must not be negative.</param>
    /// <param name="prunedBackupIds">The ids of the pruned backups, in prune order. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retainedCount"/> is negative.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="prunedBackupIds"/> is <c>null</c>.</exception>
    public BackupRetentionReport(int retainedCount, IReadOnlyList<string> prunedBackupIds)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retainedCount);
        ArgumentNullException.ThrowIfNull(prunedBackupIds);
        RetainedCount = retainedCount;
        PrunedBackupIds = prunedBackupIds;
    }

    /// <summary>The number of backups retained by the pass.</summary>
    [Id(0)]
    public int RetainedCount { get; init; }

    /// <summary>The ids of the backups pruned by the pass.</summary>
    [Id(1)]
    public IReadOnlyList<string> PrunedBackupIds { get; init; }

    /// <summary>The number of backups pruned by the pass.</summary>
    public int PrunedCount => PrunedBackupIds.Count;

    /// <summary>An empty report: nothing retained, nothing pruned.</summary>
    public static BackupRetentionReport Empty { get; } = new(0, Array.Empty<string>());
}
