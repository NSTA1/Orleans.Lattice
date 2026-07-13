namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome summary of a
/// <see cref="ILatticeBackupCatalogScrubService.ScrubAsync"/> pass: how many
/// catalog rows were cross-checked against the sink, how many were orphans (a
/// catalog row whose sink payload is no longer resolvable - the manifest or a
/// referenced artifact is missing), how many orphan rows were removed, and the
/// ids of the orphans found. The scrub is non-destructive by default, so
/// <see cref="RemovedCount"/> is zero and <see cref="Pruned"/> is
/// <see langword="false"/> unless the caller explicitly opts in to pruning; a
/// flag-only pass still reports every orphan so an operator can inspect before
/// deleting.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupCatalogScrubReport)]
[Immutable]
public sealed record BackupCatalogScrubReport
{
    /// <summary>Initializes a new <see cref="BackupCatalogScrubReport"/>.</summary>
    /// <param name="scannedCount">The number of catalog rows cross-checked against the sink.</param>
    /// <param name="orphanCount">How many of those rows are orphans with no resolvable sink payload.</param>
    /// <param name="removedCount">How many orphan rows were removed from the catalog (zero on a non-destructive pass).</param>
    /// <param name="pruned">Whether destructive pruning was requested and applied.</param>
    /// <param name="orphanBackupIds">The ids of the orphan rows found. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="orphanBackupIds"/> is <c>null</c>.</exception>
    public BackupCatalogScrubReport(
        long scannedCount,
        long orphanCount,
        long removedCount,
        bool pruned,
        IReadOnlyList<string> orphanBackupIds)
    {
        ArgumentNullException.ThrowIfNull(orphanBackupIds);
        ScannedCount = scannedCount;
        OrphanCount = orphanCount;
        RemovedCount = removedCount;
        Pruned = pruned;
        OrphanBackupIds = orphanBackupIds;
    }

    /// <summary>The number of catalog rows cross-checked against the sink.</summary>
    [Id(0)] public long ScannedCount { get; init; }

    /// <summary>How many of the scanned rows are orphans with no resolvable sink payload.</summary>
    [Id(1)] public long OrphanCount { get; init; }

    /// <summary>How many orphan rows were removed from the catalog (zero on a non-destructive pass).</summary>
    [Id(2)] public long RemovedCount { get; init; }

    /// <summary>Whether destructive pruning was requested and applied.</summary>
    [Id(3)] public bool Pruned { get; init; }

    /// <summary>The ids of the orphan rows found, whether or not they were removed.</summary>
    [Id(4)] public IReadOnlyList<string> OrphanBackupIds { get; init; }
}
