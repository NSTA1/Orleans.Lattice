namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome summary of a <see cref="ILatticeBackupCatalogRebuildService.RebuildFromSinkAsync"/>
/// pass: how many manifests were scanned out of the sink, how many were freshly
/// added to the catalog, and how many were already present and reconciled in
/// place. Because the sink is the single source of truth and the catalog is a
/// rebuildable projection over it, <see cref="ScannedCount"/> always equals
/// <see cref="RegisteredCount"/> plus <see cref="ReconciledCount"/>: every
/// manifest the scan surfaced is re-registered, either as a new catalog row or as
/// an idempotent upsert over an existing one.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupCatalogRebuildReport)]
[Immutable]
public sealed record BackupCatalogRebuildReport
{
    /// <summary>Initializes a new <see cref="BackupCatalogRebuildReport"/>.</summary>
    /// <param name="scannedCount">The number of manifests enumerated from the sink.</param>
    /// <param name="registeredCount">How many of those manifests were absent from the catalog and freshly added.</param>
    /// <param name="reconciledCount">How many of those manifests were already catalogued and reconciled in place.</param>
    public BackupCatalogRebuildReport(long scannedCount, long registeredCount, long reconciledCount)
    {
        ScannedCount = scannedCount;
        RegisteredCount = registeredCount;
        ReconciledCount = reconciledCount;
    }

    /// <summary>The number of manifests enumerated from the sink.</summary>
    [Id(0)] public long ScannedCount { get; init; }

    /// <summary>How many scanned manifests were absent from the catalog and freshly added.</summary>
    [Id(1)] public long RegisteredCount { get; init; }

    /// <summary>How many scanned manifests were already catalogued and reconciled in place.</summary>
    [Id(2)] public long ReconciledCount { get; init; }
}
