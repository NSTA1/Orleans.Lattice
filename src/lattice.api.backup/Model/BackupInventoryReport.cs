namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// An operator-facing inventory summary of the whole backup catalog: absolute
/// counts and byte totals read from the durable catalog, plus the process-lifetime
/// aggregate capture-failure, restore-failure, and bytes-reclaimed tallies read
/// from the in-memory metric registry. Returned by
/// <see cref="ILatticeBackupControl.GetInventoryAsync"/> so an operator can read
/// backup health without scraping metrics.
/// </summary>
[GenerateSerializer]
[Alias(ApiBackupTypeAliases.BackupInventoryReport)]
[Immutable]
public sealed record BackupInventoryReport
{
    /// <summary>Initializes a new <see cref="BackupInventoryReport"/>.</summary>
    /// <param name="totalBackupCount">The number of catalogued backups the caller may read.</param>
    /// <param name="totalCatalogBytes">The cumulative artifact bytes across those backups.</param>
    /// <param name="fullBackupCount">How many of those backups are full backups.</param>
    /// <param name="incrementalBackupCount">How many of those backups are incremental backups.</param>
    /// <param name="oldestBackupUtc">The capture time of the oldest backup, or <c>null</c> when the catalog is empty.</param>
    /// <param name="newestBackupUtc">The capture time of the newest backup, or <c>null</c> when the catalog is empty.</param>
    /// <param name="captureFailureCount">The process-lifetime aggregate capture-failure count.</param>
    /// <param name="restoreFailureCount">The process-lifetime aggregate restore-failure count.</param>
    /// <param name="bytesReclaimed">The process-lifetime bytes reclaimed by retention / deletion.</param>
    public BackupInventoryReport(
        long totalBackupCount,
        long totalCatalogBytes,
        long fullBackupCount,
        long incrementalBackupCount,
        DateTimeOffset? oldestBackupUtc,
        DateTimeOffset? newestBackupUtc,
        long captureFailureCount,
        long restoreFailureCount,
        long bytesReclaimed)
    {
        TotalBackupCount = totalBackupCount;
        TotalCatalogBytes = totalCatalogBytes;
        FullBackupCount = fullBackupCount;
        IncrementalBackupCount = incrementalBackupCount;
        OldestBackupUtc = oldestBackupUtc;
        NewestBackupUtc = newestBackupUtc;
        CaptureFailureCount = captureFailureCount;
        RestoreFailureCount = restoreFailureCount;
        BytesReclaimed = bytesReclaimed;
    }

    /// <summary>The number of catalogued backups the caller may read.</summary>
    [Id(0)] public long TotalBackupCount { get; init; }

    /// <summary>The cumulative artifact bytes across those backups.</summary>
    [Id(1)] public long TotalCatalogBytes { get; init; }

    /// <summary>How many of those backups are full backups.</summary>
    [Id(2)] public long FullBackupCount { get; init; }

    /// <summary>How many of those backups are incremental backups.</summary>
    [Id(3)] public long IncrementalBackupCount { get; init; }

    /// <summary>The capture time of the oldest backup, or <c>null</c> when the catalog is empty.</summary>
    [Id(4)] public DateTimeOffset? OldestBackupUtc { get; init; }

    /// <summary>The capture time of the newest backup, or <c>null</c> when the catalog is empty.</summary>
    [Id(5)] public DateTimeOffset? NewestBackupUtc { get; init; }

    /// <summary>The process-lifetime aggregate capture-failure count.</summary>
    [Id(6)] public long CaptureFailureCount { get; init; }

    /// <summary>The process-lifetime aggregate restore-failure count.</summary>
    [Id(7)] public long RestoreFailureCount { get; init; }

    /// <summary>The process-lifetime bytes reclaimed by retention / deletion.</summary>
    [Id(8)] public long BytesReclaimed { get; init; }
}
