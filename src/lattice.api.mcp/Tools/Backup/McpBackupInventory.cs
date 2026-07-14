namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_inventory</c> tool: the
/// catalog-wide summary counts and byte totals over the backups the caller may
/// read, plus the process-lifetime failure and reclaimed-bytes tallies. A direct
/// projection of the facade's inventory report.
/// </summary>
internal sealed record McpBackupInventory
{
    /// <summary>The number of catalogued backups the caller may read.</summary>
    public required long TotalBackupCount { get; init; }

    /// <summary>The cumulative artifact bytes across those backups.</summary>
    public required long TotalCatalogBytes { get; init; }

    /// <summary>How many of those backups are full backups.</summary>
    public required long FullBackupCount { get; init; }

    /// <summary>How many of those backups are incremental backups.</summary>
    public required long IncrementalBackupCount { get; init; }

    /// <summary>The capture time of the oldest backup, or <see langword="null"/> when the catalog is empty.</summary>
    public DateTimeOffset? OldestBackupUtc { get; init; }

    /// <summary>The capture time of the newest backup, or <see langword="null"/> when the catalog is empty.</summary>
    public DateTimeOffset? NewestBackupUtc { get; init; }

    /// <summary>The process-lifetime aggregate capture-failure count.</summary>
    public required long CaptureFailureCount { get; init; }

    /// <summary>The process-lifetime aggregate restore-failure count.</summary>
    public required long RestoreFailureCount { get; init; }

    /// <summary>The process-lifetime bytes reclaimed by retention / deletion.</summary>
    public required long BytesReclaimed { get; init; }
}
