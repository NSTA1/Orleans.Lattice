using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Paging request for the backup-catalog listing
/// (<see cref="ILatticeBackupControl.ListBackupsAsync"/>).
/// </summary>
/// <remarks>
/// The catalog is enumerated in a deterministic, stable order (ascending by
/// backup id). <see cref="PageToken"/> is the exclusive cursor: pass the
/// <see cref="BackupCatalogPage.NextPageToken"/> returned by the previous page
/// to fetch the next one. A request with a <see langword="null"/> token starts
/// from the beginning. Leaving <see cref="PageSize"/> unset (<c>0</c> or
/// negative) falls back to the facade's configured
/// <see cref="LatticeApiBackupOptions.DefaultListPageSize"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiBackupTypeAliases.BackupCatalogRequest)]
[Immutable]
public sealed record BackupCatalogRequest
{
    /// <summary>
    /// Maximum number of manifests to return in a single page. Values below
    /// <c>1</c> fall back to the facade's configured default page size; values
    /// above the configured maximum are clamped to it.
    /// </summary>
    [Id(0)] public int PageSize { get; init; }

    /// <summary>
    /// Exclusive continuation cursor: the backup id of the last manifest on the
    /// previous page. <see langword="null"/> (the default) starts from the
    /// beginning of the catalog.
    /// </summary>
    [Id(1)] public string? PageToken { get; init; }
}
