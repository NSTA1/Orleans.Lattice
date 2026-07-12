using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The catalog-wide facets the Existing Backups filter row needs, gathered once
/// from a full pass over the visible catalog: the distinct kinds and scope tree
/// ids actually present (so each filter drop-down only offers values that match
/// at least one backup), and the full standalone backups (the candidate bases an
/// incremental capture can build on). A denial or a transport failure is folded
/// into <see cref="Status"/> so the caller never has to catch.
/// </summary>
public sealed record BackupCatalogSummary
{
    /// <summary>An empty, successful summary.</summary>
    public static BackupCatalogSummary Empty { get; } = new()
    {
        Status = BackupOperationStatus.Succeeded,
    };

    /// <summary>The outcome of gathering the summary.</summary>
    public required BackupOperationStatus Status { get; init; }

    /// <summary>The distinct backup kinds present in the catalog, ascending.</summary>
    public IReadOnlyList<BackupKind> Kinds { get; init; } = Array.Empty<BackupKind>();

    /// <summary>The distinct scope tree ids present in the catalog, alphabetically ascending.</summary>
    public IReadOnlyList<string> Scopes { get; init; } = Array.Empty<string>();

    /// <summary>The full standalone backups, newest first, that an incremental capture may build on.</summary>
    public IReadOnlyList<BackupManifest> FullBackups { get; init; } = Array.Empty<BackupManifest>();
}
