using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// One page of the backup catalog. <see cref="NextPageToken"/> is the cursor to
/// pass back in the next <see cref="BackupCatalogRequest"/> to continue
/// enumeration; it is <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiBackupTypeAliases.BackupCatalogPage)]
[Immutable]
public sealed record BackupCatalogPage
{
    /// <summary>The backup manifests on this page, ordered by backup id.</summary>
    [Id(0)] public IReadOnlyList<BackupManifest> Entries { get; init; } = Array.Empty<BackupManifest>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
