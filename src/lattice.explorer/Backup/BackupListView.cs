using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// A page of the backup catalog for the Backups list, folding a permission
/// denial and a transport failure into flags so the UI renders a clean state
/// rather than catching an exception. On success <see cref="Entries"/> carries
/// the visible manifests and <see cref="NextPageToken"/> the continuation cursor.
/// </summary>
public sealed record BackupListView
{
    /// <summary>An empty, successful view (no backups visible).</summary>
    public static BackupListView Empty { get; } = new()
    {
        Status = BackupOperationStatus.Succeeded,
        Entries = Array.Empty<BackupManifest>(),
    };

    /// <summary>The outcome of loading the page.</summary>
    public required BackupOperationStatus Status { get; init; }

    /// <summary>The manifests the caller may see on this page.</summary>
    public IReadOnlyList<BackupManifest> Entries { get; init; } = Array.Empty<BackupManifest>();

    /// <summary>The continuation cursor for the next page, or <see langword="null"/> when the catalog end was reached.</summary>
    public string? NextPageToken { get; init; }

    /// <summary>A user-facing message set when <see cref="Status"/> is not <see cref="BackupOperationStatus.Succeeded"/>.</summary>
    public string? Message { get; init; }

    /// <summary><see langword="true"/> when the page loaded successfully.</summary>
    public bool IsSuccess => Status == BackupOperationStatus.Succeeded;
}
