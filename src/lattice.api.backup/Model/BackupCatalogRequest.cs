using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Paging request for the backup-catalog listing
/// (<see cref="ILatticeBackupControl.ListBackupsAsync"/>).
/// </summary>
/// <remarks>
/// <para>
/// By default the catalog is enumerated in a deterministic, stable order
/// (ascending by backup id) and <see cref="PageToken"/> is the exclusive cursor:
/// the backup id of the last manifest on the previous page.
/// </para>
/// <para>
/// Setting <see cref="OrderByCreatedDescending"/> switches to the filtered,
/// newest-first listing served from the backup-catalog index: entries come back
/// ordered by capture time (most recent first) with the members of a backup set
/// adjacent, the optional <see cref="Kind"/> / <see cref="NamePrefix"/> /
/// <see cref="TreeId"/> / <see cref="CreatedPrefix"/> predicates pushed into the
/// scan, and <see cref="PageToken"/> carrying an opaque continuation cursor
/// returned as <see cref="BackupCatalogPage.NextPageToken"/>. A request with a
/// <see langword="null"/> token starts from the beginning. Leaving
/// <see cref="PageSize"/> unset (<c>0</c> or negative) falls back to the facade's
/// configured <see cref="LatticeApiBackupOptions.DefaultListPageSize"/>.
/// </para>
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
    /// Exclusive continuation cursor. In the default backup-id order this is the
    /// backup id of the last manifest on the previous page; when
    /// <see cref="OrderByCreatedDescending"/> is set it is the opaque
    /// <see cref="BackupCatalogPage.NextPageToken"/> returned by the previous page.
    /// <see langword="null"/> (the default) starts from the beginning.
    /// </summary>
    [Id(1)] public string? PageToken { get; init; }

    /// <summary>
    /// When set, returns the catalog newest-first (by capture time) with backup-set
    /// members adjacent, served from the backup-catalog index and honouring the
    /// filter predicates below. When <see langword="false"/> (the default) the
    /// listing keeps the legacy ascending-by-backup-id order and ignores the
    /// filters.
    /// </summary>
    [Id(2)] public bool OrderByCreatedDescending { get; init; }

    /// <summary>
    /// Optional exact backup-kind filter (full or incremental). A backup-set row
    /// matches when any of its members has the requested kind. Applied only when
    /// <see cref="OrderByCreatedDescending"/> is set.
    /// </summary>
    [Id(3)] public BackupKind? Kind { get; init; }

    /// <summary>
    /// Optional case-insensitive starts-with filter on the row's display name (the
    /// set name for a set, otherwise the backup name). Applied only when
    /// <see cref="OrderByCreatedDescending"/> is set.
    /// </summary>
    [Id(4)] public string? NamePrefix { get; init; }

    /// <summary>
    /// Optional exact scope tree-id filter. A backup-set row matches when any of
    /// its members captured the requested tree. Applied only when
    /// <see cref="OrderByCreatedDescending"/> is set.
    /// </summary>
    [Id(5)] public string? TreeId { get; init; }

    /// <summary>
    /// Optional starts-with filter on the row's created timestamp rendered as the
    /// invariant UTC string <c>yyyy-MM-dd HH:mm:ss</c>. Applied only when
    /// <see cref="OrderByCreatedDescending"/> is set.
    /// </summary>
    [Id(6)] public string? CreatedPrefix { get; init; }
}
