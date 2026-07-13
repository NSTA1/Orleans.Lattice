namespace Orleans.Lattice.Backup;

/// <summary>
/// The compact, self-contained row the backup-catalog index materialised view
/// stores for one catalogued backup. It carries exactly the fields the catalog
/// listing filters and sorts on - so a filtered, created-descending, paged query
/// can be answered by scanning the index alone, without deserialising a full
/// <see cref="BackupManifest"/> for every candidate. The full manifest is read
/// only for the handful of rows that survive the filter and land on the page.
/// </summary>
/// <remarks>
/// The index view keys each row so a forward scan yields rows newest-first with
/// the members of a backup set contiguous; the values stored here let the reader
/// evaluate name / kind / scope / created predicates inline (a push-down) as it
/// walks that order.
/// </remarks>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupCatalogIndexRow)]
[Immutable]
public sealed record BackupCatalogIndexRow
{
    /// <summary>The content-addressed id of the indexed backup.</summary>
    [Id(0)] public string BackupId { get; init; } = string.Empty;

    /// <summary>The indexed backup's own human-readable name.</summary>
    [Id(1)] public string Name { get; init; } = string.Empty;

    /// <summary>Whether the indexed backup is full or incremental.</summary>
    [Id(2)] public BackupKind Kind { get; init; }

    /// <summary>The scope tree id the indexed backup captured.</summary>
    [Id(3)] public string TreeId { get; init; } = string.Empty;

    /// <summary>The wall-clock time the indexed backup was captured.</summary>
    [Id(4)] public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>
    /// The id of the backup set this backup belongs to, or <see langword="null"/>
    /// when it was captured standalone. Every member of a set shares this value so
    /// the reader can collapse a set's contiguous index rows into one logical row.
    /// </summary>
    [Id(5)] public string? SetId { get; init; }

    /// <summary>
    /// The human-readable name of the backup set this backup belongs to, or
    /// <see langword="null"/> when it was captured standalone.
    /// </summary>
    [Id(6)] public string? SetName { get; init; }

    /// <summary>
    /// The id of the base backup this incremental is layered on, or
    /// <see langword="null"/> for a full backup. Lets the listing fold an
    /// incremental chain to its tip (the most recent increment) by recognising
    /// every id referenced as a base as an ancestor that a chain-tip row owns,
    /// without deserialising a full manifest.
    /// </summary>
    [Id(7)] public string? BaseBackupId { get; init; }

    /// <summary>
    /// The name shown for the logical row this backup belongs to: the set name for
    /// a set member, otherwise the backup's own name. Filters that target the
    /// displayed name evaluate against this value.
    /// </summary>
    public string DisplayName => SetName ?? Name;
}
