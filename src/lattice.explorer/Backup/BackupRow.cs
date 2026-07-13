using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// One row in the Backups list: either a single standalone backup or a backup
/// set collapsed from its per-tree member manifests. A set row carries every
/// member so the UI can restore each member back to its own tree and delete the
/// whole set, while presenting the set as a single logical entry. Membership is
/// taken from the first-class <see cref="BackupManifest.SetId"/> stamped at
/// capture, never inferred from the backup name.
/// </summary>
public sealed record BackupRow
{
    /// <summary>
    /// The set id when this row is a collapsed backup set, or
    /// <see langword="null"/> when it is a single standalone backup.
    /// </summary>
    public string? SetId { get; init; }

    /// <summary>
    /// The id shown and copied for this row: the set id for a set row, otherwise
    /// the single backup's content-addressed id.
    /// </summary>
    public required string DisplayId { get; init; }

    /// <summary>The set name for a set row, otherwise the single backup's name.</summary>
    public required string Name { get; init; }

    /// <summary>The backup kind (set members are always full backups).</summary>
    public required BackupKind Kind { get; init; }

    /// <summary>
    /// The row's timestamp: the earliest member capture time for a set, otherwise
    /// the single backup's capture time.
    /// </summary>
    public required DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>
    /// The backups this row represents: exactly one for a standalone backup, or
    /// every member for a set, in catalog order.
    /// </summary>
    public required IReadOnlyList<BackupManifest> Members { get; init; }

    /// <summary><see langword="true"/> when this row is a collapsed backup set.</summary>
    public bool IsSet => SetId is not null;

    /// <summary>
    /// <see langword="true"/> when this row is the tip of an incremental chain: a
    /// standalone incremental backup that the listing collapses to a single row,
    /// its earlier increments and base full backup folded behind it. Restoring
    /// such a row offers a point-in-time choice across the chain; deleting it
    /// removes every backup in the chain.
    /// </summary>
    public bool IsIncrementalChain => !IsSet && Kind == BackupKind.Incremental;

    /// <summary>The distinct scope tree ids this row covers, in first-seen order.</summary>
    public IReadOnlyList<string> TreeIds =>
        Members.Select(m => m.Scope.TreeId).Distinct(StringComparer.Ordinal).ToList();
}
