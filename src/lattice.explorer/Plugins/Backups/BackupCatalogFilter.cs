using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The set of push-down predicates the Existing Backups list applies to the
/// backup-catalog listing: an exact kind, an exact scope tree, and starts-with
/// prefixes on the display name and the rendered created timestamp. Every field
/// is optional; an unset field imposes no constraint. The predicates are carried
/// into the server scan (they are not evaluated client-side), so a filtered page
/// only materialises the rows that match.
/// </summary>
public sealed record BackupCatalogFilter
{
    /// <summary>An empty filter that matches every backup.</summary>
    public static BackupCatalogFilter None { get; } = new();

    /// <summary>The exact backup kind to match, or <see langword="null"/> for any kind.</summary>
    public BackupKind? Kind { get; init; }

    /// <summary>The exact scope tree id to match, or <see langword="null"/> for any scope.</summary>
    public string? Scope { get; init; }

    /// <summary>The case-insensitive starts-with filter on the row's display name, or <see langword="null"/>.</summary>
    public string? NamePrefix { get; init; }

    /// <summary>The starts-with filter on the row's <c>yyyy-MM-dd HH:mm:ss</c> UTC created timestamp, or <see langword="null"/>.</summary>
    public string? CreatedPrefix { get; init; }
}
