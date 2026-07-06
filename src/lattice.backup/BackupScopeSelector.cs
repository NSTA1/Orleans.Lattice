namespace Orleans.Lattice.Backup;

/// <summary>
/// Identifies the region of a tree a backup captures: an entire tree, every key
/// sharing a prefix, or a single key. Modelled as a small discriminated shape - a
/// <see cref="Kind"/> discriminator, the always-present <see cref="TreeId"/>, and
/// an optional <see cref="KeyOrPrefix"/> - and constructed through the
/// <see cref="WholeTree(string)"/>, <see cref="Prefix(string, string)"/> and
/// <see cref="Key(string, string)"/> factory methods. The scope drives the
/// content-descriptor granularity: a whole-tree backup and a single-key backup do
/// not share a fixed per-shard or per-page shape - the descriptor shape follows
/// the backup definition.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupScopeSelector)]
[Immutable]
public sealed record BackupScopeSelector
{
    /// <summary>
    /// Initializes a new <see cref="BackupScopeSelector"/>. Prefer the
    /// <see cref="WholeTree(string)"/> / <see cref="Prefix(string, string)"/> /
    /// <see cref="Key(string, string)"/> factory methods; this constructor exists
    /// for serialization and exhaustive construction.
    /// </summary>
    /// <param name="kind">The extent of the tree the scope covers.</param>
    /// <param name="treeId">The captured tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="keyOrPrefix">
    /// The exact key (for <see cref="BackupScopeKind.Key"/>) or key prefix (for
    /// <see cref="BackupScopeKind.Prefix"/>). Must be <c>null</c> for
    /// <see cref="BackupScopeKind.WholeTree"/> and non-<c>null</c> otherwise.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="treeId"/> is <c>null</c> or empty, or
    /// <paramref name="keyOrPrefix"/> is inconsistent with <paramref name="kind"/>.
    /// </exception>
    public BackupScopeSelector(BackupScopeKind kind, string treeId, string? keyOrPrefix = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        switch (kind)
        {
            case BackupScopeKind.WholeTree when keyOrPrefix is not null:
                throw new ArgumentException(
                    "A WholeTree scope must not carry a key or prefix.", nameof(keyOrPrefix));
            case BackupScopeKind.Key when string.IsNullOrEmpty(keyOrPrefix):
                throw new ArgumentException(
                    "A Key scope requires a non-empty key.", nameof(keyOrPrefix));
            case BackupScopeKind.Prefix when string.IsNullOrEmpty(keyOrPrefix):
                throw new ArgumentException(
                    "A Prefix scope requires a non-empty prefix.", nameof(keyOrPrefix));
        }

        Kind = kind;
        TreeId = treeId;
        KeyOrPrefix = keyOrPrefix;
    }

    /// <summary>The extent of the tree this scope covers.</summary>
    [Id(0)]
    public BackupScopeKind Kind { get; init; }

    /// <summary>The captured tree id. Always present.</summary>
    [Id(1)]
    public string TreeId { get; init; }

    /// <summary>
    /// The exact key (when <see cref="Kind"/> is <see cref="BackupScopeKind.Key"/>)
    /// or the key prefix (when it is <see cref="BackupScopeKind.Prefix"/>);
    /// <c>null</c> for a whole-tree scope.
    /// </summary>
    [Id(2)]
    public string? KeyOrPrefix { get; init; }

    /// <summary>Creates a scope covering the entire tree <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The captured tree id. Must not be <c>null</c> or empty.</param>
    /// <returns>A whole-tree scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public static BackupScopeSelector WholeTree(string treeId) =>
        new(BackupScopeKind.WholeTree, treeId);

    /// <summary>Creates a scope covering every key beginning with <paramref name="prefix"/> within <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The captured tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="prefix">The key prefix. Must not be <c>null</c> or empty.</param>
    /// <returns>A prefix scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="prefix"/> is <c>null</c> or empty.</exception>
    public static BackupScopeSelector Prefix(string treeId, string prefix) =>
        new(BackupScopeKind.Prefix, treeId, prefix);

    /// <summary>Creates a scope covering the single key <paramref name="key"/> within <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The captured tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="key">The exact key. Must not be <c>null</c> or empty.</param>
    /// <returns>A single-key scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="key"/> is <c>null</c> or empty.</exception>
    public static BackupScopeSelector Key(string treeId, string key) =>
        new(BackupScopeKind.Key, treeId, key);
}
