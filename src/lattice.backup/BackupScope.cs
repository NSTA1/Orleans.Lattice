namespace Orleans.Lattice.Backup;

/// <summary>
/// The region of the keyspace a single backup or restore call targets: an
/// entire tree, every key under a prefix, or one exact key. This is in-process
/// request vocabulary handed to <see cref="BackupAccessAuthorizer"/> so the
/// backup capability can be checked at the same tree / prefix / key granularity
/// the authorization rules are authored against. It never crosses a grain
/// boundary and is never persisted, so it carries no Orleans serialization
/// attributes.
/// </summary>
internal readonly record struct BackupScope
{
    private BackupScope(BackupScopeKind kind, string treeId, string? keyOrPrefix)
    {
        Kind = kind;
        TreeId = treeId;
        KeyOrPrefix = keyOrPrefix;
    }

    /// <summary>The extent of the tree this scope covers.</summary>
    public BackupScopeKind Kind { get; }

    /// <summary>The governed tree id. Always present.</summary>
    public string TreeId { get; }

    /// <summary>
    /// The exact key (when <see cref="Kind"/> is <see cref="BackupScopeKind.Key"/>)
    /// or the key prefix (when it is <see cref="BackupScopeKind.Prefix"/>);
    /// <see langword="null"/> for a whole-tree scope.
    /// </summary>
    public string? KeyOrPrefix { get; }

    /// <summary>Creates a scope covering the entire tree <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <returns>A whole-tree scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public static BackupScope Tree(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return new BackupScope(BackupScopeKind.Tree, treeId, keyOrPrefix: null);
    }

    /// <summary>Creates a scope covering every key beginning with <paramref name="prefix"/> within <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="prefix">The key prefix. Must not be <c>null</c> or empty.</param>
    /// <returns>A prefix scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="prefix"/> is <c>null</c> or empty.</exception>
    public static BackupScope Prefix(string treeId, string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        return new BackupScope(BackupScopeKind.Prefix, treeId, prefix);
    }

    /// <summary>Creates a scope covering the single key <paramref name="key"/> within <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="key">The exact key. Must not be <c>null</c> or empty.</param>
    /// <returns>A single-key scope.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="key"/> is <c>null</c> or empty.</exception>
    public static BackupScope Key(string treeId, string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new BackupScope(BackupScopeKind.Key, treeId, key);
    }
}
