namespace Orleans.Lattice.Backup;

/// <summary>
/// The extent of the keyspace a <see cref="BackupScope"/> authorizes for a
/// backup or restore: an entire tree, every key under a prefix, or a single
/// exact key. Mirrors the region model the authorization rules are authored
/// against so a captured / restored scope maps one-to-one onto the grant that
/// governs it.
/// </summary>
internal enum BackupScopeKind
{
    /// <summary>The whole tree.</summary>
    Tree = 0,

    /// <summary>Every key that begins with a given prefix within a tree.</summary>
    Prefix = 1,

    /// <summary>A single exact key within a tree.</summary>
    Key = 2,
}
