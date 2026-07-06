namespace Orleans.Lattice.Backup;

/// <summary>
/// Discriminates the extent of the keyspace a <see cref="BackupScopeSelector"/>
/// captures: an entire tree, every key sharing a prefix within a tree, or a
/// single key within a tree.
/// </summary>
public enum BackupScopeKind
{
    /// <summary>The backup captures an entire tree.</summary>
    WholeTree = 0,

    /// <summary>The backup captures every key sharing a prefix within a tree.</summary>
    Prefix = 1,

    /// <summary>The backup captures a single key within a tree.</summary>
    Key = 2,
}
