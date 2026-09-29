namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>The page a Cluster address names.</summary>
internal enum ClusterPageKind
{
    /// <summary><c>/cluster</c>: the estate overview and the region picture.</summary>
    Overview,

    /// <summary><c>/cluster/trees</c>: every tree, by logical name.</summary>
    Trees,

    /// <summary><c>/cluster/trees/{tree-path}[/{view}]</c>: one tree's administration.</summary>
    Tree,

    /// <summary><c>/cluster/wal</c>: WAL placement audit, then move plan, execute and reclaim.</summary>
    Wal,

    /// <summary><c>/cluster/orphans</c>: the orphaned-leaf survey, audit and repair.</summary>
    Orphans,
}
