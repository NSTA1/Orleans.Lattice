namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The view of one tree a <c>/cluster/trees/{tree-path}</c> address names: the
/// administration page itself, its admin tools, or the resumable status page of
/// one of its long-running operations (E15).
/// </summary>
internal enum ClusterTreeView
{
    /// <summary>The tree's administration page.</summary>
    Overview,

    /// <summary><c>.../tools</c>: compaction trigger, projection digest and bulk load.</summary>
    Tools,

    /// <summary><c>.../reshard</c>: stage an online reshard and follow its status.</summary>
    Reshard,

    /// <summary><c>.../resize</c>: stage an online resize, follow it, and undo it.</summary>
    Resize,

    /// <summary><c>.../snapshot</c>: stage a snapshot and follow its status.</summary>
    Snapshot,
}
