namespace Orleans.Lattice.Api.State;

/// <summary>
/// Lifecycle state of a tree as seen by the read-only cluster state API.
/// </summary>
public enum TreeLifecycleState
{
    /// <summary>The tree is live and accepting reads and writes.</summary>
    Active = 0,

    /// <summary>
    /// The tree has been soft-deleted and is within its retention window. Its
    /// data is retained so the tree can be recovered, but it is not readable:
    /// reads and writes on a soft-deleted tree throw until it is recovered.
    /// </summary>
    SoftDeleted = 1,

    /// <summary>
    /// The tree's retention window has elapsed and a purge is in progress
    /// (or pending) that will physically remove its data. The current state API
    /// never reports this value: the tree catalog reports a tree as
    /// <see cref="SoftDeleted"/> until the purge removes it.
    /// </summary>
    Purging = 2,
}

/// <summary>
/// Kind of a B+ tree node surfaced by the structure-query endpoint.
/// </summary>
public enum NodeKind
{
    /// <summary>The per-shard root node.</summary>
    ShardRoot = 0,

    /// <summary>An internal (separator) node.</summary>
    Internal = 1,

    /// <summary>A leaf node holding entries.</summary>
    Leaf = 2,
}
