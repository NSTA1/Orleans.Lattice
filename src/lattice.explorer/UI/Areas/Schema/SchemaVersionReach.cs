namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// How a stored value stamped with one version reads while the tree's target is
/// another, as the cluster's read path decides it.
/// </summary>
internal enum SchemaVersionReach
{
    /// <summary>The tree is not configured with this version's family, so nothing is decided.</summary>
    Unconfigured = 0,

    /// <summary>The version is the target: a value stamped with it reads as stored.</summary>
    Target = 1,

    /// <summary>A chain of registered migration hops leads to the target: a value is upgraded as it is read.</summary>
    Upgraded = 2,

    /// <summary>No chain of hops leads to the target: a value stamped with it cannot be read.</summary>
    NoPath = 3,

    /// <summary>The version is above the target: a value stamped with it cannot be read until the target reaches it.</summary>
    AboveTarget = 4,
}
