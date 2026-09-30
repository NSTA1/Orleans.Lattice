namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The scope a Cluster page answers for, cascaded by the area's routed page to
/// every page and link inside it: the tenant a tenant-rooted address names
/// (<c>/t/{tenant}/cluster/...</c>), whose own trees and storage are all it
/// shows, or <see langword="null"/> on a cluster-wide address.
/// </summary>
internal static class ClusterScope
{
    /// <summary>The name of the cascading value carrying the scope tenant.</summary>
    public const string CascadeName = "lattice-cluster-scope";
}
