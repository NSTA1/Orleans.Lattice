namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>A Cluster address, read into the page it names and its arguments.</summary>
/// <param name="Kind">The page.</param>
/// <param name="TreeId">The logical tree the page is about, when it names one.</param>
/// <param name="View">For a tree page, which view of the tree.</param>
/// <param name="Partition">For the WAL page, the partition a move plan is for.</param>
/// <param name="Target">For the WAL page, the provider key a move plan targets.</param>
internal sealed record ClusterLocation(
    ClusterPageKind Kind,
    string? TreeId = null,
    ClusterTreeView View = ClusterTreeView.Overview,
    int? Partition = null,
    string? Target = null);
