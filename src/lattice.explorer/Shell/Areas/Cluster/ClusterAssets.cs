using Orleans.Lattice.Explorer.Shell.Design;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>The Cluster area's static assets, derived from the Shell's one content base path.</summary>
internal static class ClusterAssets
{
    /// <summary>The area's asset folder.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "cluster/";

    /// <summary>The area's stylesheet (the <c>lt-cluster-*</c> classes). The head links it after the chrome stylesheet.</summary>
    public const string Stylesheet = BasePath + "lattice-cluster.css";
}
