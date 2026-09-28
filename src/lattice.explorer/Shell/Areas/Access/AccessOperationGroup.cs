namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>The groups the rule editor's operation checklist is drawn in.</summary>
internal enum AccessOperationGroup
{
    /// <summary>Keyspace operations on a tree.</summary>
    Data = 0,

    /// <summary>Tree administration operations.</summary>
    Administration = 1,

    /// <summary>Scopeless capabilities granted only at the cluster-wide scope.</summary>
    ClusterWide = 2,
}
