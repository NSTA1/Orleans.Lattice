namespace Orleans.Lattice;

/// <summary>
/// How a materialised view's tree is made available across replicating clusters.
/// Selected per view through <see cref="LatticeViewOptions.ReplicationMode"/>.
/// <para>
/// The choice is purely about who runs the maintainer and whether the view tree
/// itself is replicated; it never changes the projection logic. The default
/// (<see cref="DeriveLocally"/>) is fully backward compatible with every
/// single-cluster and full-replication deployment.
/// </para>
/// </summary>
public enum LatticeViewReplicationMode
{
    /// <summary>
    /// The maintainer runs on every cluster and rebuilds the view from the
    /// replicated <em>source</em>; the view's data is never replicated, only the
    /// projection code and its configuration are deployed to each cluster. A
    /// joining cluster self-bootstraps its view locally from its local copy of the
    /// source. Assumes a deterministic projection at a uniform version across
    /// clusters. This is the default and matches every existing deployment, where
    /// the source is local and the maintainer always runs.
    /// </summary>
    DeriveLocally = 0,

    /// <summary>
    /// The maintainer runs only on one producer cluster and the view tree is
    /// replicated to consumers. When consumers do not replicate the source, local
    /// source-WAL ownership identifies the producer. When both source and view trees
    /// replicate, <see cref="LatticeViewOptions.ShipViewProducerClusterId"/> must
    /// explicitly identify the single producer. Every other cluster suppresses its
    /// maintainer and receives the view through the ordinary replication bootstrap /
    /// catch-up / apply path. Opt-in.
    /// </summary>
    ShipView = 1,
}
