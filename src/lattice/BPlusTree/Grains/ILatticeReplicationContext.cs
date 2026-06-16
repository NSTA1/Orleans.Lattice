namespace Orleans.Lattice;

/// <summary>
/// Injectable, read-only view of the host's replication <em>configuration</em>
/// that core features (which must never reference the replication package) can
/// consume to behave correctly under active-active replication. It answers
/// three questions a feature needs at write time: is replication enabled at
/// all, what is the local replica identity to author CRDT dots with, and what
/// <see cref="LatticeMergeMode"/> was declared for a given tree.
/// <para>
/// On a single-cluster host the core default registration reports
/// <see cref="IsReplicationEnabled"/> <c>= false</c>, an empty
/// <see cref="LocalReplicaId"/>, and a <c>null</c> mode for every tree, so a
/// feature transparently uses its single-writer path. When a host registers the
/// replication package, that registration replaces this seam with one backed by
/// the replication options, so the same feature observes the configured cluster
/// id and per-tree merge modes.
/// </para>
/// <para>
/// This seam deliberately exposes <em>configuration only</em>. It never surfaces
/// replication transport endpoints, peer topology, credentials, or any other
/// secret - a core feature only needs to know how its writes must converge, not
/// how or to whom they are shipped.
/// </para>
/// </summary>
public interface ILatticeReplicationContext
{
    /// <summary>
    /// <c>true</c> when the host has registered the replication package (the
    /// local cluster participates in cross-cluster replication); <c>false</c>
    /// on a single-cluster host. A feature that only diverges under
    /// multi-writer replication can short-circuit to its simple path when this
    /// is <c>false</c>.
    /// </summary>
    bool IsReplicationEnabled { get; }

    /// <summary>
    /// The local cluster's stable replica identity (the configured cluster id),
    /// or <see cref="string.Empty"/> when replication is not enabled. Features
    /// that author flag- or set-CRDT membership use this as the dot-authoring
    /// replica id so concurrent writes from different clusters carry distinct,
    /// attributable provenance.
    /// </summary>
    string LocalReplicaId { get; }

    /// <summary>
    /// Returns the <see cref="LatticeMergeMode"/> declared for
    /// <paramref name="treeId"/>, or <c>null</c> when the tree is not
    /// replicated (or replication is disabled). Called on write-time paths;
    /// implementations should be O(1) and side-effect free.
    /// </summary>
    /// <param name="treeId">The logical tree id whose declared merge mode is requested.</param>
    LatticeMergeMode? ResolveMergeMode(string treeId);
}
