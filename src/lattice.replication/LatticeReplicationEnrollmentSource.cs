namespace Orleans.Lattice.Replication;

/// <summary>
/// Which of the two enrollment sources put a tree's reported replication
/// configuration <i>in force</i>: the runtime
/// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> OR-Map authored through
/// <see cref="ILatticeReplicationConfigAuthority"/>, or the static
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> deployment map.
/// Carried by <see cref="LatticeReplicationTreeStatus.Source"/>.
/// <para>
/// A replication-enabled host resolves a tree's merge mode from <b>both</b>
/// sources - the runtime OR-Map first, with the static map as the fallback floor
/// - so an operator report that named only one of them would misdescribe an
/// estate that is demonstrably replicating. This enum names the source whose
/// declaration produced the reported <see cref="LatticeReplicationTreeStatus.Mode"/>,
/// so an operator can tell a runtime enable from a deployment-time declaration
/// and knows which one to change.
/// </para>
/// </summary>
public enum LatticeReplicationEnrollmentSource
{
    /// <summary>
    /// Only the runtime config tree declares this tree, and its entry is in
    /// force. This is the default so a status projected without an explicit
    /// source - as every runtime-authored result is - reads correctly.
    /// </summary>
    Runtime = 0,

    /// <summary>
    /// The static <see cref="LatticeReplicationOptions.ReplicatedTrees"/>
    /// deployment map is what puts this tree in force: either the runtime config
    /// tree carries no entry for it at all, or it carries one that does not
    /// yield an enabled, unambiguous mode and the merge-mode resolver therefore
    /// falls back to the static declaration.
    /// <para>
    /// The second case is the operationally important one: because the static
    /// map is a <i>floor</i>, a runtime disable does not stop a statically
    /// declared tree from shipping. A tree reported <see cref="Static"/> is
    /// changed by editing the deployment configuration, not by calling disable.
    /// </para>
    /// </summary>
    Static = 1,

    /// <summary>
    /// Both sources declare this tree and the runtime config entry is the one in
    /// force, so the reported mode is the runtime-fixed mode. Disabling the tree
    /// at runtime falls back to the static declaration rather than stopping
    /// replication.
    /// </summary>
    RuntimeAndStatic = 2,
}
