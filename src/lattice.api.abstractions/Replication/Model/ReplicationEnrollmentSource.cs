namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Which enrollment source put a tree's reported replication configuration
/// <i>in force</i>: the runtime config tree authored through
/// <see cref="ILatticeReplicationControl.EnableReplicationAsync"/>, or the
/// static replicated-tree map supplied as deployment configuration. Carried by
/// <see cref="ReplicationTreeConfigEntry.Source"/>.
/// <para>
/// A replication-enabled host resolves a tree's merge mode from <b>both</b>
/// sources - the runtime config tree first, with the static deployment map as a
/// fallback floor - so a report naming only one of them would misdescribe an
/// estate that is demonstrably replicating. This enum tells an operator which
/// declaration produced the reported
/// <see cref="ReplicationTreeConfigEntry.Mode"/>, and therefore which one to
/// change.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationEnrollmentSource)]
public enum ReplicationEnrollmentSource
{
    /// <summary>
    /// Only the runtime config tree declares this tree, and its entry is in
    /// force. The default, so an entry projected without an explicit source - or
    /// received from a peer predating this field - reads as the runtime
    /// enrollment it has always described.
    /// </summary>
    Runtime = 0,

    /// <summary>
    /// The static deployment-time replicated-tree map is what puts this tree in
    /// force: either the runtime config tree carries no entry for it, or it
    /// carries one that yields no enabled, unambiguous mode and the merge-mode
    /// resolver falls back to the static declaration.
    /// <para>
    /// Because the static map is a floor, a tree reported <see cref="Static"/>
    /// keeps shipping even after
    /// <see cref="ILatticeReplicationControl.DisableReplicationAsync"/>; it is
    /// changed by editing the deployment configuration, not at runtime.
    /// </para>
    /// </summary>
    Static = 1,

    /// <summary>
    /// Both sources declare this tree and the runtime config entry is in force,
    /// so the reported mode is the runtime-fixed mode. Disabling at runtime falls
    /// back to the static declaration rather than stopping replication.
    /// </summary>
    RuntimeAndStatic = 2,
}
