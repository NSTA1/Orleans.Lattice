namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// A read-only projection of a single tree's effective replication
/// configuration, carried in a <see cref="ReplicationConfigReport"/> returned by
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/>. It
/// reconciles the two enrollment sources a replication-enabled host resolves
/// against - the runtime config tree and the static deployment-time
/// replicated-tree map - into the facts an operator surface needs: whether the
/// tree is enrolled, the merge mode in force, whether that mode is currently
/// ambiguous (so shipping is paused fail-closed), and which source put it in
/// force.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationTreeConfigEntry)]
[Immutable]
public sealed record ReplicationTreeConfigEntry
{
    /// <summary>Initializes a new <see cref="ReplicationTreeConfigEntry"/>.</summary>
    /// <param name="treeId">The target tree id. Must not be <c>null</c>.</param>
    /// <param name="enabled">Whether the tree is effectively enrolled for replication.</param>
    /// <param name="mode">
    /// The single unambiguous merge mode in force, or <c>null</c> when no mode
    /// has been assigned or the mode is ambiguous.
    /// </param>
    /// <param name="ambiguous">
    /// Whether the tree's merge-mode register carries more than one live value,
    /// so shipping is paused fail-closed until an operator resolves it.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public ReplicationTreeConfigEntry(
        string treeId,
        bool enabled,
        LatticeMergeMode? mode,
        bool ambiguous)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        Enabled = enabled;
        Mode = mode;
        Ambiguous = ambiguous;
    }

    /// <summary>The target tree id this entry describes.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// <c>true</c> when the tree is <b>effectively enrolled</b>, i.e. the host
    /// admits its mutations for shipping. That is the runtime enablement flag
    /// (at least one live enable dot and no surviving disable dot) when the
    /// runtime entry is in force, and always <c>true</c> for a tree the static
    /// deployment map declares - the static map is a floor, so a runtime disable
    /// does not stop a statically declared tree.
    /// </summary>
    [Id(1)] public bool Enabled { get; init; }

    /// <summary>
    /// The single unambiguous merge mode in force, or <c>null</c> when no mode
    /// has been assigned or the mode is ambiguous. Always <c>null</c> when
    /// <see cref="Ambiguous"/> is <c>true</c>.
    /// </summary>
    [Id(2)] public LatticeMergeMode? Mode { get; init; }

    /// <summary>
    /// <c>true</c> when the tree's merge-mode register carries more than one
    /// live value, i.e. concurrent clusters assigned divergent modes that have
    /// not been reconciled. While this holds the resolver fails closed and
    /// pauses shipping the tree until an operator disables then re-enables it.
    /// Ambiguity wins over a static declaration, exactly as it does on the
    /// commit path.
    /// </summary>
    [Id(3)] public bool Ambiguous { get; init; }

    /// <summary>
    /// Which enrollment source put this entry's configuration in force - the
    /// runtime config tree, the static deployment map, or both with the runtime
    /// entry winning. Defaults to
    /// <see cref="ReplicationEnrollmentSource.Runtime"/>, so an entry received
    /// from a peer that predates this field reads as the runtime enrollment the
    /// report has always described.
    /// </summary>
    [Id(4)] public ReplicationEnrollmentSource Source { get; init; }
}
