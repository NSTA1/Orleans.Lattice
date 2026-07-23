namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// A read-only projection of a single tree's runtime replication configuration,
/// carried in a <see cref="ReplicationConfigReport"/> returned by
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/>. It
/// distills the tree's converged config into the facts an operator surface
/// needs: whether the tree is enabled, its unambiguous declared merge mode, and
/// whether the mode is currently ambiguous (so shipping is paused fail-closed).
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationTreeConfigEntry)]
[Immutable]
public sealed record ReplicationTreeConfigEntry
{
    /// <summary>Initializes a new <see cref="ReplicationTreeConfigEntry"/>.</summary>
    /// <param name="treeId">The target tree id. Must not be <c>null</c>.</param>
    /// <param name="enabled">Whether the tree's enablement flag is currently set.</param>
    /// <param name="mode">
    /// The single unambiguous declared merge mode, or <c>null</c> when no mode
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
    /// <c>true</c> when the tree's enablement flag is currently set (at least
    /// one live enable dot and no surviving disable dot).
    /// </summary>
    [Id(1)] public bool Enabled { get; init; }

    /// <summary>
    /// The single unambiguous declared merge mode, or <c>null</c> when no mode
    /// has been assigned or the mode is ambiguous. Always <c>null</c> when
    /// <see cref="Ambiguous"/> is <c>true</c>.
    /// </summary>
    [Id(2)] public LatticeMergeMode? Mode { get; init; }

    /// <summary>
    /// <c>true</c> when the tree's merge-mode register carries more than one
    /// live value, i.e. concurrent clusters assigned divergent modes that have
    /// not been reconciled. While this holds the resolver fails closed and
    /// pauses shipping the tree until an operator disables then re-enables it.
    /// </summary>
    [Id(3)] public bool Ambiguous { get; init; }
}
