namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The transport-agnostic outcome of an
/// <see cref="ILatticeReplicationControl.EnableReplicationAsync"/> call. Reports
/// the fixed merge mode the tree is now enabled under, whether the tree was
/// already enabled under the same mode, and whether a snapshot bootstrap was
/// requested to seed this cluster with a source's pre-existing rows.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationEnableResult)]
[Immutable]
public sealed record ReplicationEnableResult
{
    /// <summary>Initializes a new <see cref="ReplicationEnableResult"/>.</summary>
    /// <param name="treeId">The target tree id the enable was authored for. Must not be <c>null</c>.</param>
    /// <param name="mode">The wire merge mode the tree is enabled under.</param>
    /// <param name="alreadyEnabled">
    /// Whether the tree was already enabled under <paramref name="mode"/>, so no
    /// configuration was authored.
    /// </param>
    /// <param name="bootstrapRequested">
    /// Whether a snapshot bootstrap from the named source cluster was requested or
    /// is already in flight, so this cluster converges on the source's
    /// pre-existing rows.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public ReplicationEnableResult(
        string treeId,
        LatticeMergeMode mode,
        bool alreadyEnabled,
        bool bootstrapRequested)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        Mode = mode;
        AlreadyEnabled = alreadyEnabled;
        BootstrapRequested = bootstrapRequested;
    }

    /// <summary>The target tree id the enable was authored for.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// The wire merge mode the tree is enabled under. Fixed at enable time and
    /// changeable only by disabling then re-enabling the tree.
    /// </summary>
    [Id(1)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// <c>true</c> when the tree was already enabled under <see cref="Mode"/>, so
    /// no new configuration was authored. A named bootstrap source is still
    /// honoured; see <see cref="BootstrapRequested"/>.
    /// </summary>
    [Id(2)] public bool AlreadyEnabled { get; init; }

    /// <summary>
    /// <c>true</c> when the call named a bootstrap source cluster and a snapshot
    /// bootstrap from it was requested or is already in flight, so this cluster
    /// converges on the source's pre-existing rows. <c>false</c> when no source
    /// was named.
    /// </summary>
    [Id(3)] public bool BootstrapRequested { get; init; }
}
