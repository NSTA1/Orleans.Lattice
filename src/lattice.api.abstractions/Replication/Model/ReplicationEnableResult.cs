namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The transport-agnostic outcome of an
/// <see cref="ILatticeReplicationControl.EnableReplicationAsync"/> call. Reports
/// the fixed merge mode the tree is now enabled under, whether the request was
/// an idempotent no-op because the tree was already enabled under the same
/// mode, and whether a snapshot bootstrap was requested to seed a peer with the
/// tree's pre-existing data.
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
    /// Whether the tree was already enabled under <paramref name="mode"/> and the
    /// call was an idempotent no-op.
    /// </param>
    /// <param name="bootstrapRequested">
    /// Whether the engine requested a snapshot bootstrap so a peer converges on
    /// the tree's pre-existing data.
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
    /// <c>true</c> when the tree was already enabled under <see cref="Mode"/> and
    /// the call was an idempotent no-op (no new dot was authored and no bootstrap
    /// was requested).
    /// </summary>
    [Id(2)] public bool AlreadyEnabled { get; init; }

    /// <summary>
    /// <c>true</c> when the engine requested a snapshot bootstrap (because the
    /// tree already held data and a bootstrap source cluster was supplied) so a
    /// peer converges on the pre-existing data.
    /// </summary>
    [Id(3)] public bool BootstrapRequested { get; init; }
}
