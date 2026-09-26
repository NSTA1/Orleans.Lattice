namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The transport-agnostic outcome of a
/// <see cref="ILatticeReplicationControl.DisableReplicationAsync"/> call.
/// Disabling removes the tree's resolved runtime mode but does not tear down an
/// already-active shipper; if it keeps shipping, peers that resolve no mode drop
/// those entries while acknowledging the batch. It never purges already-replicated
/// peer data and keeps the tree's fixed merge mode in the config. A later
/// re-enable bootstraps only when a source cluster is supplied and the tree
/// already holds data.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationDisableResult)]
[Immutable]
public sealed record ReplicationDisableResult
{
    /// <summary>Initializes a new <see cref="ReplicationDisableResult"/>.</summary>
    /// <param name="treeId">The target tree id the disable was authored for. Must not be <c>null</c>.</param>
    /// <param name="alreadyDisabled">
    /// Whether the tree was already disabled (or was never configured) and the
    /// call was an idempotent no-op.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public ReplicationDisableResult(string treeId, bool alreadyDisabled)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        AlreadyDisabled = alreadyDisabled;
    }

    /// <summary>The target tree id the disable was authored for.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// <c>true</c> when the tree was already disabled (or was never configured)
    /// and the call was an idempotent no-op; <c>false</c> when a fresh disable
    /// was authored.
    /// </summary>
    [Id(1)] public bool AlreadyDisabled { get; init; }
}
