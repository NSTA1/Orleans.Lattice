namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The transport-agnostic outcome of a
/// <see cref="ILatticeReplicationControl.DisableReplicationAsync"/> call.
/// Disabling pauses shipping for the tree; it never purges already-replicated
/// peer data and keeps the tree's fixed merge mode in the config so a later
/// re-enable is a fresh bootstrap.
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
