namespace Orleans.Lattice.Replication;

/// <summary>
/// The outcome of an
/// <see cref="ILatticeReplicationConfigAuthority.DisableReplicationAsync"/> call.
/// Disabling authors a disable-wins flag dot that pauses shipping for the tree;
/// it never purges already-replicated peer data and keeps the tree's fixed
/// merge mode in the config OR-Map so a later re-enable is a fresh bootstrap.
/// </summary>
/// <param name="TreeId">The target tree id the disable was authored for.</param>
/// <param name="AlreadyDisabled">
/// <see langword="true"/> when the tree was already disabled (or was never
/// configured) and the call was an idempotent no-op; <see langword="false"/>
/// when a fresh disable dot was authored.
/// </param>
public readonly record struct LatticeReplicationDisableResult(
    string TreeId,
    bool AlreadyDisabled);
