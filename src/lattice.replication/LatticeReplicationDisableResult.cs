namespace Orleans.Lattice.Replication;

/// <summary>
/// The outcome of an
/// <see cref="ILatticeReplicationConfigAuthority.DisableReplicationAsync"/> call.
/// Disabling authors a disable-wins flag dot so the resolver returns no runtime
/// mode, but it does not tear down an already-active shipper; if one keeps shipping,
/// peers that resolve no mode drop those entries while acknowledging the batch. It
/// never purges already-replicated peer data and keeps the tree's fixed
/// merge mode in the config OR-Map. A later re-enable requests a snapshot bootstrap
/// only when a source cluster is supplied and the tree already holds data.
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
