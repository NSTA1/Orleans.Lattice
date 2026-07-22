namespace Orleans.Lattice.Replication;

/// <summary>
/// The outcome of an
/// <see cref="ILatticeReplicationConfigAuthority.EnableReplicationAsync"/> call.
/// Reports the fixed merge mode the tree is now enabled under, whether the
/// request was a no-op because the tree was already enabled under the same
/// mode, and whether a snapshot bootstrap was requested to seed a peer with the
/// tree's pre-existing data.
/// </summary>
/// <param name="TreeId">The target tree id the enable was authored for.</param>
/// <param name="Mode">
/// The wire <see cref="LatticeMergeMode"/> the tree is enabled under. This mode
/// is fixed at enable time and can only be changed by disabling then
/// re-enabling the tree.
/// </param>
/// <param name="AlreadyEnabled">
/// <see langword="true"/> when the tree was already enabled under
/// <paramref name="Mode"/> and the call was an idempotent no-op (no new dot was
/// authored and no bootstrap was requested).
/// </param>
/// <param name="BootstrapRequested">
/// <see langword="true"/> when the authority requested a snapshot bootstrap
/// (because the tree already held data and a bootstrap source cluster was
/// supplied) so a peer converges on the pre-existing data.
/// </param>
public readonly record struct LatticeReplicationEnableResult(
    string TreeId,
    LatticeMergeMode Mode,
    bool AlreadyEnabled,
    bool BootstrapRequested);
