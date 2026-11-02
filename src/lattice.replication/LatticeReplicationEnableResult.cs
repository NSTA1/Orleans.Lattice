namespace Orleans.Lattice.Replication;

/// <summary>
/// The outcome of an
/// <see cref="ILatticeReplicationConfigAuthority.EnableReplicationAsync"/> call.
/// Reports the fixed merge mode the tree is now enabled under, whether the
/// tree was already enabled under the same mode, and whether a snapshot
/// bootstrap was requested to seed this cluster with a source's pre-existing
/// rows.
/// </summary>
/// <param name="TreeId">The target tree id the enable was authored for.</param>
/// <param name="Mode">
/// The wire <see cref="LatticeMergeMode"/> the tree is enabled under. This mode
/// is fixed at enable time and can only be changed by disabling then
/// re-enabling the tree.
/// </param>
/// <param name="AlreadyEnabled">
/// <see langword="true"/> when the tree was already enabled under
/// <paramref name="Mode"/>, so no new dot was authored. A named bootstrap
/// source may still have been requested on that path; see
/// <paramref name="BootstrapRequested"/>.
/// </param>
/// <param name="BootstrapRequested">
/// <see langword="true"/> when the call named a bootstrap source cluster and a
/// snapshot bootstrap from it was requested, or was already in flight, so this
/// cluster converges on the source's pre-existing rows.
/// <see langword="false"/> when no source was named.
/// </param>
public readonly record struct LatticeReplicationEnableResult(
    string TreeId,
    LatticeMergeMode Mode,
    bool AlreadyEnabled,
    bool BootstrapRequested);
