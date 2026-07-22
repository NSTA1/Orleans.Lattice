namespace Orleans.Lattice.Replication;

/// <summary>
/// The engine-level authoring seam for runtime per-tree replication
/// configuration. It is the single write path that turns an operator "enable
/// this tree" / "disable this tree" intent into the correct sequence of CRDT
/// mutations on the replicated
/// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> OR-Map, so every
/// enrolled peer converges on the same per-tree
/// <see cref="LatticeReplicationConfigEntry"/> (enablement flag + fixed merge
/// mode). The API facade (the gRPC / MCP surface) depends on this seam; it owns
/// authentication and authorization and calls this authority only after the
/// caller has been authorized. The authority itself performs no authorization -
/// it assumes an already-authorized caller and runs its config reads and writes
/// under the system origin.
/// <para>
/// <b>Mode is fixed at enable time.</b> A tree's merge mode is chosen when it is
/// first enabled and cannot be changed in place: enabling an already-enabled
/// tree under a different mode is rejected with
/// <see cref="LatticeReplicationModeChangeRejectedException"/>. To change a
/// mode, disable the tree (which pauses shipping) and re-enable it under the new
/// mode, which re-fixes the mode and re-bootstraps peers.
/// </para>
/// <para>
/// <b>Enabling a non-empty tree.</b> The replication change feed only carries
/// mutations authored <i>after</i> a tree is enabled, so a tree that already
/// holds data needs a one-off snapshot bootstrap for peers to converge on the
/// pre-existing rows. See <see cref="EnableReplicationAsync"/> for how this
/// authority composes with the bootstrap seam and the documented limitation of
/// that composition.
/// </para>
/// </summary>
public interface ILatticeReplicationConfigAuthority
{
    /// <summary>
    /// Enables replication for <paramref name="treeId"/> under
    /// <paramref name="mode"/>, authoring the enablement flag and (when the tree
    /// is not already enabled) fixing the merge mode, then writing the merged
    /// <see cref="LatticeReplicationConfigEntry"/> back to the config OR-Map so
    /// every peer converges.
    /// <para>
    /// <b>Preconditions.</b> The runtime precondition validator runs first: a
    /// flag merge mode (<see cref="LatticeMergeMode.OrFlag"/> /
    /// <see cref="LatticeMergeMode.RwFlag"/>) requires a configured local replica
    /// id and is rejected with
    /// <see cref="LatticeReplicationPreconditionFailedException"/> otherwise. The
    /// config entry's own enablement flag also needs a non-empty local replica id
    /// to mint its dot, so a host with no configured
    /// <see cref="LatticeReplicationOptions.ClusterId"/> is rejected for any mode.
    /// </para>
    /// <para>
    /// <b>Mode immutability.</b> If the tree is already enabled under a single
    /// unambiguous mode equal to <paramref name="mode"/>, the call is an
    /// idempotent no-op (<see cref="LatticeReplicationEnableResult.AlreadyEnabled"/>
    /// is <see langword="true"/>, no new dot is authored). If it is enabled under
    /// a different unambiguous mode, or its mode is currently ambiguous, the call
    /// is rejected with <see cref="LatticeReplicationModeChangeRejectedException"/>
    /// instructing the operator to disable then re-enable.
    /// </para>
    /// <para>
    /// <b>Bootstrap composition.</b> When <paramref name="bootstrapSourceClusterId"/>
    /// is supplied and the tree already holds data
    /// (<c>CountAsync &gt; 0</c>), the authority requests a snapshot re-seed
    /// through <see cref="ILatticeReplicationAdmin.RequestSnapshotAsync"/> so a
    /// peer converges on the pre-existing rows the change feed will not carry.
    /// <b>Limitation:</b> the bootstrap seam is receiver-driven - it pulls a
    /// snapshot into the local cluster from the named source cluster - so
    /// <paramref name="bootstrapSourceClusterId"/> names the cluster holding the
    /// authoritative pre-existing data. When enabling on the cluster that already
    /// holds the data, its own rows reach peers through each peer's receiver-side
    /// bootstrap (fall-off-triggered auto-bootstrap or an operator re-seed on that
    /// peer); this authority cannot drive a push into remote peers. Supply the
    /// parameter only when the local cluster should pull an initial snapshot; omit
    /// it (the default) to author the enable without any bootstrap.
    /// </para>
    /// </summary>
    /// <param name="treeId">The target tree id to enable. Must be non-empty.</param>
    /// <param name="mode">The wire merge mode to fix for the tree when first enabled.</param>
    /// <param name="bootstrapSourceClusterId">
    /// Optional id of the cluster to pull an initial snapshot from when the tree
    /// already holds data. <see langword="null"/> or empty skips the bootstrap.
    /// </param>
    /// <param name="cancellationToken">Cancels the read, write, and bootstrap hops.</param>
    /// <returns>The outcome, including the fixed mode and whether a bootstrap was requested.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="LatticeReplicationPreconditionFailedException">A runtime precondition is not satisfied.</exception>
    /// <exception cref="LatticeReplicationModeChangeRejectedException">The tree is enabled under a different or ambiguous mode.</exception>
    Task<LatticeReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Disables replication for <paramref name="treeId"/>, authoring a
    /// disable-wins flag dot so the merge-mode resolver returns <see langword="null"/>
    /// and shipping pauses. The tree's <see cref="LatticeReplicationConfigEntry"/>
    /// (including its fixed merge mode) is kept in the config OR-Map, and
    /// already-replicated peer data is <b>never</b> purged - disable only stops
    /// shipping <i>new</i> mutations. A later re-enable is therefore a fresh
    /// bootstrap under whatever mode is then chosen.
    /// <para>
    /// Idempotent: disabling a tree that is absent or already disabled authors no
    /// new dot and returns <see cref="LatticeReplicationDisableResult.AlreadyDisabled"/>
    /// set to <see langword="true"/>. Authoring the disable dot needs a non-empty
    /// local replica id, so a host with no configured
    /// <see cref="LatticeReplicationOptions.ClusterId"/> is rejected with
    /// <see cref="LatticeReplicationPreconditionFailedException"/> when a real
    /// disable would be authored.
    /// </para>
    /// </summary>
    /// <param name="treeId">The target tree id to disable. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    /// <returns>The outcome, including whether the tree was already disabled.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="LatticeReplicationPreconditionFailedException">A real disable would be authored but no local replica id is configured.</exception>
    Task<LatticeReplicationDisableResult> DisableReplicationAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the current runtime replication status for <paramref name="treeId"/>
    /// from the config OR-Map, or <see langword="null"/> when the tree has no
    /// config entry. Sourced from the same OR-Map the compiled snapshot is built
    /// from, so it reflects converged state (subject to replication propagation
    /// delay).
    /// </summary>
    /// <param name="treeId">The target tree id. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tree's status, or <see langword="null"/> when it is not configured.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <see langword="null"/> or empty.</exception>
    Task<LatticeReplicationTreeStatus?> GetTreeStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the current runtime replication status for every configured tree
    /// from the config OR-Map, keyed by target tree id. Returns an empty map when
    /// no tree is configured.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>Per-tree status, keyed by target tree id.</returns>
    Task<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>> GetAllTreeStatusesAsync(
        CancellationToken cancellationToken = default);
}
