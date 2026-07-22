namespace Orleans.Lattice.Replication;

/// <summary>
/// Reads the durable per-tree runtime replication configuration out of the
/// replicated <see cref="LatticeSystemTreeNames.ReplicationConfig"/> tree. The
/// whole configuration is a single
/// <see cref="Orleans.Lattice.OrMap{TKey, TValue}"/> stored under
/// <see cref="LatticeSystemTreeNames.ReplicationConfigMapKey"/>, keyed by target
/// tree id, so one read returns every configured tree's
/// <see cref="LatticeReplicationConfigEntry"/>.
/// <para>
/// This seam exists so the
/// <see cref="CompiledReplicationConfigSnapshotMaintainer"/> can rescan the
/// config tree off the change feed without depending on the grain factory
/// directly, and so unit tests can project a snapshot from an in-memory entry
/// set with no cluster.
/// </para>
/// </summary>
internal interface ILatticeReplicationConfigStore
{
    /// <summary>
    /// Reads every live per-tree entry currently converged into the config
    /// OR-Map, keyed by target tree id. Returns an empty map when the config
    /// tree is empty or has never been written.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The per-tree configuration entries, keyed by target tree id.</returns>
    Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the single lattice-merged <see cref="LatticeReplicationConfigEntry"/>
    /// currently converged for <paramref name="treeId"/>, or <see langword="null"/>
    /// when the config OR-Map carries no live entry for that tree. Used by the
    /// authoring path (<c>ILatticeReplicationConfigAuthority</c>) to perform the
    /// read-modify-write that mints a fresh enable/disable/mode dot on top of the
    /// current converged state.
    /// </summary>
    /// <param name="treeId">The target tree id. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The converged entry for the tree, or <see langword="null"/> when absent.</returns>
    Task<LatticeReplicationConfigEntry?> ReadEntryAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes <paramref name="entry"/> as the value for <paramref name="treeId"/>
    /// in the config OR-Map, authored by <paramref name="replicaId"/>. The whole
    /// composite CRDT is written under a fresh OR-Map dot; concurrent authoring on
    /// other clusters converges by recursing into
    /// <see cref="LatticeReplicationConfigEntry.MergeFrom"/>, so the caller must
    /// pass a fully-merged entry (typically the result of
    /// <see cref="ReadEntryAsync"/> mutated in place) rather than a partial delta.
    /// </summary>
    /// <param name="treeId">The target tree id (the OR-Map key). Must be non-empty.</param>
    /// <param name="replicaId">The replica authoring the OR-Map write. Must be non-empty.</param>
    /// <param name="entry">The composite config entry to store. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read and write hops.</param>
    Task WriteEntryAsync(
        string treeId,
        string replicaId,
        LatticeReplicationConfigEntry entry,
        CancellationToken cancellationToken = default);
}
