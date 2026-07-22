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
}
