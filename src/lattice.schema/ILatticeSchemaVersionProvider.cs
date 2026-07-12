namespace Orleans.Lattice.Schema;

/// <summary>
/// Resolves the cached per-tree <see cref="LatticeSchemaVersionConfig"/> for the
/// write interceptor and value decoder, so per-write and per-read resolution is a
/// dictionary lookup rather than a store round-trip. Mirrors
/// <c>ILatticeSchemaPolicyProvider</c>.
/// </summary>
public interface ILatticeSchemaVersionProvider
{
    /// <summary>
    /// Whether strict-mode ingest is globally enabled. When <c>false</c>, the write
    /// interceptor never inspects system-origin (replication apply / restore)
    /// writes, so trusted ingest pays zero overhead.
    /// </summary>
    bool StrictIngestEnabled { get; }

    /// <summary>
    /// Resolves the cached version config for <paramref name="treeId"/>, loading it
    /// from the durable store on a cache miss. Returns <c>null</c> for an unversioned
    /// tree (and caches that fact).
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the load.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    ValueTask<LatticeSchemaVersionConfig?> GetConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Evicts the cached config for <paramref name="treeId"/> so the next resolve
    /// reloads it. Called by the admin after a config change.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    void Invalidate(string treeId);
}
