using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The durable per-tree store of schema-version configuration. When a tree opts in
/// to envelope versioning (or advances its target version), the version admin
/// writes a <see cref="LatticeSchemaVersionConfig"/> here; the write interceptor
/// and value decoder read it (through the cached
/// <see cref="ILatticeSchemaVersionProvider"/>) to know whether and how to stamp
/// or upcast a tree's values.
/// </summary>
public interface ILatticeSchemaVersionStore
{
    /// <summary>
    /// Sets the schema-version configuration for <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="config">The version configuration to store.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    Task SetConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the schema-version configuration for <paramref name="treeId"/>, or
    /// <c>null</c> when the tree is not versioned.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeSchemaVersionConfig?> GetConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears the schema-version configuration for <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the delete.</param>
    /// <returns><c>true</c> when a config was removed; <c>false</c> when the tree was not versioned.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<bool> ClearConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates every versioned tree's configuration, keyed by governed tree id.
    /// </summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<KeyValuePair<string, LatticeSchemaVersionConfig>> ListConfigsAsync(
        CancellationToken cancellationToken = default);
}
