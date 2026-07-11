namespace Orleans.Lattice.Schema;

/// <summary>
/// The durable per-tree store of strict-mode dead-letter entries. When strict
/// ingest diverts a non-compliant replicated / restored item, the enforcement
/// interceptor appends a <see cref="LatticeSchemaDeadLetterEntry"/> here; the
/// entries are retained for inspection and replay. The list / count read surface
/// is public and resolvable so a state API (the DLQ-surfacing feature) can read
/// it without taking a dependency on the interceptor.
/// </summary>
public interface ILatticeSchemaDeadLetterStore
{
    /// <summary>
    /// Appends a dead-letter entry for <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The governed tree the item was destined for. Must not be <c>null</c> or empty.</param>
    /// <param name="entry">The dead-letter entry. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="entry"/> is <c>null</c>.</exception>
    Task AppendAsync(string treeId, LatticeSchemaDeadLetterEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates every dead-letter entry for <paramref name="treeId"/>, in
    /// append (time) order, as a single prefix scan.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Counts the dead-letter entries for <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default);
}
