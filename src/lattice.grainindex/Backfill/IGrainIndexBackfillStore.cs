namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The durable resume point of each index's background backfill, kept in the
/// index-registry system tree.
/// </summary>
/// <remarks>
/// A separate abstraction from the definition store and the enrolment store
/// because the three have different shapes and lifetimes: a definition is
/// written once per index per silo start, an enrolment once per grain per
/// mutation, and a checkpoint once per backfill pass. They share the tree and
/// the key scheme, which is what keeps a scan of one kind from ever seeing
/// another.
/// </remarks>
internal interface IGrainIndexBackfillStore
{
    /// <summary>
    /// Reads an index's checkpoint, or <c>null</c> when no crawl has ever been
    /// started for it.
    /// </summary>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The checkpoint, or <c>null</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    Task<GrainIndexBackfillCheckpoint?> ReadAsync(string indexName, CancellationToken cancellationToken);

    /// <summary>
    /// Durably records an index's checkpoint, replacing whatever it held.
    /// </summary>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <param name="checkpoint">The checkpoint to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the checkpoint is durable.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    Task WriteAsync(
        string indexName,
        GrainIndexBackfillCheckpoint checkpoint,
        CancellationToken cancellationToken);
}
