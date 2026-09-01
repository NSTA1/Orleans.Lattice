namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// Reads and writes the grain-index registry's persisted definition records.
/// <para>
/// Declared as a seam rather than used directly so the startup reconciliation
/// logic - which is where the drift rules live - can be exercised without a
/// cluster, a grain factory, or a serializer.
/// </para>
/// </summary>
internal interface IGrainIndexRegistryStore
{
    /// <summary>
    /// Reads the record persisted for <paramref name="indexName"/>, or
    /// <c>null</c> when the index has never been reconciled.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The stored record, or <c>null</c> on a first run.</returns>
    Task<GrainIndexRegistryRecord?> ReadAsync(string indexName, CancellationToken cancellationToken);

    /// <summary>
    /// Persists <paramref name="record"/> as the record of truth for
    /// <paramref name="indexName"/>, replacing any previous one.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <param name="record">The record to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task WriteAsync(
        string indexName,
        GrainIndexRegistryRecord record,
        CancellationToken cancellationToken);
}
