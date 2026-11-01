using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The portable body of <see cref="ILatticeTreeAdmin.BulkLoadStreamAsync"/>: it cuts a
/// sorted entry stream into chunks and drives the chunked session verbs
/// (<see cref="ILatticeTreeAdmin.BeginBulkLoadAsync"/>,
/// <see cref="ILatticeTreeAdmin.AppendBulkLoadAsync"/> and
/// <see cref="ILatticeTreeAdmin.CommitBulkLoadAsync"/>), so every implementation streams
/// with no change and authorization stays exactly where those verbs put it.
/// </summary>
internal static class TreeBulkLoadStreaming
{
    /// <summary>Streams <paramref name="sortedEntries"/> into <paramref name="treeId"/> through <paramref name="admin"/>.</summary>
    /// <param name="admin">The facade whose chunked verbs carry the load.</param>
    /// <param name="treeId">The tree to load.</param>
    /// <param name="operationId">The session's idempotency key.</param>
    /// <param name="sortedEntries">The entries, in strictly ascending key order.</param>
    /// <param name="progress">Receives each chunk's acknowledgement.</param>
    /// <param name="options">The chunking options.</param>
    /// <param name="cancellationToken">Stops the load between chunks.</param>
    /// <returns>The committed load's result.</returns>
    public static async Task<TreeBulkLoadResult> RunAsync(
        ILatticeTreeAdmin admin,
        string treeId,
        string operationId,
        IAsyncEnumerable<DataEntry> sortedEntries,
        IProgress<TreeBulkLoadChunkAck>? progress,
        TreeBulkLoadStreamOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(admin);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(sortedEntries);
        options ??= TreeBulkLoadStreamOptions.Default;
        options.Validate();

        if (options.FirstChunkIndex == 0)
        {
            await admin.BeginBulkLoadAsync(treeId, operationId, cancellationToken).ConfigureAwait(false);
        }

        var chunkIndex = options.FirstChunkIndex;
        var chunk = new List<DataEntry>(options.ChunkSize);
        string? previousKey = null;
        await foreach (var entry in sortedEntries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            ArgumentNullException.ThrowIfNull(entry);

            // The chunked verb checks order within a chunk; the stream also owes it across
            // chunk boundaries, which only the producer of the whole stream can see.
            if (previousKey is not null && string.CompareOrdinal(entry.Key, previousKey) <= 0)
            {
                throw new BulkLoadOrderException(treeId, chunkIndex, entry.Key, previousKey);
            }

            previousKey = entry.Key;
            chunk.Add(entry);
            if (chunk.Count == options.ChunkSize)
            {
                chunkIndex = await AppendAsync(admin, treeId, operationId, chunkIndex, chunk, progress, cancellationToken).ConfigureAwait(false);

                // A fresh list per chunk: an implementation may hold on to the one it was given.
                chunk = new List<DataEntry>(options.ChunkSize);
            }
        }

        if (chunk.Count > 0)
        {
            await AppendAsync(admin, treeId, operationId, chunkIndex, chunk, progress, cancellationToken).ConfigureAwait(false);
        }

        return await admin.CommitBulkLoadAsync(treeId, operationId, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<long> AppendAsync(
        ILatticeTreeAdmin admin,
        string treeId,
        string operationId,
        long chunkIndex,
        List<DataEntry> chunk,
        IProgress<TreeBulkLoadChunkAck>? progress,
        CancellationToken cancellationToken)
    {
        var ack = await admin.AppendBulkLoadAsync(treeId, operationId, chunkIndex, chunk, cancellationToken).ConfigureAwait(false);
        progress?.Report(ack);
        return ack.NextChunkIndex;
    }
}
