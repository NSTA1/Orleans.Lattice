namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Public entry point for a distributed B+ tree.
/// A stateless-worker grain that routes requests to the correct shard root
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c> — the tree this grain manages.
/// </summary>
public interface ILattice : IGrainWithStringKey
{
    /// <summary>Gets the value associated with <paramref name="key"/>, or <c>null</c> if not found.</summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value);

    /// <summary>Deletes the value for <paramref name="key"/>. Returns <c>true</c> if it existed.</summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Returns all live keys in the tree as an ordered async stream.
    /// Keys are returned in lexicographic order (or reverse if <paramref name="reverse"/> is <c>true</c>).
    /// Optionally filters to keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// </summary>
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false);

    /// <summary>
    /// Bulk-loads key-value pairs into an empty tree, building leaves and
    /// internal nodes bottom-up without any splits. Significantly faster than
    /// individual <see cref="SetAsync"/> calls for initial data seeding.
    /// Entries do not need to be pre-sorted; the implementation sorts them internally.
    /// Throws <see cref="InvalidOperationException"/> if any shard already contains data.
    /// </summary>
    Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Soft-deletes the entire tree. All shards are immediately marked as deleted,
    /// causing subsequent reads and writes to throw <see cref="InvalidOperationException"/>.
    /// A grain reminder is registered to permanently purge all tree data after the
    /// configured <see cref="LatticeOptions.SoftDeleteDuration"/> has elapsed.
    /// Idempotent — calling on an already-deleted tree is a no-op.
    /// </summary>
    Task DeleteTreeAsync();

    /// <summary>
    /// Recovers a soft-deleted tree, restoring it to normal operation.
    /// All data written before the delete is accessible again.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed (data is gone).
    /// </summary>
    Task RecoverTreeAsync();

    /// <summary>
    /// Immediately purges a soft-deleted tree without waiting for the
    /// <see cref="LatticeOptions.SoftDeleteDuration"/> window to elapse.
    /// Permanently removes all leaf and internal node state.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed.
    /// </summary>
    Task PurgeTreeAsync();
}

/// <summary>
/// Extension methods for <see cref="ILattice"/>.
/// </summary>
public static class LatticeExtensions
{
    /// <summary>
    /// Streams sorted key-value pairs into the tree, partitioning by shard and
    /// flushing chunks in parallel across shards. Each shard receives its entries
    /// in key order via <see cref="IShardRootGrain.BulkAppendAsync"/>, which
    /// appends to the right edge without splits.
    /// <para>
    /// The input <paramref name="sortedEntries"/> <b>must</b> be in ascending key order.
    /// Per-shard ordering is preserved because hash-partitioning a globally sorted
    /// stream preserves the relative order within each partition.
    /// </para>
    /// </summary>
    /// <param name="lattice">The tree to load into.</param>
    /// <param name="sortedEntries">Entries in ascending key order.</param>
    /// <param name="grainFactory">The grain factory (needed to address shard grains directly).</param>
    /// <param name="shardCount">Number of shards (must match <see cref="LatticeOptions.ShardCount"/>).</param>
    /// <param name="chunkSize">Max entries per shard before flushing (default 10 000).</param>
    public static async Task BulkLoadAsync(
        this ILattice lattice,
        IAsyncEnumerable<KeyValuePair<string, byte[]>> sortedEntries,
        IGrainFactory grainFactory,
        int shardCount,
        int chunkSize = 10_000)
    {
        var treeId = lattice.GetPrimaryKeyString();

        // Per-shard buffers, in-flight tasks, and chunk counters for operation IDs.
        var buffers = new List<KeyValuePair<string, byte[]>>[shardCount];
        var inFlight = new Task[shardCount];
        var chunkCounters = new int[shardCount];
        var batchId = Guid.NewGuid().ToString("N");
        for (int i = 0; i < shardCount; i++)
        {
            buffers[i] = new(chunkSize);
            inFlight[i] = Task.CompletedTask;
        }

        await foreach (var entry in sortedEntries)
        {
            var shardIdx = LatticeSharding.GetShardIndex(entry.Key, shardCount);
            buffers[shardIdx].Add(entry);

            if (buffers[shardIdx].Count >= chunkSize)
            {
                // Wait for the previous flush to this shard to complete (preserves ordering).
                await inFlight[shardIdx];

                var shard = grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIdx}");
                var chunk = buffers[shardIdx];
                var opId = $"{batchId}-{shardIdx}-{chunkCounters[shardIdx]++}";
                inFlight[shardIdx] = shard.BulkAppendAsync(opId, chunk);
                buffers[shardIdx] = new(chunkSize);
            }
        }

        // Flush remaining buffers.
        var finalTasks = new List<Task>();
        for (int i = 0; i < shardCount; i++)
        {
            if (buffers[i].Count > 0)
            {
                // Wait for previous in-flight for this shard, then flush.
                await inFlight[i];
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}");
                var opId = $"{batchId}-{i}-{chunkCounters[i]++}";
                finalTasks.Add(shard.BulkAppendAsync(opId, buffers[i]));
            }
            else
            {
                finalTasks.Add(inFlight[i]);
            }
        }

        await Task.WhenAll(finalTasks);
    }
}
