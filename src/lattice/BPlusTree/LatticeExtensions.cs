using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

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
