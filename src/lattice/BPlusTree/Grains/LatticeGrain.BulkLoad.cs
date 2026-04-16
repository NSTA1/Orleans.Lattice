namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bulk-load, tree deletion, recovery, and purge operations.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)
    {
        var shardCount = Options.ShardCount;
        var operationId = Guid.NewGuid().ToString("N");

        var shardBuckets = new List<KeyValuePair<string, byte[]>>[shardCount];
        for (int i = 0; i < shardCount; i++)
            shardBuckets[i] = [];

        foreach (var entry in entries)
            shardBuckets[GetShardIndex(entry.Key, shardCount)].Add(entry);

        var tasks = new List<Task>();
        for (int i = 0; i < shardCount; i++)
        {
            var bucket = shardBuckets[i];
            if (bucket.Count == 0) continue;

            bucket.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            tasks.Add(shard.BulkLoadAsync($"{operationId}-{i}", bucket));
        }

        await Task.WhenAll(tasks);
    }

    public async Task DeleteTreeAsync()
    {
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await deletion.DeleteTreeAsync();
    }

    public async Task RecoverTreeAsync()
    {
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await deletion.RecoverAsync();
    }

    public async Task PurgeTreeAsync()
    {
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await deletion.PurgeNowAsync();
    }
}
