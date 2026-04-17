namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bulk-load, tree deletion, recovery, and purge operations.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var physicalTreeId = await GetPhysicalTreeIdAsync();
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
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{i}");
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

    public async Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await resize.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren);
    }

    public async Task UndoResizeAsync()
    {
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await resize.UndoResizeAsync();
    }

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null)
    {
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await snapshot.SnapshotAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren);
    }

    public async Task<bool> TreeExistsAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.ExistsAsync(TreeId);
    }

    public async Task<IReadOnlyList<string>> GetAllTreeIdsAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.GetAllTreeIdsAsync();
    }
}
