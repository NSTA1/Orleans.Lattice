namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bulk-load, tree deletion, recovery, and purge operations.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();
        var operationId = Guid.NewGuid().ToString("N");

        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(physicalShards.Count);
        foreach (var idx in physicalShards)
            shardBuckets[idx] = [];

        foreach (var entry in entries)
        {
            var idx = shardMap.Resolve(entry.Key);
            shardBuckets[idx].Add(entry);
        }

        var tasks = new List<Task>();
        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            if (bucket.Count == 0) continue;

            bucket.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            tasks.Add(shard.BulkLoadAsync($"{operationId}-{shardIdx}", bucket));
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

    public async Task MergeAsync(string sourceTreeId)
    {
        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        await merge.MergeAsync(sourceTreeId);
    }

    public async Task<bool> IsMergeCompleteAsync()
    {
        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        return await merge.IsCompleteAsync();
    }

    public async Task<bool> IsSnapshotCompleteAsync()
    {
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        return await snapshot.IsCompleteAsync();
    }

    public async Task<bool> IsResizeCompleteAsync()
    {
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        return await resize.IsCompleteAsync();
    }
}
