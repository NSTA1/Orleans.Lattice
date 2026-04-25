namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bulk-load, tree deletion, recovery, and purge operations.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
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

            cancellationToken.ThrowIfCancellationRequested();
            bucket.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            tasks.Add(shard.BulkLoadAsync($"{operationId}-{shardIdx}", bucket));
        }

        await Task.WhenAll(tasks);
    }

    public async Task DeleteTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await deletion.DeleteTreeAsync();
    }

    public async Task RecoverTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await deletion.RecoverAsync();
    }

    public async Task PurgeTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await deletion.PurgeNowAsync();
    }

    public async Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await resize.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren);
    }

    public async Task UndoResizeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await resize.UndoResizeAsync();
    }

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await snapshot.SnapshotAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren);
    }

    public async Task<bool> TreeExistsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.ExistsAsync(TreeId);
    }

    public async Task<IReadOnlyList<string>> GetAllTreeIdsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.GetAllTreeIdsAsync();
    }

    public async Task SetPublishEventsEnabledAsync(bool? enabled, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetPublishEventsAsync(TreeId, enabled);
        // Make sure this activation re-reads the registry next time it publishes
        // so the override takes effect immediately locally.
        _eventsGate.Invalidate();
        LatticeMetrics.ConfigChanged.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagConfig, "publish_events"));
    }

    public async Task MergeAsync(string sourceTreeId, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        await merge.MergeAsync(sourceTreeId);
    }

    public async Task<bool> IsMergeCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        return await merge.IsCompleteAsync();
    }

    public async Task<bool> IsSnapshotCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        return await snapshot.IsIdleAsync();
    }

    public async Task<bool> IsResizeCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        return await resize.IsIdleAsync();
    }
}
