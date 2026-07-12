using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bulk-load, tree deletion, recovery, and purge operations.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfLwwWriteToCrdtReplicatedTree();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.BulkLoad, cancellationToken);
        IReadOnlyList<KeyValuePair<string, byte[]>> effectiveEntries = entries;
        if (WriteInterceptionActive)
        {
            var list = entries as List<KeyValuePair<string, byte[]>>
                ?? new List<KeyValuePair<string, byte[]>>(entries);
            effectiveEntries = await InterceptEntriesAsync(
                LatticeOperation.BulkLoad, list, atomic: false, cancellationToken);
        }
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap.GetPhysicalShardIndices();
        var operationId = Guid.NewGuid().ToString("N");

        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(physicalShards.Count);
        foreach (var idx in physicalShards)
            shardBuckets[idx] = [];

        foreach (var entry in effectiveEntries)
        {
            var idx = shardMap.Resolve(entry.Key);
            shardBuckets[idx].Add(entry);
        }

        // Fan out to *every* physical shard, including those whose bucket
        // is empty. Each shard's own `RootNodeId is not null` guard inside
        // `ShardRootGrain.BulkLoadAsync` fires before the empty-list
        // short-circuit, so a shard that already contains data on this
        // tree (from a prior `SetAsync` or earlier bulk load) rejects the
        // call with `InvalidOperationException`. Skipping empty buckets
        // would silently miss the case where pre-existing data lives on
        // a shard that the current batch's keys do not partition into.
        // Per the contract on `ILattice.BulkLoadAsync`, "Throws
        // InvalidOperationException if any shard already contains data".
        //
        // Per-shard buckets are sorted up-front. Each shard's RPC is
        // wrapped in its own ShardActivationRetry envelope so a single
        // shard's seed-timeout only retries that shard, not every
        // sibling. The shard-side `operationId` deduplication keeps the
        // per-shard retry idempotent.
        foreach (var (_, bucket) in shardBuckets)
        {
            if (bucket.Count > 0)
                bucket.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
        }

        var tasks = new List<Task>(shardBuckets.Count);
        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            var capturedShardIdx = shardIdx;
            var capturedBucket = bucket;
            tasks.Add(ShardActivationRetry.RunAsync(
                () => shard.BulkLoadAsync($"{operationId}-{capturedShardIdx}", capturedBucket),
                cancellationToken));
        }

        await Task.WhenAll(tasks);
    }

    public async Task DeleteTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        await ThrowIfSourceOfMaterialisedViewAsync(cancellationToken);
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => deletion.DeleteTreeAsync(),
            cancellationToken);
    }

    /// <summary>
    /// Rejects deleting this tree while it is the source of one or more
    /// materialised views. A view derives its contents from its source tree's
    /// write-ahead log; deleting the source out from under a live view would
    /// freeze the view at stale state, leak the maintainer's source-WAL cursor
    /// pin, and fault any later view rebuild. The dependent view(s) must be torn
    /// down first via <c>ILatticeViewFactory.DeleteAsync</c>. The guard is an
    /// optional service registered only by <c>AddLatticeViews</c>, so a host
    /// without materialised views resolves nothing and is unaffected.
    /// </summary>
    private async Task ThrowIfSourceOfMaterialisedViewAsync(CancellationToken cancellationToken)
    {
        var guard = services.GetService<IViewSourceGuard>();
        if (guard is null)
        {
            return;
        }

        var dependents = await guard.FindDependentViewsAsync(TreeId, cancellationToken);
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException(
                $"Tree '{TreeId}' cannot be deleted because {dependents.Count} materialised view(s) derive from it: {string.Join(", ", dependents)}. Delete the dependent view(s) first via ILatticeViewFactory.DeleteAsync, then delete the source tree.");
        }
    }

    public async Task RecoverTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => deletion.RecoverAsync(),
            cancellationToken);
    }

    public async Task PurgeTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => deletion.PurgeNowAsync(),
            cancellationToken);
    }

    public async Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => resize.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren),
            cancellationToken);
    }

    public async Task UndoResizeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => resize.UndoResizeAsync(),
            cancellationToken);
    }

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => snapshot.SnapshotAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren),
            cancellationToken);
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
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetPublishEventsAsync(TreeId, enabled);
        // Make sure this activation re-reads the registry next time it publishes
        // so the override takes effect immediately locally.
        _eventsGate.Invalidate();
        LatticeMetrics.ConfigChanged.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagConfig, "publish_events"));
    }

    public async Task SetHistoryRetentionAsync(HistoryRetentionMode? mode, TimeSpan? window, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetHistoryRetentionAsync(TreeId, mode, window);
        LatticeMetrics.ConfigChanged.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagConfig, "history_retention"));
    }

    public async Task<HistoryRetentionSettings> GetHistoryRetentionAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = await registry.GetEntryAsync(TreeId);
        return new HistoryRetentionSettings
        {
            Mode = entry?.HistoryRetentionMode ?? HistoryRetentionMode.MetadataOnly,
            Window = entry?.HistoryRetentionWindowTicks is { } ticks
                ? TimeSpan.FromTicks(ticks)
                : TimeSpan.Zero,
        };
    }

    public async Task MergeAsync(string sourceTreeId, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => merge.MergeAsync(sourceTreeId),
            cancellationToken);
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
