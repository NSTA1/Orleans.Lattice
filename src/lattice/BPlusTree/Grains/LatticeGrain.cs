using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless worker that routes requests to the correct <see cref="IShardRootGrain"/>
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c>.
/// </summary>
[StatelessWorker]
internal sealed partial class LatticeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILattice
{
    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    private bool _compactionEnsured;
    private string? _physicalTreeId;
    private ShardMap? _shardMap;

    public async Task<byte[]?> GetAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetAsync(key);
        }
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetWithVersionAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetWithVersionAsync(key);
        }
    }

    public async Task<bool> ExistsAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.ExistsAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.ExistsAsync(key);
        }
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        try
        {
            return await GetManyAsyncCore(keys);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await GetManyAsyncCore(keys);
        }
    }

    private async Task<Dictionary<string, byte[]>> GetManyAsyncCore(List<string> keys)
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var shardCount = Options.ShardCount;

        // Group keys by shard.
        var shardBuckets = new Dictionary<int, List<string>>();
        foreach (var key in keys)
        {
            var idx = GetShardIndex(key, shardCount);
            if (!shardBuckets.TryGetValue(idx, out var bucket))
            {
                bucket = [];
                shardBuckets[idx] = bucket;
            }
            bucket.Add(key);
        }

        // Fan out batch reads in parallel per shard.
        var result = new ConcurrentDictionary<string, byte[]>();
        var tasks = new List<Task>(shardBuckets.Count);

        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            tasks.Add(FetchFromShardAsync(shard, bucket, result));
        }

        await Task.WhenAll(tasks);
        return new Dictionary<string, byte[]>(result);

        static async Task FetchFromShardAsync(
            IShardRootGrain shard,
            List<string> keys,
            ConcurrentDictionary<string, byte[]> result)
        {
            var values = await shard.GetManyAsync(keys);
            foreach (var (key, value) in values)
            {
                result[key] = value;
            }
        }
    }

    public async Task SetAsync(string key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        await EnsureCompactionReminderAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            await shard.SetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value);
        }
    }

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        await EnsureCompactionReminderAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        await EnsureCompactionReminderAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetOrSetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetOrSetAsync(key, value);
        }
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        await EnsureCompactionReminderAsync();
        try
        {
            await SetManyAsyncCore(entries);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            await SetManyAsyncCore(entries);
        }
    }

    private async Task SetManyAsyncCore(List<KeyValuePair<string, byte[]>> entries)
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var shardCount = Options.ShardCount;

        // Group entries by shard.
        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        foreach (var entry in entries)
        {
            var idx = GetShardIndex(entry.Key, shardCount);
            if (!shardBuckets.TryGetValue(idx, out var bucket))
            {
                bucket = [];
                shardBuckets[idx] = bucket;
            }
            bucket.Add(entry);
        }

        // Fan out writes in parallel per shard.
        var tasks = new List<Task>(shardBuckets.Count);
        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            tasks.Add(WriteToShardAsync(shard, bucket));
        }

        await Task.WhenAll(tasks);

        static async Task WriteToShardAsync(
            IShardRootGrain shard,
            List<KeyValuePair<string, byte[]>> entries)
        {
            await shard.SetManyAsync(entries);
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        await EnsureCompactionReminderAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.DeleteAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.DeleteAsync(key);
        }
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        await EnsureCompactionReminderAsync();
        try
        {
            return await DeleteRangeAsyncCore(startInclusive, endExclusive);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await DeleteRangeAsyncCore(startInclusive, endExclusive);
        }
    }

    private async Task<int> DeleteRangeAsyncCore(string startInclusive, string endExclusive)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Fan out to all physical shards in parallel — any may contain keys in the range.
        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.DeleteRangeAsync(startInclusive, endExclusive);
        }

        await Task.WhenAll(tasks);

        var total = 0;
        for (int i = 0; i < tasks.Length; i++)
            total += tasks[i].Result;
        return total;
    }

    public async Task<int> CountAsync()
    {
        try
        {
            return await CountAsyncCore();
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await CountAsyncCore();
        }
    }

    private async Task<int> CountAsyncCore()
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.CountAsync();
        }

        await Task.WhenAll(tasks);

        var total = 0;
        for (int i = 0; i < tasks.Length; i++)
            total += tasks[i].Result;
        return total;
    }

    public async Task<IReadOnlyList<int>> CountPerShardAsync()
    {
        try
        {
            return await CountPerShardAsyncCore();
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await CountPerShardAsyncCore();
        }
    }

    private async Task<IReadOnlyList<int>> CountPerShardAsyncCore()
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.CountAsync();
        }

        await Task.WhenAll(tasks);

        var counts = new int[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
            counts[i] = tasks[i].Result;
        return counts;
    }

    private async Task EnsureCompactionReminderAsync()
    {
        if (_compactionEnsured) return;
        if (Options.TombstoneGracePeriod == Timeout.InfiniteTimeSpan) return;

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.EnsureReminderAsync();
        _compactionEnsured = true;
    }

    /// <summary>
    /// Resolves the physical tree ID for this logical tree, caching the result
    /// for the lifetime of this activation. Different physical tree IDs produce
    /// different leaf <see cref="GrainId"/> values, which automatically creates
    /// fresh <see cref="ILeafCacheGrain"/> instances — no cache invalidation needed.
    /// </summary>
    private async Task<string> GetPhysicalTreeIdAsync()
    {
        if (_physicalTreeId is not null) return _physicalTreeId;

        // System trees (e.g. _lattice_trees) must not resolve aliases — the
        // registry itself is backed by an ILattice tree, so calling ResolveAsync
        // here would create a circular call chain and deadlock.
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            _physicalTreeId = TreeId;
            return _physicalTreeId;
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        _physicalTreeId = await registry.ResolveAsync(TreeId);
        return _physicalTreeId;
    }

    private async Task<IShardRootGrain> GetShardGrainAsync(string key)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var shardIndex = shardMap.Resolve(key);
        var shardKey = $"{physicalTreeId}/{shardIndex}";
        return grainFactory.GetGrain<IShardRootGrain>(shardKey);
    }

    /// <summary>
    /// Returns the routing context for this tree: the resolved physical tree
    /// ID and the effective <see cref="ShardMap"/>. Both are cached for the
    /// lifetime of this activation and invalidated together by
    /// <see cref="TryInvalidateStaleAlias"/> when a downstream shard reports
    /// the tree as deleted.
    /// </summary>
    public async Task<RoutingInfo> GetRoutingAsync()
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        if (_shardMap is not null) return new RoutingInfo(physicalTreeId, _shardMap);

        var options = Options;
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            // System trees never have a custom shard map; using the default
            // also avoids a circular registry call.
            _shardMap = ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);
        }
        else
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            _shardMap = await registry.GetShardMapAsync(TreeId)
                ?? ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);
        }

        return new RoutingInfo(physicalTreeId, _shardMap);
    }

    /// <summary>
    /// Returns <c>true</c> if the cached alias was stale and has been invalidated,
    /// allowing a retry with a fresh resolution. Returns <c>false</c> if no alias
    /// was cached (meaning the tree is genuinely deleted, not a stale alias).
    /// Used as a <c>when</c> filter in catch clauses.
    /// </summary>
    private bool TryInvalidateStaleAlias()
    {
        if (_physicalTreeId is null) return false;
        _physicalTreeId = null;
        _shardMap = null;
        return true;
    }

    /// <summary>
    /// Computes a stable shard index for the given key using XxHash32.
    /// Provided for backward compatibility; new routing should go through
    /// the per-tree <see cref="ShardMap"/> via <see cref="GetRoutingAsync"/>.
    /// </summary>
    internal static int GetShardIndex(string key, int shardCount) =>
        LatticeSharding.GetShardIndex(key, shardCount);
}
