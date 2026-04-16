using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;

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

    public async Task<byte[]?> GetAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = GetShardGrain(key);
        return await shard.GetAsync(key);
    }

    public async Task<bool> ExistsAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = GetShardGrain(key);
        return await shard.ExistsAsync(key);
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
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
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIdx}");
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
        var shard = GetShardGrain(key);
        await shard.SetAsync(key, value);
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        await EnsureCompactionReminderAsync();
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
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIdx}");
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
        var shard = GetShardGrain(key);
        return await shard.DeleteAsync(key);
    }

    private async Task EnsureCompactionReminderAsync()
    {
        if (_compactionEnsured) return;
        if (Options.TombstoneGracePeriod == Timeout.InfiniteTimeSpan) return;

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.EnsureReminderAsync();
        _compactionEnsured = true;
    }

    private IShardRootGrain GetShardGrain(string key)
    {
        var shardIndex = GetShardIndex(key, Options.ShardCount);
        var shardKey = $"{TreeId}/{shardIndex}";
        return grainFactory.GetGrain<IShardRootGrain>(shardKey);
    }

    /// <summary>
    /// Computes a stable shard index for the given key using XxHash32.
    /// </summary>
    internal static int GetShardIndex(string key, int shardCount) =>
        LatticeSharding.GetShardIndex(key, shardCount);
}
