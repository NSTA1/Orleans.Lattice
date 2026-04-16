using System.Buffers;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless worker that routes requests to the correct <see cref="IShardRootGrain"/>
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c>.
/// </summary>
[StatelessWorker]
internal sealed class LatticeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILattice
{
    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    private bool _compactionEnsured;

    public async Task<byte[]?> GetAsync(string key)
    {
        var shard = GetShardGrain(key);
        return await shard.GetAsync(key);
    }

    public async Task<bool> ExistsAsync(string key)
    {
        var shard = GetShardGrain(key);
        return await shard.ExistsAsync(key);
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
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

        // Fan out reads in parallel per shard.
        var result = new Dictionary<string, byte[]>(keys.Count);
        var tasks = new List<Task>(shardBuckets.Count);

        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIdx}");
            tasks.Add(FetchFromShardAsync(shard, bucket, result));
        }

        await Task.WhenAll(tasks);
        return result;

        static async Task FetchFromShardAsync(
            IShardRootGrain shard,
            List<string> keys,
            Dictionary<string, byte[]> result)
        {
            foreach (var key in keys)
            {
                var value = await shard.GetAsync(key);
                if (value is not null)
                {
                    lock (result)
                    {
                        result[key] = value;
                    }
                }
            }
        }
    }

    public async Task SetAsync(string key, byte[] value)
    {
        await EnsureCompactionReminderAsync();
        var shard = GetShardGrain(key);
        await shard.SetAsync(key, value);
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
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
            foreach (var entry in entries)
            {
                await shard.SetAsync(entry.Key, entry.Value);
            }
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
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

    public async IAsyncEnumerable<string> KeysAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false)
    {
        var shardCount = Options.ShardCount;
        var pageSize = Options.KeysPageSize;

        // Incremental k-way merge with per-shard pagination.
        // At any given time, we hold at most shardCount × pageSize keys in memory.
        var cursors = ArrayPool<ShardCursor>.Shared.Rent(shardCount);
        var initTasks = ArrayPool<Task>.Shared.Rent(shardCount);
        try
        {
        for (int i = 0; i < shardCount; i++)
        {
            var shardKey = $"{TreeId}/{i}";
            cursors[i] = new ShardCursor(
                grainFactory.GetGrain<IShardRootGrain>(shardKey),
                startInclusive, endExclusive, pageSize, reverse);
            initTasks[i] = cursors[i].MoveNextAsync();
        }
        await Task.WhenAll(initTasks.AsSpan(0, shardCount));

        // For forward: min-heap (ascending). For reverse: max-heap (descending).
        IComparer<string> comparer = reverse
            ? Comparer<string>.Create((a, b) => string.Compare(b, a, StringComparison.Ordinal))
            : StringComparer.Ordinal;
        var pq = new PriorityQueue<int, string>(comparer);

        for (int i = 0; i < shardCount; i++)
        {
            if (cursors[i].HasCurrent)
                pq.Enqueue(i, cursors[i].Current!);
        }

        while (pq.Count > 0)
        {
            var idx = pq.Dequeue();
            yield return cursors[idx].Current!;

            await cursors[idx].MoveNextAsync();
            if (cursors[idx].HasCurrent)
                pq.Enqueue(idx, cursors[idx].Current!);
        }
        }
        finally
        {
            ArrayPool<ShardCursor>.Shared.Return(cursors, clearArray: true);
            ArrayPool<Task>.Shared.Return(initTasks, clearArray: true);
        }
    }

    /// <summary>
    /// Lazily paginates through a single shard's keys, fetching the next
    /// page only when the current page is exhausted. Supports both forward
    /// and reverse iteration.
    /// </summary>
    private sealed class ShardCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse)
    {
        private List<string>? _page;
        private int _index;
        private bool _hasMore = true;
        private string? _continuation;

        public bool HasCurrent => _page is not null && _index < _page.Count;
        public string? Current => HasCurrent ? _page![_index] : null;

        public async Task MoveNextAsync()
        {
            if (_page is not null)
            {
                _index++;
            }

            if (_page is null || (_index >= _page.Count && _hasMore))
            {
                var result = reverse
                    ? await shard.GetSortedKeysBatchReverseAsync(
                        startInclusive, endExclusive, pageSize, _continuation)
                    : await shard.GetSortedKeysBatchAsync(
                        startInclusive, endExclusive, pageSize, _continuation);
                _page = result.Keys;
                _index = 0;
                _hasMore = result.HasMore;
                if (_page.Count > 0)
                    _continuation = _page[^1];
            }
        }
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

    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)
    {
        var shardCount = Options.ShardCount;
        var operationId = Guid.NewGuid().ToString("N");

        // Group entries by shard, sorting each group by key.
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
