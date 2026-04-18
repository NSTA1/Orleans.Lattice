namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Key enumeration via paginated k-way merge across shards.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async IAsyncEnumerable<string> KeysAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();
        var pageSize = Options.KeysPageSize;
        var usePrefetch = prefetch ?? Options.PrefetchKeysScan;

        var cursors = new ShardCursor[physicalShards.Count];
        var initTasks = new Task[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shardKey = $"{physicalTreeId}/{physicalShards[i]}";
            cursors[i] = new ShardCursor(
                grainFactory.GetGrain<IShardRootGrain>(shardKey),
                startInclusive, endExclusive, pageSize, reverse, usePrefetch);
            initTasks[i] = cursors[i].MoveNextAsync();
        }
        await Task.WhenAll(initTasks);

        IComparer<string> comparer = reverse
            ? Comparer<string>.Create((a, b) => string.Compare(b, a, StringComparison.Ordinal))
            : StringComparer.Ordinal;
        var pq = new PriorityQueue<int, string>(comparer);

        for (int i = 0; i < physicalShards.Count; i++)
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

    /// <summary>
    /// Lazily paginates through a single shard's keys, optionally pre-fetching
    /// the next page in the background to hide grain-call latency.
    /// </summary>
    private sealed class ShardCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse,
        bool prefetch)
    {
        private List<string>? _page;
        private int _index;
        private bool _hasMore = true;
        private string? _continuation;
        private Task<KeysPage>? _prefetchTask;

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
                KeysPage result;
                if (_prefetchTask is not null)
                {
                    result = await _prefetchTask;
                    _prefetchTask = null;
                }
                else
                {
                    result = await FetchPageAsync(_continuation);
                }

                _page = result.Keys;
                _index = 0;
                _hasMore = result.HasMore;
                if (_page.Count > 0)
                    _continuation = _page[^1];

                // Kick off background fetch for the next page.
                if (prefetch && _hasMore)
                    _prefetchTask = FetchPageAsync(_continuation);
            }
        }

        private Task<KeysPage> FetchPageAsync(string? continuation)
        {
            return reverse
                ? shard.GetSortedKeysBatchReverseAsync(
                    startInclusive, endExclusive, pageSize, continuation)
                : shard.GetSortedKeysBatchAsync(
                    startInclusive, endExclusive, pageSize, continuation);
        }
    }
}
