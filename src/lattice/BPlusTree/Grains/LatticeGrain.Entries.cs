namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Entry enumeration via paginated k-way merge across shards.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();
        var pageSize = Options.KeysPageSize;

        var cursors = new EntriesCursor[physicalShards.Count];
        var initTasks = new Task[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shardKey = $"{physicalTreeId}/{physicalShards[i]}";
            cursors[i] = new EntriesCursor(
                grainFactory.GetGrain<IShardRootGrain>(shardKey),
                startInclusive, endExclusive, pageSize, reverse);
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
                pq.Enqueue(i, cursors[i].Current.Key);
        }

        while (pq.Count > 0)
        {
            var idx = pq.Dequeue();
            yield return cursors[idx].Current;

            await cursors[idx].MoveNextAsync();
            if (cursors[idx].HasCurrent)
                pq.Enqueue(idx, cursors[idx].Current.Key);
        }
    }

    /// <summary>
    /// Lazily paginates through a single shard's entries.
    /// </summary>
    private sealed class EntriesCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse)
    {
        private List<KeyValuePair<string, byte[]>>? _page;
        private int _index;
        private bool _hasMore = true;
        private string? _continuation;

        public bool HasCurrent => _page is not null && _index < _page.Count;
        public KeyValuePair<string, byte[]> Current => _page![_index];

        public async Task MoveNextAsync()
        {
            if (_page is not null)
            {
                _index++;
            }

            if (_page is null || (_index >= _page.Count && _hasMore))
            {
                var result = reverse
                    ? await shard.GetSortedEntriesBatchReverseAsync(
                        startInclusive, endExclusive, pageSize, _continuation)
                    : await shard.GetSortedEntriesBatchAsync(
                        startInclusive, endExclusive, pageSize, _continuation);
                _page = result.Entries;
                _index = 0;
                _hasMore = result.HasMore;
                if (_page.Count > 0)
                    _continuation = _page[^1].Key;
            }
        }
    }
}
