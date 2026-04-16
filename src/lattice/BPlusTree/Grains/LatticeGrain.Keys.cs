using System.Buffers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Key enumeration via paginated k-way merge across shards.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async IAsyncEnumerable<string> KeysAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false)
    {
        var shardCount = Options.ShardCount;
        var pageSize = Options.KeysPageSize;

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
    /// Lazily paginates through a single shard's keys.
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
}
