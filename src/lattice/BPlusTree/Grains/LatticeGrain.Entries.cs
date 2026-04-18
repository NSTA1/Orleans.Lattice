using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Strongly-consistent entry enumeration via paginated k-way merge across
/// shards with per-slot reconciliation against concurrent adaptive splits (F-011).
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Enumerates the live key/value entries of this tree in (mostly) sorted
    /// key order. Same correctness model as <see cref="KeysAsync"/>: pass 1
    /// streams across the start-of-scan owners with per-slot filtering;
    /// pass 2 reconciles topology changes by querying current owners with
    /// <see cref="IShardRootGrain.GetSortedEntriesBatchForSlotsAsync"/>;
    /// duplicates are suppressed via a per-call <c>HashSet&lt;string&gt;</c>
    /// of yielded keys. Pass-2 contributions appear at the end in
    /// unspecified order when topology changes mid-scan.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false)
    {
        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        var physicalShards = shardMap0.GetPhysicalShardIndices();
        var pageSize = Options.KeysPageSize;
        var isSystemTree = TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

        var movedReported = isSystemTree ? null : new HashSet<int>();
        var cursors = new EntriesCursor[physicalShards.Count];
        var initTasks = new Task[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shardKey = $"{physicalTreeId}/{physicalShards[i]}";
            cursors[i] = new EntriesCursor(
                grainFactory.GetGrain<IShardRootGrain>(shardKey),
                startInclusive, endExclusive, pageSize, reverse, movedReported);
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

        HashSet<string>? yielded = isSystemTree ? null : new HashSet<string>(StringComparer.Ordinal);

        while (pq.Count > 0)
        {
            var idx = pq.Dequeue();
            var entry = cursors[idx].Current;
            yielded?.Add(entry.Key);
            yield return entry;

            await cursors[idx].MoveNextAsync();
            if (cursors[idx].HasCurrent)
                pq.Enqueue(idx, cursors[idx].Current.Key);
        }

        if (isSystemTree) yield break;

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var virtualShardCount = shardMap0.VirtualShardCount;
        var coveredSlots = new HashSet<int>();
        var maxRetries = Math.Max(1, Options.MaxScanRetries);

        for (int retry = 0; retry < maxRetries; retry++)
        {
            var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap0;

            var needSlots = new HashSet<int>();
            foreach (var s in movedReported!)
                if (!coveredSlots.Contains(s)) needSlots.Add(s);
            var ownerDiff = ComputeOwnerDiff(shardMap0, shardMapNow);
            if (ownerDiff is not null)
            {
                foreach (var s in ownerDiff)
                    if (!coveredSlots.Contains(s)) needSlots.Add(s);
            }

            if (needSlots.Count == 0) yield break;

            var versionBeforePass2 = shardMapNow.Version;
            var byOwner = GroupSlotsByOwner(needSlots, shardMapNow);

            foreach (var (owner, slotList) in byOwner)
            {
                var sortedSlots = ToSortedArray(slotList);
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{owner}");
                string? continuation = null;
                while (true)
                {
                    var page = await shard.GetSortedEntriesBatchForSlotsAsync(
                        startInclusive, endExclusive, pageSize, continuation,
                        sortedSlots, virtualShardCount);

                    foreach (var entry in page.Entries)
                    {
                        if (yielded!.Add(entry.Key))
                            yield return entry;
                    }

                    if (!page.HasMore) break;
                    if (page.Entries.Count == 0) break;
                    continuation = page.Entries[^1].Key;
                }
            }

            foreach (var s in needSlots) coveredSlots.Add(s);

            var shardMapAfter = await registry.GetShardMapAsync(TreeId) ?? shardMapNow;
            if (shardMapAfter.Version == versionBeforePass2) yield break;
        }

        throw new InvalidOperationException(
            $"EntriesAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
            "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
    }

    /// <summary>
    /// Lazily paginates through a single shard's entries, accumulating
    /// reported <see cref="EntriesPage.MovedAwaySlots"/> into
    /// <paramref name="movedSlotSink"/> for the orchestrator's pass-2
    /// reconciliation (F-011).
    /// </summary>
    private sealed class EntriesCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse,
        HashSet<int>? movedSlotSink)
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

                if (movedSlotSink is not null && result.MovedAwaySlots is { Length: > 0 } moved)
                {
                    foreach (var s in moved) movedSlotSink.Add(s);
                }
            }
        }
    }
}
