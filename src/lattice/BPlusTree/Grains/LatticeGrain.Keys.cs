using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Strongly-consistent key enumeration via paginated k-way merge across shards
/// with per-slot reconciliation against concurrent adaptive splits (F-011).
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Enumerates the live keys of this tree in (mostly) sorted order.
    /// <para>
    /// <b>Pass 1</b> performs a streaming k-way merge across all physical shards
    /// owned by the snapshot of the shard map taken at scan start, yielding
    /// keys in sort order. Each shard filters out virtual slots that have
    /// been (or are being) moved to another physical shard by an adaptive
    /// split (F-011) and reports them via <see cref="KeysPage.MovedAwaySlots"/>.
    /// </para>
    /// <para>
    /// <b>Pass 2</b> reconciles topology changes that happened during pass 1.
    /// It re-reads the shard map and computes the union of (a) slots filtered
    /// by pass 1 and (b) slots whose owner differs between the start-of-scan
    /// map and the latest map. For each affected slot it queries the current
    /// owner with <see cref="IShardRootGrain.GetSortedKeysBatchForSlotsAsync"/>
    /// and yields any keys not already produced by pass 1. Pass 2 may iterate
    /// up to <see cref="LatticeOptions.MaxScanRetries"/> times to converge if
    /// the map keeps changing.
    /// </para>
    /// <para>
    /// <b>Order:</b> pass-1 output is in (or reverse) sort order. When topology
    /// changes mid-scan, pass-2 contributions are appended at the end in
    /// unspecified order. <b>Memory:</b> a per-call <c>HashSet&lt;string&gt;</c>
    /// is kept for the duration of the enumeration to deduplicate against
    /// pass-2 output (cost is proportional to the number of keys yielded by
    /// pass 1).
    /// </para>
    /// </summary>
    public async IAsyncEnumerable<string> KeysAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null)
    {
        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        var physicalShards = shardMap0.GetPhysicalShardIndices();
        var pageSize = Options.KeysPageSize;
        var usePrefetch = prefetch ?? Options.PrefetchKeysScan;
        var isSystemTree = TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

        // Pass 1: streaming k-way merge across the start-of-scan owners.
        var movedReported = isSystemTree ? null : new HashSet<int>();
        var cursors = new ShardCursor[physicalShards.Count];
        var initTasks = new Task[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shardKey = $"{physicalTreeId}/{physicalShards[i]}";
            cursors[i] = new ShardCursor(
                grainFactory.GetGrain<IShardRootGrain>(shardKey),
                startInclusive, endExclusive, pageSize, reverse, usePrefetch, movedReported);
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

        // System trees never split — skip dedup tracking entirely.
        HashSet<string>? yielded = isSystemTree ? null : new HashSet<string>(StringComparer.Ordinal);

        while (pq.Count > 0)
        {
            var idx = pq.Dequeue();
            var key = cursors[idx].Current!;
            yielded?.Add(key);
            yield return key;

            await cursors[idx].MoveNextAsync();
            if (cursors[idx].HasCurrent)
                pq.Enqueue(idx, cursors[idx].Current!);
        }

        if (isSystemTree) yield break;

        // Pass 2: reconcile against current map. Iterate until the map is
        // stable across one full pass-2 round, bounded by MaxScanRetries.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var virtualShardCount = shardMap0.VirtualShardCount;
        var coveredSlots = new HashSet<int>();
        var maxRetries = Math.Max(1, Options.MaxScanRetries);

        for (int retry = 0; retry < maxRetries; retry++)
        {
            var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap0;

            // Slots that need pass-2 attention this round.
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
                    var page = await shard.GetSortedKeysBatchForSlotsAsync(
                        startInclusive, endExclusive, pageSize, continuation,
                        sortedSlots, virtualShardCount);

                    foreach (var k in page.Keys)
                    {
                        if (yielded!.Add(k))
                            yield return k;
                    }

                    if (!page.HasMore) break;
                    if (page.Keys.Count == 0) break;
                    continuation = page.Keys[^1];
                }
            }

            foreach (var s in needSlots) coveredSlots.Add(s);

            // Stability: if no new ownership changes happened during pass 2,
            // we are done. Otherwise loop and pick up the new owner-diffs.
            var shardMapAfter = await registry.GetShardMapAsync(TreeId) ?? shardMapNow;
            if (shardMapAfter.Version == versionBeforePass2) yield break;
            // Else: another swap happened; recompute needSlots vs shardMap0.
        }

        // Best-effort fallthrough: we exceeded MaxScanRetries during continuous
        // topology changes. Some keys may still be missing. We choose to throw
        // (parallel to CountAsync) so callers can detect the inconsistency
        // rather than silently observing partial results.
        throw new InvalidOperationException(
            $"KeysAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
            "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
    }

    /// <summary>
    /// Lazily paginates through a single shard's keys, optionally pre-fetching
    /// the next page in the background to hide grain-call latency. Accumulates
    /// the union of <see cref="KeysPage.MovedAwaySlots"/> reported by every
    /// page consumed into <paramref name="movedSlotSink"/> for the
    /// orchestrator's pass-2 reconciliation (F-011).
    /// </summary>
    private sealed class ShardCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse,
        bool prefetch,
        HashSet<int>? movedSlotSink)
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

                if (movedSlotSink is not null && result.MovedAwaySlots is { Length: > 0 } moved)
                {
                    foreach (var s in moved) movedSlotSink.Add(s);
                }

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
