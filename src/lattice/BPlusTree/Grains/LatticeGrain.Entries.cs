using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Strongly-consistent, globally-sorted entry enumeration via paginated k-way
/// merge across shards with in-line reconciliation against concurrent
/// adaptive splits.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Pre-allocated comparers for entries reconciliation sorts. Hoisted so
    /// the reconcile path does not allocate a closure on every split.
    /// </summary>
    private static readonly Comparer<KeyValuePair<string, byte[]>> EntriesForwardComparer =
        Comparer<KeyValuePair<string, byte[]>>.Create(
            static (a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));

    private static readonly Comparer<KeyValuePair<string, byte[]>> EntriesReverseComparer =
        Comparer<KeyValuePair<string, byte[]>>.Create(
            static (a, b) => string.Compare(b.Key, a.Key, StringComparison.Ordinal));

    /// <summary>
    /// Enumerates the live key/value entries of this tree in strict sorted
    /// key order (ascending, or descending when <paramref name="reverse"/>
    /// is <c>true</c>). Same correctness model as <see cref="KeysAsync"/>:
    /// a streaming k-way merge with in-line reconciliation. When any shard
    /// reports newly-moved slots, the orchestrator queries their current
    /// owners, sorts the reconciled entries, and injects them as an
    /// additional cursor into the same priority queue - so output remains
    /// globally sorted even when an adaptive split commits mid-scan.
    /// Duplicates (keys visible in both pre- and post-swap views) are
    /// suppressed by a per-call <c>HashSet&lt;string&gt;</c>. Liveness is
    /// bounded by <see cref="LatticeOptions.MaxScanRetries"/>. See
    /// <see cref="KeysAsync"/> for the single-threaded-turn ordering
    /// invariant that backs this guarantee.
    /// </summary>
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        return EntriesAsyncCore(startInclusive, endExclusive, reverse, prefetch, null, enforceAccessGate: true, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesWherePredicateAsync(
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        return EntriesAsyncCore(startInclusive, endExclusive, reverse, prefetch, predicate, enforceAccessGate: true, cancellationToken);
    }

    IAsyncEnumerable<KeyValuePair<string, byte[]>> ISystemLattice.EntriesAsync(
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        CancellationToken cancellationToken)
        => EntriesAsyncCore(startInclusive, endExclusive, reverse, prefetch, null, enforceAccessGate: false, cancellationToken);

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsyncCore(
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        LatticePredicateNode? predicate,
        bool enforceAccessGate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Read-path access-gate key-filter: resolved once per scan and applied
        // at the yield point so an unauthorized entry's value never crosses the
        // grain boundary. Null (no per-key work) on the default / system-origin
        // path; ISystemLattice callers pass enforceAccessGate: false.
        Func<string, bool>? keyFilter = enforceAccessGate
            ? await ResolveRangeReadKeyFilterAsync(startInclusive, endExclusive, cancellationToken)
            : null;

        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        var physicalShards = shardMap0.GetPhysicalShardIndices();
        var pageSize = Options.KeysPageSize;
        var usePrefetch = prefetch ?? Options.PrefetchEntriesScan;
        var isSystemTree = TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

        // Streaming-scan isolation: see KeysAsyncCore for the full
        // rationale. One registry snapshot pinned on the ambient for
        // the lifetime of the IAsyncEnumerable so every per-shard
        // page reads the same saga-decision view.
        Dictionary<Guid, TxStatus>? scanSnapshot = null;
        var ownsScanSnapshotScope = !isSystemTree
            && LatticeRegistrySnapshotContext.Current is null;
        if (ownsScanSnapshotScope)
        {
            scanSnapshot = (await FetchRegistrySnapshotAsync()).Snap;
        }
        using var scanSnapshotScope = ownsScanSnapshotScope
            ? LatticeRegistrySnapshotContext.BeginScope(scanSnapshot)
            : null;

        IComparer<string> comparer = reverse ? ReverseOrdinal : StringComparer.Ordinal;
        var entriesComparer = reverse ? EntriesReverseComparer : EntriesForwardComparer;

        // Per-shard ShardActivationRetry wrap on the initial MoveNextAsync:
        // a single shard's cold-start seed-timeout retries only that shard,
        // not the whole scan init. Routing resolution + the initial page fetch
        // additionally run under a stale-alias self-heal retry so a
        // shadow-cutover restore that supersedes this tree mid-setup makes the
        // scan re-resolve onto the current physical tree (see KeysAsyncCore).
        var movedReported = isSystemTree ? null : new HashSet<int>();
        List<IEntriesCursor> cursors;
        var scanInitDeadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
        while (true)
        {
            cursors = new List<IEntriesCursor>(physicalShards.Count);
            var initTasks = new Task[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
            {
                var sc = new EntriesCursor(
                    GetShardGrainByIndex(physicalTreeId, physicalShards[i]),
                    startInclusive, endExclusive, pageSize, reverse, usePrefetch, movedReported, predicate);
                cursors.Add(sc);
                initTasks[i] = ShardActivationRetry.RunAsync(
                    () => sc.MoveNextAsync());
            }
            try
            {
                await Task.WhenAll(initTasks);
                break;
            }
            catch (StaleTreeRoutingException)
            {
                if (DateTime.UtcNow >= scanInitDeadline) throw;
                if (!TryInvalidateStaleAlias()) throw;
                movedReported?.Clear();
                (physicalTreeId, shardMap0) = await GetRoutingAsync();
                physicalShards = shardMap0.GetPhysicalShardIndices();
            }
        }

        var pq = new PriorityQueue<int, string>(comparer);
        for (int i = 0; i < cursors.Count; i++)
        {
            if (cursors[i].HasCurrent)
                pq.Enqueue(i, cursors[i].Current.Key);
        }

        HashSet<string>? yielded = isSystemTree ? null : new HashSet<string>(capacity: pageSize, comparer: StringComparer.Ordinal);
        // Frontier of the k-way merge: the last key emitted to the caller.
        // Used only on predicate scans to suppress reconciliation entries the
        // merge has already passed. A churn key can be filtered out when its
        // position is first scanned, then cross the predicate threshold via a
        // concurrent write and be re-surfaced by a slot-move drain; injecting
        // it behind the frontier would break global ordering. Dropping it keeps
        // the scan's "value as-of visit time" semantics consistent and ordered.
        string? lastYieldedKey = null;
        var maxRetries = isSystemTree ? 0 : Math.Max(1, Options.MaxScanRetries);
        var retriesUsed = 0;
        var coveredSlots = isSystemTree ? null : new HashSet<int>();
        var lastMapVersion = shardMap0.Version;
        ILatticeRegistry? registry = null;
        var virtualShardCount = shardMap0.VirtualShardCount;
        // Start at 0 so any MovedAwaySlots already reported by the initial
        // page fetches trigger reconciliation on the first iteration before
        // any key is dequeued.
        var reconciledCount = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // In-line reconciliation: run BEFORE each dequeue whenever a
            // live cursor has reported new moved slots since the last
            // reconciliation. See KeysAsync for the ordering proof.
            if (!isSystemTree && HasNewMovedSlots(movedReported!, coveredSlots!, reconciledCount))
            {
                if (retriesUsed >= maxRetries)
                {
                    throw new InvalidOperationException(
                        $"EntriesAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
                        "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
                }
                retriesUsed++;

                registry ??= grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap0;

                var needSlots = new HashSet<int>();
                foreach (var s in movedReported!)
                    if (!coveredSlots!.Contains(s)) needSlots.Add(s);
                if (shardMapNow.Version != lastMapVersion)
                {
                    var ownerDiff = ComputeOwnerDiff(shardMap0, shardMapNow);
                    if (ownerDiff is not null)
                    {
                        foreach (var s in ownerDiff)
                            if (!coveredSlots!.Contains(s)) needSlots.Add(s);
                    }
                    lastMapVersion = shardMapNow.Version;
                }

                if (needSlots.Count > 0)
                {
                    var buffer = await DrainEntrySlotsToBufferAsync(
                        physicalTreeId, startInclusive, endExclusive, pageSize,
                        needSlots, shardMapNow, virtualShardCount, yielded, predicate,
                        comparer, lastYieldedKey);

                    foreach (var s in needSlots) coveredSlots!.Add(s);

                    if (buffer.Count > 0)
                    {
                        buffer.Sort(entriesComparer);
                        var memCursor = new MemoryEntriesCursor(buffer);
                        cursors.Add(memCursor);
                        memCursor.MoveNext();
                        if (memCursor.HasCurrent)
                            pq.Enqueue(cursors.Count - 1, memCursor.Current.Key);
                    }
                }

                reconciledCount = movedReported!.Count;
            }

            if (pq.Count == 0)
            {
                if (isSystemTree) yield break;

                // Final stability check: handle the edge case where all
                // live cursors finished before the split report propagated.
                registry ??= grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                var shardMapFinal = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
                if (shardMapFinal.Version == lastMapVersion) yield break;

                if (retriesUsed >= maxRetries)
                {
                    throw new InvalidOperationException(
                        $"EntriesAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
                        "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
                }
                retriesUsed++;

                var finalDiff = ComputeOwnerDiff(shardMap0, shardMapFinal);
                lastMapVersion = shardMapFinal.Version;
                if (finalDiff is null) yield break;

                var finalNeedSlots = new HashSet<int>();
                foreach (var s in finalDiff)
                    if (!coveredSlots!.Contains(s)) finalNeedSlots.Add(s);
                if (finalNeedSlots.Count == 0) yield break;

                var finalBuffer = await DrainEntrySlotsToBufferAsync(
                    physicalTreeId, startInclusive, endExclusive, pageSize,
                    finalNeedSlots, shardMapFinal, virtualShardCount, yielded, predicate,
                    comparer, lastYieldedKey);

                foreach (var s in finalNeedSlots) coveredSlots!.Add(s);

                if (finalBuffer.Count == 0) continue;

                finalBuffer.Sort(entriesComparer);
                var finalCursor = new MemoryEntriesCursor(finalBuffer);
                cursors.Add(finalCursor);
                finalCursor.MoveNext();
                if (finalCursor.HasCurrent)
                    pq.Enqueue(cursors.Count - 1, finalCursor.Current.Key);
                continue;
            }

            var idx = pq.Dequeue();
            var entry = cursors[idx].Current;

            if (yielded is null || yielded.Add(entry.Key))
            {
                // Advance the merge frontier for every key the merge passes,
                // independently of the access-gate visibility prune, so
                // reconciliation ordering/dedup bookkeeping stays correct even
                // when an entry is hidden from the caller.
                lastYieldedKey = entry.Key;
                if (keyFilter is null || keyFilter(entry.Key))
                    yield return entry;
            }

            await cursors[idx].MoveNextAsync();
            if (cursors[idx].HasCurrent)
                pq.Enqueue(idx, cursors[idx].Current.Key);
        }
    }

    private static bool HasNewMovedSlots(
        HashSet<int> movedReported,
        HashSet<int> coveredSlots,
        int lastReconciledMovedCount)
    {
        if (movedReported.Count <= lastReconciledMovedCount) return false;
        foreach (var s in movedReported)
        {
            if (!coveredSlots.Contains(s)) return true;
        }
        return false;
    }

    private async Task<List<KeyValuePair<string, byte[]>>> DrainEntrySlotsToBufferAsync(
        string physicalTreeId,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        HashSet<int> needSlots,
        ShardMap shardMapNow,
        int virtualShardCount,
        HashSet<string>? yielded,
        LatticePredicateNode? predicate,
        IComparer<string> comparer,
        string? frontier)
    {
        var buffer = new List<KeyValuePair<string, byte[]>>();
        // On predicate scans, drop any drained key the merge frontier has
        // already passed: its position was scanned earlier (and filtered), so
        // re-surfacing it now would emit it behind already-yielded keys.
        var suppressBehindFrontier = predicate is not null && frontier is not null;
        var byOwner = GroupSlotsByOwner(needSlots, shardMapNow);
        foreach (var (owner, slotList) in byOwner)
        {
            var sortedSlots = ToSortedArray(slotList);
            var shard = GetShardGrainByIndex(physicalTreeId, owner);
            string? continuation = null;
            while (true)
            {
                var page = await shard.GetSortedEntriesBatchForSlotsAsync(
                    startInclusive, endExclusive, pageSize, continuation,
                    sortedSlots, virtualShardCount, predicate);

                foreach (var entry in page.Entries)
                {
                    if (suppressBehindFrontier && comparer.Compare(entry.Key, frontier!) <= 0)
                        continue;
                    if (yielded is null || !yielded.Contains(entry.Key))
                        buffer.Add(entry);
                }

                if (!page.HasMore) break;
                if (page.Entries.Count == 0) break;
                continuation = page.Entries[^1].Key;
            }
        }
        return buffer;
    }

    /// <summary>
    /// Uniform cursor interface over live shard cursors and injected memory
    /// cursors for the entries k-way merge.
    /// </summary>
    private interface IEntriesCursor
    {
        bool HasCurrent { get; }
        KeyValuePair<string, byte[]> Current { get; }
        Task MoveNextAsync();
    }

    /// <summary>
    /// Cursor backed by a pre-sorted in-memory list of entries produced by a
    /// reconciliation step.
    /// </summary>
    private sealed class MemoryEntriesCursor(List<KeyValuePair<string, byte[]>> sortedEntries) : IEntriesCursor
    {
        private int _index = -1;
        public bool HasCurrent => _index >= 0 && _index < sortedEntries.Count;
        public KeyValuePair<string, byte[]> Current => sortedEntries[_index];

        public void MoveNext() => _index++;
        public Task MoveNextAsync() { MoveNext(); return Task.CompletedTask; }
    }

    /// <summary>
    /// Lazily paginates through a single shard's entries, optionally pre-fetching
    /// the next page in the background to hide grain-call latency. Accumulates
    /// reported <see cref="EntriesPage.MovedAwaySlots"/> into
    /// <paramref name="movedSlotSink"/> for the orchestrator's reconciliation.
    /// </summary>
    private sealed class EntriesCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse,
        bool prefetch,
        HashSet<int>? movedSlotSink,
        LatticePredicateNode? predicate) : IEntriesCursor
    {
        private List<KeyValuePair<string, byte[]>>? _page;
        private int _index;
        private bool _hasMore = true;
        private string? _continuation;
        private Task<EntriesPage>? _prefetchTask;

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
                EntriesPage result;
                if (_prefetchTask is not null)
                {
                    result = await _prefetchTask;
                    _prefetchTask = null;
                }
                else
                {
                    result = await FetchPageAsync(_continuation);
                }

                _page = result.Entries;
                _index = 0;
                _hasMore = result.HasMore;
                if (_page.Count > 0)
                    _continuation = _page[^1].Key;

                if (movedSlotSink is not null && result.MovedAwaySlots is { Length: > 0 } moved)
                {
                    foreach (var s in moved) movedSlotSink.Add(s);
                }

                // Kick off background fetch for the next page.
                if (prefetch && _hasMore)
                    _prefetchTask = FetchPageAsync(_continuation);
            }
        }

        private Task<EntriesPage> FetchPageAsync(string? continuation)
        {
            return reverse
                ? shard.GetSortedEntriesBatchReverseAsync(
                    startInclusive, endExclusive, pageSize, continuation, predicate)
                : shard.GetSortedEntriesBatchAsync(
                    startInclusive, endExclusive, pageSize, continuation, predicate);
        }
    }
}
