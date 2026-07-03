using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Strongly-consistent, globally-sorted key enumeration via paginated k-way
/// merge across shards with in-line reconciliation against concurrent
/// adaptive splits.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Pre-allocated descending-ordinal comparer used for reverse scans.
    /// Hoisted to avoid allocating a delegate + comparer on every scan call.
    /// </summary>
    private static readonly IComparer<string> ReverseOrdinal =
        Comparer<string>.Create(static (a, b) => string.Compare(b, a, StringComparison.Ordinal));

    /// <summary>
    /// Enumerates the live keys of this tree in strict lexicographic order
    /// (ascending, or descending when <paramref name="reverse"/> is <c>true</c>).
    /// <para>
    /// <b>Algorithm.</b> A streaming k-way merge across the physical shards
    /// owned by the start-of-scan shard map. Each shard filters out virtual
    /// slots that have been (or are being) moved to another physical shard by
    /// an adaptive split and reports them via
    /// <see cref="KeysPage.MovedAwaySlots"/>. Whenever the orchestrator
    /// observes (a) new moved slots reported by any cursor, or (b) a change
    /// in <see cref="ShardMap.Version"/> since the last reconciliation
    /// snapshot, it runs an <em>in-line reconciliation step</em> before the
    /// next dequeue: it queries the current owners for the newly-affected
    /// slots, loads the reconciled keys into memory, sorts them with the
    /// same comparer, and injects them as an additional cursor into the same
    /// priority queue. The merge invariant (global minimum is yielded next)
    /// then carries ordering across the topology boundary.
    /// </para>
    /// <para>
    /// <b>Ordering invariant (do not break).</b> This algorithm relies on
    /// <c>await cursors[idx].MoveNextAsync()</c> being the only await inside
    /// the merge loop body: because Orleans grain activations are
    /// single-threaded per turn, no other cursor can advance (and therefore
    /// no other key can be yielded) while a cursor's page fetch is in
    /// flight. Any <c>MovedAwaySlots</c> observed on the page fetched inside
    /// that await is therefore seen on the very next loop iteration, before
    /// the next dequeue. Any future refactor that moves cursor advances
    /// off-turn (e.g. truly concurrent prefetching across cursors) must
    /// re-establish this invariant or ordering will silently break.
    /// </para>
    /// <para>
    /// <b>Ordering.</b> Output is strictly sorted end-to-end, even when
    /// topology changes mid-scan. <b>Completeness.</b> A per-call
    /// <c>HashSet&lt;string&gt;</c> suppresses duplicates for keys that
    /// appear in both the pre- and post-swap views. <b>Liveness.</b>
    /// Reconciliation is bounded by
    /// <see cref="LatticeOptions.MaxScanRetries"/>; if the map keeps
    /// changing after every step, the method throws
    /// <see cref="InvalidOperationException"/>. <b>Memory.</b> The dedup
    /// set and per-step reconciliation buffer are bounded by the number of
    /// keys yielded plus the number of keys in reconciled slots - typically
    /// small, since only slots that actually moved during the scan are
    /// reconciled.
    /// </para>
    /// </summary>
    public IAsyncEnumerable<string> KeysAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        return KeysAsyncCore(startInclusive, endExclusive, reverse, prefetch, null, enforceAccessGate: true, cancellationToken);
    }

    public IAsyncEnumerable<string> KeysWherePredicateAsync(
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        return KeysAsyncCore(startInclusive, endExclusive, reverse, prefetch, predicate, enforceAccessGate: true, cancellationToken);
    }

    IAsyncEnumerable<string> ISystemLattice.KeysAsync(
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        CancellationToken cancellationToken)
        => KeysAsyncCore(startInclusive, endExclusive, reverse, prefetch, null, enforceAccessGate: false, cancellationToken);

    private async IAsyncEnumerable<string> KeysAsyncCore(
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        LatticePredicateNode? predicate,
        bool enforceAccessGate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Read-path access-gate key-filter. Resolved once per scan (before any
        // key is enumerated) so unauthorized keys are pruned server-side, at the
        // orchestrator's yield point, before any key crosses the grain boundary
        // to the caller. On the default path (null gate / system-origin) the
        // filter is null and the per-key loop below does no per-key work.
        // ISystemLattice callers pass enforceAccessGate: false so internal
        // machinery never self-filters.
        Func<string, bool>? keyFilter = enforceAccessGate
            ? await ResolveRangeReadKeyFilterAsync(startInclusive, endExclusive, cancellationToken)
            : null;
        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        var physicalShards = shardMap0.GetPhysicalShardIndices();
        var pageSize = Options.KeysPageSize;
        var usePrefetch = prefetch ?? Options.PrefetchKeysScan;
        var isSystemTree = TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

        // Streaming-scan isolation: capture one registry snapshot at
        // scan entry and pin it on the ambient
        // LatticeRegistrySnapshotContext for the lifetime of the
        // IAsyncEnumerable, so every per-shard page (including
        // reconciliation drains under concurrent splits) reads the
        // same decision view. System trees never carry sagas through
        // the TxRegistry, so the scope is a no-op there. A higher-level
        // caller (durable point-in-time cursor) that already set the
        // ambient retains its own snapshot - we never overwrite an
        // existing scope, so the durable-cursor pin always wins.
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

        // Live shard cursors - one per start-of-scan physical shard.
        // Per-shard ShardActivationRetry wrap on the initial MoveNextAsync:
        // a single shard's cold-start seed-timeout retries only that shard,
        // not the whole scan init.
        var movedReported = isSystemTree ? null : new HashSet<int>();
        var cursors = new List<IKeyCursor>(physicalShards.Count);
        var initTasks = new Task[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var sc = new ShardCursor(
                GetShardGrainByIndex(physicalTreeId, physicalShards[i]),
                startInclusive, endExclusive, pageSize, reverse, usePrefetch, movedReported, predicate);
            cursors.Add(sc);
            initTasks[i] = ShardActivationRetry.RunAsync(
                () => sc.MoveNextAsync());
        }
        await Task.WhenAll(initTasks);

        var pq = new PriorityQueue<int, string>(comparer);
        for (int i = 0; i < cursors.Count; i++)
        {
            if (cursors[i].HasCurrent)
                pq.Enqueue(i, cursors[i].Current!);
        }

        // System trees never split - skip reconciliation entirely.
        // For non-system trees the cross-cursor dedup set grows to roughly
        // the scan window size (~hundreds-to-thousands of keys). Pre-sizing
        // to `pageSize` saves the small-end resize chain
        // (0 -> 3 -> 7 -> 17 -> 37 -> ...) which dominates the HashSet
        // allocation cost for medium scans without committing to a large
        // upfront allocation for tiny scans. The natural bound is one full
        // page-fetch worth of keys, which is also tunable via
        // `LatticeOptions.KeysPageSize`.
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

        // Reconciliation state (unused for system trees).
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
            // Reconciliation check runs BEFORE each dequeue. This preserves
            // the k-way merge invariant: when a moved slot is first reported
            // by a shard's page, the corresponding live cursor's Current is
            // the new post-filter position, which is >= any previously
            // yielded key. Injecting the reconciled cursor now places its
            // keys into the priority queue so they participate in the merge
            // from the correct lex position forward.
            if (!isSystemTree && HasNewMovedSlots(movedReported!, coveredSlots!, reconciledCount))
            {
                if (retriesUsed >= maxRetries)
                {
                    throw new InvalidOperationException(
                        $"KeysAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
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
                    var buffer = await DrainSlotsToBufferAsync(
                        physicalTreeId, startInclusive, endExclusive, pageSize,
                        needSlots, shardMapNow, virtualShardCount, yielded, predicate,
                        comparer, lastYieldedKey);

                    foreach (var s in needSlots) coveredSlots!.Add(s);

                    if (buffer.Count > 0)
                    {
                        buffer.Sort(comparer);
                        var memCursor = new MemoryKeyCursor(buffer);
                        cursors.Add(memCursor);
                        memCursor.MoveNext();
                        if (memCursor.HasCurrent)
                            pq.Enqueue(cursors.Count - 1, memCursor.Current!);
                    }
                }

                reconciledCount = movedReported!.Count;
            }

            if (pq.Count == 0)
            {
                if (isSystemTree) yield break;

                // Final stability check: catch splits that committed after
                // all live cursors drained (so no MovedAwaySlots were ever
                // reported by a live shard). Re-read the map and, if it has
                // advanced since the last snapshot we saw, reconcile the
                // owner diff one more time.
                registry ??= grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                var shardMapFinal = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
                if (shardMapFinal.Version == lastMapVersion) yield break;

                if (retriesUsed >= maxRetries)
                {
                    throw new InvalidOperationException(
                        $"KeysAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
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

                var finalBuffer = await DrainSlotsToBufferAsync(
                    physicalTreeId, startInclusive, endExclusive, pageSize,
                    finalNeedSlots, shardMapFinal, virtualShardCount, yielded, predicate,
                    comparer, lastYieldedKey);

                foreach (var s in finalNeedSlots) coveredSlots!.Add(s);

                if (finalBuffer.Count == 0) continue; // loop back; pq empty → re-check stability.

                finalBuffer.Sort(comparer);
                var finalCursor = new MemoryKeyCursor(finalBuffer);
                cursors.Add(finalCursor);
                finalCursor.MoveNext();
                if (finalCursor.HasCurrent)
                    pq.Enqueue(cursors.Count - 1, finalCursor.Current!);
                continue;
            }

            var idx = pq.Dequeue();
            var key = cursors[idx].Current!;

            // Cross-cursor dedup: the same key may appear in an injected
            // memory cursor if the old owner produced it before the split
            // committed. Suppress silently.
            if (yielded is null || yielded.Add(key))
            {
                // Advance the merge frontier for every key the merge passes
                // (used by reconciliation to drop drained keys behind it),
                // independently of the access-gate visibility prune, so
                // ordering/dedup bookkeeping stays correct even when a key is
                // hidden from the caller. The filter is a caller-visibility
                // prune only.
                lastYieldedKey = key;
                if (keyFilter is null || keyFilter(key))
                    yield return key;
            }

            await cursors[idx].MoveNextAsync();
            if (cursors[idx].HasCurrent)
                pq.Enqueue(idx, cursors[idx].Current!);
        }
    }

    /// <summary>
    /// Drains the current owners of the given virtual <paramref name="needSlots"/>
    /// into a flat list, suppressing keys already yielded by an earlier cursor.
    /// </summary>
    private async Task<List<string>> DrainSlotsToBufferAsync(
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
        var buffer = new List<string>();
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
                var page = await shard.GetSortedKeysBatchForSlotsAsync(
                    startInclusive, endExclusive, pageSize, continuation,
                    sortedSlots, virtualShardCount, predicate);

                foreach (var k in page.Keys)
                {
                    if (suppressBehindFrontier && comparer.Compare(k, frontier!) <= 0)
                        continue;
                    // If a key was already yielded by an earlier cursor,
                    // skip it so the injected cursor never re-yields.
                    if (yielded is null || !yielded.Contains(k))
                        buffer.Add(k);
                }

                if (!page.HasMore) break;
                if (page.Keys.Count == 0) break;
                continuation = page.Keys[^1];
            }
        }
        return buffer;
    }

    /// <summary>
    /// Uniform interface over live shard cursors and injected memory cursors
    /// so the k-way merge loop can treat both the same way.
    /// </summary>
    private interface IKeyCursor
    {
        bool HasCurrent { get; }
        string? Current { get; }
        Task MoveNextAsync();
    }

    /// <summary>
    /// Cursor backed by a pre-sorted in-memory list of keys produced by a
    /// reconciliation step. Drained synchronously - <see cref="MoveNextAsync"/>
    /// never awaits.
    /// </summary>
    private sealed class MemoryKeyCursor(List<string> sortedKeys) : IKeyCursor
    {
        private int _index = -1;
        public bool HasCurrent => _index >= 0 && _index < sortedKeys.Count;
        public string? Current => HasCurrent ? sortedKeys[_index] : null;

        public void MoveNext() => _index++;
        public Task MoveNextAsync() { MoveNext(); return Task.CompletedTask; }
    }

    /// <summary>
    /// Lazily paginates through a single shard's keys, optionally pre-fetching
    /// the next page in the background to hide grain-call latency. Accumulates
    /// the union of <see cref="KeysPage.MovedAwaySlots"/> reported by every
    /// page consumed into <paramref name="movedSlotSink"/> for the
    /// orchestrator's reconciliation.
    /// </summary>
    private sealed class ShardCursor(
        IShardRootGrain shard,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        bool reverse,
        bool prefetch,
        HashSet<int>? movedSlotSink,
        LatticePredicateNode? predicate) : IKeyCursor
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
                    startInclusive, endExclusive, pageSize, continuation, predicate)
                : shard.GetSortedKeysBatchAsync(
                    startInclusive, endExclusive, pageSize, continuation, predicate);
        }
    }
}
