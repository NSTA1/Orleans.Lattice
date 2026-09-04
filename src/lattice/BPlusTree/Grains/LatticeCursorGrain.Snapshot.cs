using System.Buffers.Binary;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Zero-observable-writes snapshot-cursor partial. Hosts the
/// <see cref="ILatticeCursorGrain.OpenSnapshotAsync"/> entry point, the
/// snapshot-aware <c>Next*Async</c> page paths that fan out across per-shard
/// <see cref="ISnapshotLeafGrain"/> activations, and the WAL retention pin
/// lifecycle that registers/refreshes/releases the snapshot cursor's
/// consumer entry on <see cref="IWalCursorRegistry"/>.
/// </summary>
internal sealed partial class LatticeCursorGrain
{
    /// <summary>
    /// In-memory marker that tracks whether the WAL retention pin
    /// gauge has been incremented for this activation. Pin reports
    /// happen on every <c>Next*Async</c> page (slide semantics), so
    /// the bookkeeping flag prevents double-increments. Reset on
    /// activation; the first successful report on a reactivated
    /// cursor will re-increment the gauge.
    /// </summary>
    private bool _snapshotPinGaugeHeld;

    /// <summary>
    /// Per-activation cache of the pinned shard map's owned-slot sets,
    /// keyed by physical shard index. Lazily built on first page from
    /// <see cref="LatticeSnapshotCoordinate.PinnedShardMap"/> and reused
    /// across every page of the cursor's lifetime; rebuilt after a
    /// reactivation from the persisted coordinate. <see langword="null"/>
    /// until first built, and the build yields an empty map / null
    /// per-shard sets when the coordinate carries no pinned map (single-
    /// shard or legacy snapshot - no donor-orphan filtering required).
    /// </summary>
    private Dictionary<int, int[]>? _ownedSlotsByShard;

    /// <summary>Virtual shard count backing <see cref="_ownedSlotsByShard"/>.</summary>
    private int _ownedSlotsVsc;

    /// <summary>True once <see cref="_ownedSlotsByShard"/> has been built for this activation.</summary>
    private bool _ownedSlotsBuilt;

    /// <summary>
    /// Resolves the owned virtual-slot set the snapshot leaf for
    /// <paramref name="shardIndex"/> must filter against, derived from the
    /// coordinate's pinned shard map. Returns <c>(null, 0)</c> when the
    /// coordinate carries no pinned map, disabling the leaf-side filter.
    /// The per-shard sets are computed once per activation and cached.
    /// </summary>
    private (IReadOnlyList<int>? OwnedSlots, int VirtualShardCount) ResolveOwnedSlots(
        LatticeSnapshotCoordinate coord, int shardIndex)
    {
        if (!_ownedSlotsBuilt)
        {
            var pinned = coord.PinnedShardMap;
            if (pinned is not null && pinned.Slots.Length > 0)
            {
                _ownedSlotsByShard = BuildOwnedSlotsByShard(pinned.Slots);
                _ownedSlotsVsc = pinned.VirtualShardCount;
            }
            _ownedSlotsBuilt = true;
        }

        if (_ownedSlotsByShard is null)
            return (null, 0);

        // A shard that appears in the fan-out but owns no slot under the
        // pinned map surfaces nothing (any key it holds is an orphan); the
        // empty array makes every key fail the ownership check.
        return _ownedSlotsByShard.TryGetValue(shardIndex, out var owned)
            ? (owned, _ownedSlotsVsc)
            : (Array.Empty<int>(), _ownedSlotsVsc);
    }

    /// <summary>
    /// Partitions a pinned shard map's slot array into per-shard ascending
    /// owned-slot lists. The forward scan over <paramref name="slots"/>
    /// produces each shard's list already sorted ascending, so the snapshot
    /// leaf can binary-search it.
    /// </summary>
    private static Dictionary<int, int[]> BuildOwnedSlotsByShard(int[] slots)
    {
        var lists = new Dictionary<int, List<int>>();
        for (var slot = 0; slot < slots.Length; slot++)
        {
            var shard = slots[slot];
            if (!lists.TryGetValue(shard, out var list))
            {
                list = new List<int>();
                lists[shard] = list;
            }
            list.Add(slot);
        }

        var result = new Dictionary<int, int[]>(lists.Count);
        foreach (var (shard, list) in lists)
            result[shard] = list.ToArray();
        return result;
    }
    /// <inheritdoc />
    public async Task OpenSnapshotAsync(string treeId, LatticeCursorSpec spec, LatticeSnapshotCoordinate coordinate)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (!spec.ZeroObservableWrites)
        {
            throw new ArgumentException(
                "OpenSnapshotAsync requires a LatticeCursorSpec with ZeroObservableWrites = true.",
                nameof(spec));
        }
        if (spec.Kind == LatticeCursorKind.DeleteRange)
        {
            throw new ArgumentException(
                "Snapshot cursors are read-only; DeleteRange is not supported under ZeroObservableWrites.",
                nameof(spec));
        }

        if (state.State.Phase == LatticeCursorPhase.NotStarted)
        {
            var prevTreeId = state.State.TreeId;
            var prevSpec = state.State.Spec;
            var prevPhase = state.State.Phase;
            var prevSnapshot = state.State.PointInTimeSnapshot;
            var prevPinId = state.State.SnapshotPinId;
            var prevCoord = state.State.SnapshotCoordinate;

            state.State.TreeId = treeId;
            state.State.Spec = spec;
            state.State.Phase = LatticeCursorPhase.Open;
            state.State.PointInTimeSnapshot = null;
            state.State.SnapshotPinId = Guid.NewGuid();
            state.State.SnapshotCoordinate = coordinate;

            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.TreeId = prevTreeId;
                state.State.Spec = prevSpec;
                state.State.Phase = prevPhase;
                state.State.PointInTimeSnapshot = prevSnapshot;
                state.State.SnapshotPinId = prevPinId;
                state.State.SnapshotCoordinate = prevCoord;
                throw;
            }

            await TryReportSnapshotPinAsync();
            await SlideTtlAsync();
            return;
        }

        if (state.State.TreeId != treeId
            || !state.State.Spec.Equals(spec)
            || !Nullable.Equals(state.State.SnapshotCoordinate, coordinate))
        {
            throw new InvalidOperationException(
                $"Snapshot cursor '{CursorKey}' is already open with a different specification or snapshot coordinate.");
        }

        await TryReportSnapshotPinAsync();
        await SlideTtlAsync();
    }

    /// <summary>
    /// Re-reports this cursor's WAL retention pin against the
    /// registry. Best-effort: a registry failure is logged and
    /// swallowed; the snapshot continues to read locally and trim
    /// safety degrades to the GC's other predicate branches.
    /// </summary>
    private async Task TryReportSnapshotPinAsync()
    {
        if (state.State.SnapshotCoordinate is not { } coord) return;
        var registry = WalCursorRegistry;
        if (registry is null) return;

        try
        {
            var pinHlc = coord.RegistrySnapshotHlc;
            await registry.ReportCursorAsync(
                state.State.TreeId,
                SnapshotConsumerId,
                pinHlc,
                blockedAtHlc: pinHlc > HybridLogicalClock.Zero ? pinHlc : null);
            if (!_snapshotPinGaugeHeld)
            {
                LatticeMetrics.SnapshotPinCount.Add(1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
                    LatticeTenantLabel.ForTree(state.State.TreeId));
                _snapshotPinGaugeHeld = true;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Snapshot cursor {CursorKey}: failed to report WAL retention pin; the WAL GC may trim past the captured offset.",
                CursorKey);
        }
    }

    /// <summary>
    /// Unregisters this cursor's WAL retention pin. Called from
    /// the close path and TTL expiry so the pin does not outlive
    /// the cursor.
    /// </summary>
    private async Task TryUnregisterSnapshotPinAsync()
    {
        if (state.State.SnapshotCoordinate is null) return;
        var registry = WalCursorRegistry;
        if (registry is null) return;

        try
        {
            await registry.UnregisterAsync(state.State.TreeId, SnapshotConsumerId);
            if (_snapshotPinGaugeHeld)
            {
                LatticeMetrics.SnapshotPinCount.Add(-1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
                    LatticeTenantLabel.ForTree(state.State.TreeId));
                _snapshotPinGaugeHeld = false;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Snapshot cursor {CursorKey}: failed to unregister WAL retention pin; the pin will fall out via its own TTL.",
                CursorKey);
        }
    }

    /// <summary>
    /// Deletes every per-shard frozen baseline this snapshot cursor captured at
    /// open time. Called from the close path and the idle-TTL expiry so a
    /// baseline never outlives its cursor. Best-effort: a clear failure leaves
    /// an orphaned baseline row that is harmless (it is keyed by a token no
    /// other cursor reuses) and can be reclaimed by storage GC. No-op for a
    /// legacy coordinate that carries no baseline token.
    /// </summary>
    private async Task TryDeleteSnapshotBaselinesAsync()
    {
        if (state.State.SnapshotCoordinate is not { } coord) return;
        if (coord.SnapshotBaselineToken == Guid.Empty) return;

        // A single-page scan never flushed its baselines to durable storage (they
        // lived only in the transient snapshot leaves' memory and are gone with
        // the cursor), so there is nothing to delete and issuing a ClearAsync per
        // shard would be pure write-amplification - exactly what issue #916
        // removes. Only a cursor that paged past page 1 (and therefore set this
        // flag while persisting) has durable rows to reclaim here.
        if (!state.State.SnapshotBaselinePersisted) return;

        var treeId = SnapshotLeafTreeId(coord);
        foreach (var shardIndex in coord.PerShardWalOffsets.Keys)
        {
            try
            {
                var baselineGrain = grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(
                    SnapshotLeafGrain.BuildBaselineKey(treeId, shardIndex, coord.SnapshotBaselineToken));
                await baselineGrain.ClearAsync(default);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex,
                    "Snapshot cursor {CursorKey}: failed to delete frozen baseline for shard {ShardIndex}; the row is keyed by a per-open token and will be reclaimed by storage GC.",
                    CursorKey, shardIndex);
            }
        }
    }

    /// <summary>
    /// Durably flushes every per-shard frozen baseline this snapshot cursor
    /// seeded in memory at open. Called the first time a page returns
    /// <c>HasMore = true</c>, BEFORE the cursor's advanced position is persisted
    /// and the page is returned, so the client only ever observes a continuation
    /// token after every shard's baseline is durable (issue #916). This upholds
    /// the failover-durability invariant for any cursor that survives past page
    /// 1, while a single-page scan (which never reaches here) pays zero storage
    /// writes. No-op for a legacy coordinate that carries no baseline token, or
    /// once already persisted (idempotent and cheap on the leaf side).
    /// </summary>
    private async Task EnsureSnapshotBaselinesPersistedAsync(LatticeSnapshotCoordinate coord)
    {
        if (coord.SnapshotBaselineToken == Guid.Empty) return;

        var shards = ResolvePerShardPerPartitionOffsets(coord);
        var tasks = new List<Task>(shards.Count);
        foreach (var (shardIdx, _) in shards)
        {
            var leaf = grainFactory.GetGrain<ISnapshotLeafGrain>(BuildSnapshotLeafKey(coord, shardIdx));
            tasks.Add(leaf.EnsurePersistedAsync(default));
        }
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Snapshot-aware keys page. Fans out across per-shard snapshot
    /// leaves indicated by the persisted coordinate, k-way merges
    /// their sorted slices, and persists the new cursor position.
    /// </summary>
    private async Task<LatticeCursorKeysPage> NextSnapshotKeysAsync(int pageSize)
    {
        var coord = state.State.SnapshotCoordinate!.Value;
        var (effStart, effEnd) = ComputeEffectiveRange();
        var reverse = state.State.Spec.Reverse;

        var perShardLists = await FetchPerShardKeysAsync(coord, effStart, effEnd, pageSize, state.State.Spec.Predicate, reverse);
        var collected = new List<string>(PageBufferCapacity(pageSize));
        MergeSortedKeyLists(perShardLists, reverse, pageSize, collected);

        // Pagination (continuation key + hasMore) is computed from the merged
        // page BEFORE the authorization filter is applied, so a page whose keys
        // are mostly (or wholly) pruned does not prematurely exhaust the cursor
        // and hide authorized keys on later pages. The returned page is then
        // pruned to the keys the caller may observe.
        var hasMore = collected.Count >= pageSize && AnyShardHasRemaining(perShardLists, pageSize);
        var continuationKey = collected.Count > 0 ? collected[^1] : null;

        // Snapshot cursors read snapshot leaf grains directly, bypassing the
        // public filtered scan surface, so re-apply the caller's read-path
        // key-filter here (fail-closed: a full deny prunes every key).
        var snapshotKeyFilter = await ResolveSnapshotKeyFilterAsync(effStart, effEnd);
        if (snapshotKeyFilter is not null)
        {
            collected.RemoveAll(k => !snapshotKeyFilter(k));
        }

        var prevLastYieldedKey = state.State.LastYieldedKey;
        var prevPhase = state.State.Phase;
        var prevPersisted = state.State.SnapshotBaselinePersisted;
        if (continuationKey is not null)
        {
            state.State.LastYieldedKey = continuationKey;
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        else if (!state.State.SnapshotBaselinePersisted)
        {
            // The scan must survive past this page, so durably flush the frozen
            // baselines the shard roots seeded in memory before the cursor's
            // advance is persisted - the client must not see a continuation
            // token whose baselines a failover could lose.
            await EnsureSnapshotBaselinesPersistedAsync(coord);
            state.State.SnapshotBaselinePersisted = true;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.LastYieldedKey = prevLastYieldedKey;
            state.State.Phase = prevPhase;
            state.State.SnapshotBaselinePersisted = prevPersisted;
            throw;
        }
        await TryReportSnapshotPinAsync();
        await SlideTtlAsync();

        return new LatticeCursorKeysPage { Keys = collected, HasMore = hasMore };
    }

    /// <summary>
    /// Snapshot-aware entries page. Same shape as
    /// <see cref="NextSnapshotKeysAsync"/> but k-way merges
    /// key/value pairs.
    /// </summary>
    private async Task<LatticeCursorEntriesPage> NextSnapshotEntriesAsync(int pageSize)
    {
        var coord = state.State.SnapshotCoordinate!.Value;
        var (effStart, effEnd) = ComputeEffectiveRange();
        var reverse = state.State.Spec.Reverse;

        var perShardLists = await FetchPerShardEntriesAsync(coord, effStart, effEnd, pageSize, state.State.Spec.Predicate, reverse);
        var collected = new List<KeyValuePair<string, byte[]>>(PageBufferCapacity(pageSize));
        MergeSortedEntryLists(perShardLists, reverse, pageSize, collected);

        // See NextSnapshotKeysAsync: pagination is computed from the pre-filter
        // merged page so a pruned page does not prematurely exhaust the cursor.
        var hasMore = collected.Count >= pageSize && AnyShardEntryHasRemaining(perShardLists, pageSize);
        var continuationKey = collected.Count > 0 ? collected[^1].Key : null;

        // Re-apply the caller's read-path key-filter here because the snapshot
        // leaf reads bypass the public filtered surface.
        var snapshotEntryFilter = await ResolveSnapshotKeyFilterAsync(effStart, effEnd);
        if (snapshotEntryFilter is not null)
        {
            collected.RemoveAll(e => !snapshotEntryFilter(e.Key));
        }

        var prevLastYieldedKey = state.State.LastYieldedKey;
        var prevPhase = state.State.Phase;
        var prevPersisted = state.State.SnapshotBaselinePersisted;
        if (continuationKey is not null)
        {
            state.State.LastYieldedKey = continuationKey;
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        else if (!state.State.SnapshotBaselinePersisted)
        {
            // See NextSnapshotKeysAsync: flush the seeded baselines durably
            // before the continuation token escapes to the client.
            await EnsureSnapshotBaselinesPersistedAsync(coord);
            state.State.SnapshotBaselinePersisted = true;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.LastYieldedKey = prevLastYieldedKey;
            state.State.Phase = prevPhase;
            state.State.SnapshotBaselinePersisted = prevPersisted;
            throw;
        }
        await TryReportSnapshotPinAsync();
        await SlideTtlAsync();

        return new LatticeCursorEntriesPage { Entries = collected, HasMore = hasMore };
    }

    /// <summary>
    /// Raw-entry companion of <see cref="NextSnapshotEntriesAsync"/>. Drains the
    /// same pinned point-in-time cut but k-way merges <see cref="LwwEntry"/>
    /// carriers so the causal envelope (HLC timestamp, tombstone flag, expiry,
    /// origin cluster id, version vector) reaches the backup capture engine
    /// intact. Progress bookkeeping (continuation key, exhaustion, baseline
    /// flush, TTL / pin refresh) is identical to the projection path.
    /// </summary>
    private async Task<LatticeCursorRawEntriesPage> NextSnapshotRawEntriesAsync(int pageSize)
    {
        var coord = state.State.SnapshotCoordinate!.Value;
        var (effStart, effEnd) = ComputeEffectiveRange();
        var reverse = state.State.Spec.Reverse;

        var perShardLists = await FetchPerShardRawEntriesAsync(coord, effStart, effEnd, pageSize, state.State.Spec.Predicate, reverse);
        var collected = new List<LwwEntry>(PageBufferCapacity(pageSize));
        MergeSortedRawEntryLists(perShardLists, reverse, pageSize, collected);

        var hasMore = collected.Count >= pageSize && AnyShardRawEntryHasRemaining(perShardLists, pageSize);
        var continuationKey = collected.Count > 0 ? collected[^1].Key : null;

        // Re-apply the caller's read-path key-filter here because the snapshot
        // leaf reads bypass the public filtered surface.
        var snapshotEntryFilter = await ResolveSnapshotKeyFilterAsync(effStart, effEnd);
        if (snapshotEntryFilter is not null)
        {
            collected.RemoveAll(e => !snapshotEntryFilter(e.Key));
        }

        var prevLastYieldedKey = state.State.LastYieldedKey;
        var prevPhase = state.State.Phase;
        var prevPersisted = state.State.SnapshotBaselinePersisted;
        if (continuationKey is not null)
        {
            state.State.LastYieldedKey = continuationKey;
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        else if (!state.State.SnapshotBaselinePersisted)
        {
            await EnsureSnapshotBaselinesPersistedAsync(coord);
            state.State.SnapshotBaselinePersisted = true;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.LastYieldedKey = prevLastYieldedKey;
            state.State.Phase = prevPhase;
            state.State.SnapshotBaselinePersisted = prevPersisted;
            throw;
        }
        await TryReportSnapshotPinAsync();
        await SlideTtlAsync();

        return new LatticeCursorRawEntriesPage { Entries = collected, HasMore = hasMore };
    }
    /// <summary>
    /// Fans a snapshot key page out across every physical shard the snapshot
    /// coordinate covers. Concurrent activation of per-shard snapshot
    /// leaves is intentional - replay is read-only and CPU-bound, so
    /// the fan-out reduces wall-clock per-page latency. Any per-shard
    /// failure is surfaced unwrapped to the caller (no special
    /// trim-detection translation today; the snapshot leaf currently
    /// reports trim by surfacing the underlying coordinator failure).
    /// </summary>
    private async Task<List<string>[]> FetchPerShardKeysAsync(
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize,
        LatticePredicateNode? predicate,
        bool reverse)
    {
        var shards = ResolvePerShardPerPartitionOffsets(coord);
        var tasks = new Task<List<string>>[shards.Count];
        var index = 0;
        foreach (var (shardIdx, capturedOffsets) in shards)
        {
            tasks[index++] = FetchSnapshotShardKeysAsync(shardIdx, capturedOffsets, coord, effStart, effEnd, pageSize, predicate, reverse);
        }

        return await Task.WhenAll(tasks);
    }

    private async Task<List<string>> FetchSnapshotShardKeysAsync(
        int shardIndex,
        IReadOnlyList<long> capturedOffsetsByPartition,
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize,
        LatticePredicateNode? predicate,
        bool reverse)
    {
        var leaf = grainFactory.GetGrain<ISnapshotLeafGrain>(BuildSnapshotLeafKey(coord, shardIndex));
        var (ownedSlots, vsc) = ResolveOwnedSlots(coord, shardIndex);
        await leaf.OpenAsync(SnapshotLeafTreeId(coord), shardIndex, capturedOffsetsByPartition, ownedSlots, vsc, coord.SnapshotBaselineToken, default);
        return await leaf.GetKeysAsync(effStart, effEnd, limit: pageSize, predicate: predicate, reverse: reverse);
    }

    private async Task<List<KeyValuePair<string, byte[]>>> FetchSnapshotShardEntriesAsync(
        int shardIndex,
        IReadOnlyList<long> capturedOffsetsByPartition,
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize,
        LatticePredicateNode? predicate,
        bool reverse)
    {
        var leaf = grainFactory.GetGrain<ISnapshotLeafGrain>(BuildSnapshotLeafKey(coord, shardIndex));
        var (ownedSlots, vsc) = ResolveOwnedSlots(coord, shardIndex);
        await leaf.OpenAsync(SnapshotLeafTreeId(coord), shardIndex, capturedOffsetsByPartition, ownedSlots, vsc, coord.SnapshotBaselineToken, default);
        return await leaf.GetEntriesAsync(effStart, effEnd, limit: pageSize, predicate: predicate, reverse: reverse);
    }

    /// <summary>
    /// Entries-shape companion of <see cref="FetchPerShardKeysAsync"/>.
    /// </summary>
    private async Task<List<KeyValuePair<string, byte[]>>[]> FetchPerShardEntriesAsync(
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize,
        LatticePredicateNode? predicate,
        bool reverse)
    {
        var shards = ResolvePerShardPerPartitionOffsets(coord);
        var tasks = new Task<List<KeyValuePair<string, byte[]>>>[shards.Count];
        var index = 0;
        foreach (var (shardIdx, capturedOffsets) in shards)
        {
            tasks[index++] = FetchSnapshotShardEntriesAsync(shardIdx, capturedOffsets, coord, effStart, effEnd, pageSize, predicate, reverse);
        }

        return await Task.WhenAll(tasks);
    }

    private async Task<List<LwwEntry>> FetchSnapshotShardRawEntriesAsync(
        int shardIndex,
        IReadOnlyList<long> capturedOffsetsByPartition,
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize,
        LatticePredicateNode? predicate,
        bool reverse)
    {
        var leaf = grainFactory.GetGrain<ISnapshotLeafGrain>(BuildSnapshotLeafKey(coord, shardIndex));
        var (ownedSlots, vsc) = ResolveOwnedSlots(coord, shardIndex);
        await leaf.OpenAsync(SnapshotLeafTreeId(coord), shardIndex, capturedOffsetsByPartition, ownedSlots, vsc, coord.SnapshotBaselineToken, default);
        return await leaf.GetRawEntriesAsync(effStart, effEnd, limit: pageSize, predicate: predicate, reverse: reverse);
    }

    /// <summary>
    /// Raw-entry companion of <see cref="FetchPerShardEntriesAsync"/>.
    /// </summary>
    private async Task<List<LwwEntry>[]> FetchPerShardRawEntriesAsync(
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize,
        LatticePredicateNode? predicate,
        bool reverse)
    {
        var shards = ResolvePerShardPerPartitionOffsets(coord);
        var tasks = new Task<List<LwwEntry>>[shards.Count];
        var index = 0;
        foreach (var (shardIdx, capturedOffsets) in shards)
        {
            tasks[index++] = FetchSnapshotShardRawEntriesAsync(shardIdx, capturedOffsets, coord, effStart, effEnd, pageSize, predicate, reverse);
        }

        return await Task.WhenAll(tasks);
    }
    /// <summary>
    /// Resolves the per-shard, per-partition WAL offsets the snapshot
    /// leaves replay against. Prefers
    /// <see cref="LatticeSnapshotCoordinate.PerShardPerPartitionWalOffsets"/>
    /// when non-null (multi-partition capture path); falls back to
    /// wrapping each scalar offset in
    /// <see cref="LatticeSnapshotCoordinate.PerShardWalOffsets"/> as a
    /// single-element list for legacy single-partition coordinates
    /// persisted before the per-partition slot was introduced.
    /// </summary>
    private static IReadOnlyDictionary<int, IReadOnlyList<long>> ResolvePerShardPerPartitionOffsets(LatticeSnapshotCoordinate coord)
    {
        if (coord.PerShardPerPartitionWalOffsets is { } perPartition)
            return perPartition;
        var legacy = coord.PerShardWalOffsets;
        var promoted = new Dictionary<int, IReadOnlyList<long>>(legacy.Count);
        foreach (var (shard, offset) in legacy)
        {
            promoted[shard] = new[] { offset };
        }
        return promoted;
    }

    /// <summary>
    /// k-way merge across pre-sorted per-shard key slices.
    /// </summary>
    private static void MergeSortedKeyLists(
        List<string>[] perShard,
        bool reverse,
        int pageSize,
        List<string> output)
    {
        var cursors = new int[perShard.Length];
        for (var s = 0; s < perShard.Length; s++)
        {
            cursors[s] = reverse ? perShard[s].Count - 1 : 0;
        }

        while (output.Count < pageSize)
        {
            var pickShard = -1;
            string? pickKey = null;
            for (var s = 0; s < perShard.Length; s++)
            {
                var c = cursors[s];
                if (reverse)
                {
                    if (c < 0) continue;
                }
                else
                {
                    if (c >= perShard[s].Count) continue;
                }
                var candidate = perShard[s][c];
                if (pickKey is null)
                {
                    pickKey = candidate;
                    pickShard = s;
                    continue;
                }
                var cmp = string.CompareOrdinal(candidate, pickKey);
                if (reverse ? cmp > 0 : cmp < 0)
                {
                    pickKey = candidate;
                    pickShard = s;
                }
            }
            if (pickShard < 0) return;
            var picked = perShard[pickShard][cursors[pickShard]];
            cursors[pickShard] += reverse ? -1 : 1;
            // Defensive de-duplication: the leaf-side donor-orphan filter
            // already guarantees each key is owned by exactly one shard, so
            // equal keys should never reach this merge. If a residual cross-
            // shard duplicate ever does, equal keys are adjacent in the
            // sorted stream, so collapsing against the last emitted key drops
            // it. This is a safety net only - value correctness comes from
            // the leaf-side filter, not from this guard.
            if (output.Count > 0 && string.CompareOrdinal(output[^1], picked) == 0)
                continue;
            output.Add(picked);
        }
    }

    /// <summary>
    /// Entries-shape companion of <see cref="MergeSortedKeyLists"/>.
    /// </summary>
    private static void MergeSortedEntryLists(
        List<KeyValuePair<string, byte[]>>[] perShard,
        bool reverse,
        int pageSize,
        List<KeyValuePair<string, byte[]>> output)
    {
        var cursors = new int[perShard.Length];
        for (var s = 0; s < perShard.Length; s++)
        {
            cursors[s] = reverse ? perShard[s].Count - 1 : 0;
        }

        while (output.Count < pageSize)
        {
            var pickShard = -1;
            string? pickKey = null;
            for (var s = 0; s < perShard.Length; s++)
            {
                var c = cursors[s];
                if (reverse)
                {
                    if (c < 0) continue;
                }
                else
                {
                    if (c >= perShard[s].Count) continue;
                }
                var candidate = perShard[s][c].Key;
                if (pickKey is null)
                {
                    pickKey = candidate;
                    pickShard = s;
                    continue;
                }
                var cmp = string.CompareOrdinal(candidate, pickKey);
                if (reverse ? cmp > 0 : cmp < 0)
                {
                    pickKey = candidate;
                    pickShard = s;
                }
            }
            if (pickShard < 0) return;
            var picked = perShard[pickShard][cursors[pickShard]];
            cursors[pickShard] += reverse ? -1 : 1;
            // Defensive de-duplication: see MergeSortedKeyLists. The leaf-side
            // donor-orphan filter is the real fix; this only guards against a
            // residual cross-shard duplicate reaching the client.
            if (output.Count > 0 && string.CompareOrdinal(output[^1].Key, picked.Key) == 0)
                continue;
            output.Add(picked);
        }
    }

    private static bool AnyShardHasRemaining(List<string>[] perShard, int pageSize)
    {
        for (var s = 0; s < perShard.Length; s++)
        {
            // "hasMore" must be true whenever any shard returned at
            // least one entry on this fetch, because under multi-
            // shard merging some shards' entries may sit beyond the
            // current page's cap and only get picked by the next
            // fetch (after effStart advances past the just-yielded
            // last key). The pre-multi-partition predicate compared
            // against pageSize, which was safe under the legacy
            // single-shard-WAL routing (each shard's snapshot leaf
            // saw ALL its shard's entries in one pass and either
            // returned a full page or was the only shard with data),
            // but stranded keys under multi-shard balanced fan-out:
            // 4 shards x 2 keys each at pageSize=2 would yield the
            // first two keys but report hasMore=false even though
            // the remaining 6 still lived in the per-shard fetches.
            // The next-fetch advance handles termination correctly:
            // a shard whose entries all sit at-or-below the merge's
            // last yielded key returns an empty fetch on the next
            // call, the merge yields zero new keys, and the cursor
            // exhausts naturally.
            if (perShard[s].Count > 0) return true;
        }
        return false;
    }

    private static bool AnyShardEntryHasRemaining(List<KeyValuePair<string, byte[]>>[] perShard, int pageSize)
    {
        for (var s = 0; s < perShard.Length; s++)
        {
            // See AnyShardHasRemaining for the rationale.
            if (perShard[s].Count > 0) return true;
        }
        return false;
    }

    /// <summary>
    /// Raw-entry companion of <see cref="MergeSortedEntryLists"/>: k-way merges
    /// per-shard <see cref="LwwEntry"/> runs on <see cref="LwwEntry.Key"/>.
    /// </summary>
    private static void MergeSortedRawEntryLists(
        List<LwwEntry>[] perShard,
        bool reverse,
        int pageSize,
        List<LwwEntry> output)
    {
        var cursors = new int[perShard.Length];
        for (var s = 0; s < perShard.Length; s++)
        {
            cursors[s] = reverse ? perShard[s].Count - 1 : 0;
        }

        while (output.Count < pageSize)
        {
            var pickShard = -1;
            string? pickKey = null;
            for (var s = 0; s < perShard.Length; s++)
            {
                var c = cursors[s];
                if (reverse)
                {
                    if (c < 0) continue;
                }
                else
                {
                    if (c >= perShard[s].Count) continue;
                }
                var candidate = perShard[s][c].Key;
                if (pickKey is null)
                {
                    pickKey = candidate;
                    pickShard = s;
                    continue;
                }
                var cmp = string.CompareOrdinal(candidate, pickKey);
                if (reverse ? cmp > 0 : cmp < 0)
                {
                    pickKey = candidate;
                    pickShard = s;
                }
            }
            if (pickShard < 0) return;
            var picked = perShard[pickShard][cursors[pickShard]];
            cursors[pickShard] += reverse ? -1 : 1;
            if (output.Count > 0 && string.CompareOrdinal(output[^1].Key, picked.Key) == 0)
                continue;
            output.Add(picked);
        }
    }

    private static bool AnyShardRawEntryHasRemaining(List<LwwEntry>[] perShard, int pageSize)
    {
        for (var s = 0; s < perShard.Length; s++)
        {
            if (perShard[s].Count > 0) return true;
        }
        return false;
    }

    /// <summary>
    /// Builds the deterministic per-shard snapshot-leaf grain key. For a
    /// frozen-baseline coordinate (non-empty
    /// <see cref="LatticeSnapshotCoordinate.SnapshotBaselineToken"/>) this is
    /// <c>{treeId}/{shardIndex}/{token:N}</c> - identical to the key the
    /// shard-root capture seeds (issue #916), so the cursor reaches the very
    /// activation that already holds the in-memory baseline. For a legacy
    /// from-zero replay coordinate (empty token) it falls back to the
    /// coordinate-hash key <c>{treeId}/{shardIndex}/{coordHash}</c>.
    /// </summary>
    private string BuildSnapshotLeafKey(LatticeSnapshotCoordinate coord, int shardIndex)
    {
        var treeId = SnapshotLeafTreeId(coord);
        if (coord.SnapshotBaselineToken != Guid.Empty)
            return SnapshotLeafGrain.BuildBaselineKey(treeId, shardIndex, coord.SnapshotBaselineToken);

        var hash = ComputeCoordinateHash(coord);
        return $"{treeId}/{shardIndex}/{hash}";
    }

    /// <summary>
    /// Resolves the tree id the per-shard snapshot leaves and their durable
    /// baseline rows are keyed by. This must be the physical tree id the
    /// capture/seed path (the physical shard roots) used, so the cursor reaches
    /// the very activation that holds the in-memory seed rather than a fresh
    /// activation that would fail the from-storage reload
    /// (<see cref="LatticeSnapshotExpiredException"/>) after a shadow-cutover
    /// restore aliased the logical tree to a fresh physical tree (issue #1386).
    /// Falls back to the persisted logical tree id for legacy coordinates that
    /// carry no pinned physical id, which is correct on any non-restored tree
    /// where the physical id equals the logical id.
    /// </summary>
    private string SnapshotLeafTreeId(LatticeSnapshotCoordinate coord) =>
        coord.PhysicalTreeId ?? state.State.TreeId;

    /// <summary>
    /// FNV-1a 64-bit hash over the snapshot coordinate fields in a
    /// stable order so two callers that build the same coordinate
    /// activate the same snapshot leaf.
    /// </summary>
    private static string ComputeCoordinateHash(LatticeSnapshotCoordinate coord)
    {
        unchecked
        {
            ulong hash = 14695981039346656037UL;
            void MixUInt64(ulong v)
            {
                for (int i = 0; i < 8; i++)
                {
                    hash ^= v & 0xFFUL;
                    hash *= 1099511628211UL;
                    v >>= 8;
                }
            }

            MixUInt64((ulong)coord.TreeMapVersion);
            MixUInt64((ulong)coord.RegistrySnapshotHlc.WallClockTicks);
            MixUInt64((ulong)coord.RegistrySnapshotHlc.Counter);

            // Mix the per-open baseline token so two cursors that capture
            // structurally-identical coordinates (same map version, same heads,
            // same registry HLC) still activate distinct snapshot leaves bound
            // to their own frozen-baseline rows. The token is a fresh Guid per
            // open, so it also guarantees per-open snapshot-leaf-key uniqueness.
            Span<byte> tokenBytes = stackalloc byte[16];
            coord.SnapshotBaselineToken.TryWriteBytes(tokenBytes);
            MixUInt64(BinaryPrimitives.ReadUInt64LittleEndian(tokenBytes));
            MixUInt64(BinaryPrimitives.ReadUInt64LittleEndian(tokenBytes[8..]));

            var shards = new int[coord.PerShardWalOffsets.Count];
            var idx = 0;
            foreach (var key in coord.PerShardWalOffsets.Keys)
            {
                shards[idx++] = key;
            }
            Array.Sort(shards);
            for (var s = 0; s < shards.Length; s++)
            {
                MixUInt64((ulong)shards[s]);
                MixUInt64((ulong)coord.PerShardWalOffsets[shards[s]]);
            }

            return hash.ToString("x16");
        }
    }
}

