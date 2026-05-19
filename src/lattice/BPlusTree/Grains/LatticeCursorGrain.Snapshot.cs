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
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId));
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
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId));
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
    /// Snapshot-aware keys page. Fans out across per-shard snapshot
    /// leaves indicated by the persisted coordinate, k-way merges
    /// their sorted slices, and persists the new cursor position.
    /// </summary>
    private async Task<LatticeCursorKeysPage> NextSnapshotKeysAsync(int pageSize)
    {
        var coord = state.State.SnapshotCoordinate!.Value;
        var (effStart, effEnd) = ComputeEffectiveRange();
        var reverse = state.State.Spec.Reverse;

        var perShardLists = await FetchPerShardKeysAsync(coord, effStart, effEnd, pageSize);
        var collected = new List<string>(pageSize);
        MergeSortedKeyLists(perShardLists, reverse, pageSize, collected);

        var hasMore = collected.Count >= pageSize && AnyShardHasRemaining(perShardLists, pageSize);
        var prevLastYieldedKey = state.State.LastYieldedKey;
        var prevPhase = state.State.Phase;
        if (collected.Count > 0)
        {
            state.State.LastYieldedKey = collected[^1];
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.LastYieldedKey = prevLastYieldedKey;
            state.State.Phase = prevPhase;
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

        var perShardLists = await FetchPerShardEntriesAsync(coord, effStart, effEnd, pageSize);
        var collected = new List<KeyValuePair<string, byte[]>>(pageSize);
        MergeSortedEntryLists(perShardLists, reverse, pageSize, collected);

        var hasMore = collected.Count >= pageSize && AnyShardEntryHasRemaining(perShardLists, pageSize);
        var prevLastYieldedKey = state.State.LastYieldedKey;
        var prevPhase = state.State.Phase;
        if (collected.Count > 0)
        {
            state.State.LastYieldedKey = collected[^1].Key;
        }
        if (!hasMore)
        {
            state.State.Phase = LatticeCursorPhase.Exhausted;
        }
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.LastYieldedKey = prevLastYieldedKey;
            state.State.Phase = prevPhase;
            throw;
        }
        await TryReportSnapshotPinAsync();
        await SlideTtlAsync();

        return new LatticeCursorEntriesPage { Entries = collected, HasMore = hasMore };
    }

    /// <summary>
    /// Fans out a per-shard keys fetch across every shard the snapshot
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
        int pageSize)
    {
        var shards = coord.PerShardWalOffsets;
        var tasks = new Task<List<string>>[shards.Count];
        var index = 0;
        foreach (var (shardIdx, capturedOffset) in shards)
        {
            tasks[index++] = FetchSnapshotShardKeysAsync(shardIdx, capturedOffset, coord, effStart, effEnd, pageSize);
        }

        return await Task.WhenAll(tasks);
    }

    private async Task<List<string>> FetchSnapshotShardKeysAsync(
        int shardIndex,
        long capturedOffset,
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize)
    {
        var leaf = grainFactory.GetGrain<ISnapshotLeafGrain>(BuildSnapshotLeafKey(coord, shardIndex));
        await leaf.OpenAsync(state.State.TreeId, shardIndex, capturedOffset, default);
        return await leaf.GetKeysAsync(effStart, effEnd, limit: pageSize);
    }

    private async Task<List<KeyValuePair<string, byte[]>>> FetchSnapshotShardEntriesAsync(
        int shardIndex,
        long capturedOffset,
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize)
    {
        var leaf = grainFactory.GetGrain<ISnapshotLeafGrain>(BuildSnapshotLeafKey(coord, shardIndex));
        await leaf.OpenAsync(state.State.TreeId, shardIndex, capturedOffset, default);
        return await leaf.GetEntriesAsync(effStart, effEnd, limit: pageSize);
    }

    /// <summary>
    /// Entries-shape companion of <see cref="FetchPerShardKeysAsync"/>.
    /// </summary>
    private async Task<List<KeyValuePair<string, byte[]>>[]> FetchPerShardEntriesAsync(
        LatticeSnapshotCoordinate coord,
        string? effStart,
        string? effEnd,
        int pageSize)
    {
        var shards = coord.PerShardWalOffsets;
        var tasks = new Task<List<KeyValuePair<string, byte[]>>>[shards.Count];
        var index = 0;
        foreach (var (shardIdx, capturedOffset) in shards)
        {
            tasks[index++] = FetchSnapshotShardEntriesAsync(shardIdx, capturedOffset, coord, effStart, effEnd, pageSize);
        }

        return await Task.WhenAll(tasks);
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
            output.Add(pickKey!);
            cursors[pickShard] += reverse ? -1 : 1;
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
            output.Add(perShard[pickShard][cursors[pickShard]]);
            cursors[pickShard] += reverse ? -1 : 1;
        }
    }

    private static bool AnyShardHasRemaining(List<string>[] perShard, int pageSize)
    {
        for (var s = 0; s < perShard.Length; s++)
        {
            if (perShard[s].Count >= pageSize) return true;
        }
        return false;
    }

    private static bool AnyShardEntryHasRemaining(List<KeyValuePair<string, byte[]>>[] perShard, int pageSize)
    {
        for (var s = 0; s < perShard.Length; s++)
        {
            if (perShard[s].Count >= pageSize) return true;
        }
        return false;
    }

    /// <summary>
    /// Builds the deterministic per-shard snapshot-leaf grain key:
    /// <c>{treeId}/{shardIndex}/{coordHash}</c>.
    /// </summary>
    private string BuildSnapshotLeafKey(LatticeSnapshotCoordinate coord, int shardIndex)
    {
        var hash = ComputeCoordinateHash(coord);
        return $"{state.State.TreeId}/{shardIndex}/{hash}";
    }

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

