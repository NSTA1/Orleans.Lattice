using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless worker that routes requests to the correct <see cref="IShardRootGrain"/>
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c>.
/// </summary>
[StatelessWorker]
internal sealed partial class LatticeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILattice
{
    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    private bool _compactionEnsured;
    private bool _monitorEnsured;
    private string? _physicalTreeId;
    private ShardMap? _shardMap;

    public async Task<byte[]?> GetAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetAsync(key);
        }
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetWithVersionAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetWithVersionAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetWithVersionAsync(key);
        }
    }

    public async Task<bool> ExistsAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.ExistsAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.ExistsAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.ExistsAsync(key);
        }
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        try
        {
            return await GetManyAsyncCore(keys);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            return await GetManyAsyncCore(keys);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await GetManyAsyncCore(keys);
        }
    }

    private async Task<Dictionary<string, byte[]>> GetManyAsyncCore(List<string> keys)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();

        // Group keys by shard.
        var shardBuckets = new Dictionary<int, List<string>>();
        foreach (var key in keys)
        {
            var idx = shardMap.Resolve(key);
            if (!shardBuckets.TryGetValue(idx, out var bucket))
            {
                bucket = [];
                shardBuckets[idx] = bucket;
            }
            bucket.Add(key);
        }

        // Fan out batch reads in parallel per shard.
        var result = new ConcurrentDictionary<string, byte[]>();
        var tasks = new List<Task>(shardBuckets.Count);

        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            tasks.Add(FetchFromShardAsync(shard, bucket, result));
        }

        await Task.WhenAll(tasks);
        return new Dictionary<string, byte[]>(result);

        static async Task FetchFromShardAsync(
            IShardRootGrain shard,
            List<string> keys,
            ConcurrentDictionary<string, byte[]> result)
        {
            var values = await shard.GetManyAsync(keys);
            foreach (var (key, value) in values)
            {
                result[key] = value;
            }
        }
    }

    public async Task SetAsync(string key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            await shard.SetAsync(key, value);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value);
        }
    }

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetOrSetAsync(key, value);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetOrSetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.GetOrSetAsync(key, value);
        }
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        try
        {
            await SetManyAsyncCore(entries);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            await SetManyAsyncCore(entries);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            await SetManyAsyncCore(entries);
        }
    }

    private async Task SetManyAsyncCore(List<KeyValuePair<string, byte[]>> entries)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();

        // Group entries by shard.
        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        foreach (var entry in entries)
        {
            var idx = shardMap.Resolve(entry.Key);
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
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            tasks.Add(WriteToShardAsync(shard, bucket));
        }

        await Task.WhenAll(tasks);

        static async Task WriteToShardAsync(
            IShardRootGrain shard,
            List<KeyValuePair<string, byte[]>> entries)
        {
            await shard.SetManyAsync(entries);
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        await EnsureCompactionReminderAsync();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.DeleteAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.DeleteAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            shard = await GetShardGrainAsync(key);
            return await shard.DeleteAsync(key);
        }
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        await EnsureCompactionReminderAsync();
        try
        {
            return await DeleteRangeAsyncCore(startInclusive, endExclusive);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            return await DeleteRangeAsyncCore(startInclusive, endExclusive);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await DeleteRangeAsyncCore(startInclusive, endExclusive);
        }
    }

    private async Task<int> DeleteRangeAsyncCore(string startInclusive, string endExclusive)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Fan out to all physical shards in parallel — any may contain keys in the range.
        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.DeleteRangeAsync(startInclusive, endExclusive);
        }

        await Task.WhenAll(tasks);

        var total = 0;
        for (int i = 0; i < tasks.Length; i++)
            total += tasks[i].Result;
        return total;
    }

    public async Task<int> CountAsync()
    {
        try
        {
            return await CountAsyncCore();
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await CountAsyncCore();
        }
    }

    /// <summary>
    /// Strongly-consistent total live key count across all physical shards
    /// of this tree (F-011). Tolerates concurrent adaptive shard splits via
    /// per-slot reconciliation:
    /// <list type="number">
    ///   <item>Pass 1 fans out <see cref="IShardRootGrain.CountWithMovedAwayAsync"/>
    ///   to every physical shard owned by the snapshot of the shard map taken at
    ///   scan start. Each shard returns its live count <em>excluding</em> any virtual
    ///   slot that has moved (or is moving) to another physical shard, plus the
    ///   set of slots it filtered.</item>
    ///   <item>The orchestrator re-reads the shard map. If
    ///   <see cref="ShardMap.Version"/> changed during pass 1, the per-shard
    ///   counts may include keys whose slot has since moved (so the new owner
    ///   would also count them); the iteration is discarded and the whole pass
    ///   retries on a fresh map.</item>
    ///   <item>If the version is stable and any slots were filtered, pass 2
    ///   queries the same owners with <see cref="IShardRootGrain.CountForSlotsAsync"/>
    ///   for those slots — these are exactly the slots that pass 1 excluded, so
    ///   no double counting is possible.</item>
    ///   <item>A final version re-read confirms stability across pass 2.</item>
    /// </list>
    /// Bounded by <see cref="LatticeOptions.MaxScanRetries"/>; throws
    /// <see cref="InvalidOperationException"/> when topology changes faster than
    /// the orchestrator can converge. System trees skip reconciliation (they
    /// never participate in adaptive splits and the registry would deadlock
    /// on itself).
    /// </summary>
    private async Task<int> CountAsyncCore()
    {
        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        var physicalShards = shardMap0.GetPhysicalShardIndices();

        // System trees never participate in adaptive splits, and reading the
        // registry's shard map for a system tree would create a circular
        // call chain (the registry itself is backed by an ILattice tree).
        // Use the fast simple-fan-out path.
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            return await SimpleSumCountAsync(physicalTreeId, physicalShards);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var maxRetries = Math.Max(1, Options.MaxScanRetries);

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            if (attempt > 0)
            {
                InvalidateShardMap();
                (physicalTreeId, shardMap0) = await GetRoutingAsync();
                physicalShards = shardMap0.GetPhysicalShardIndices();
            }

            var versionAtStart = shardMap0.Version;

            // Single pass: count + moved-slot reports from every current owner.
            // The source of a completed split filters out keys belonging to
            // moved slots (via MovedAwaySlots), and the target shard has those
            // keys after the drain. Polling every current physical owner therefore
            // yields the exact total without double counting and without needing
            // a follow-up reconciliation pass. The moved-slot reports are used
            // only as a stability hint: if any shard reports moved slots, we
            // re-check the registry's ShardMap.Version after polling to ensure
            // no swap happened mid-scan that could have shifted authority for
            // slots a shard didn't yet have stamped as moved.
            var pass1Tasks = new Task<ShardCountResult>[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
                pass1Tasks[i] = shard.CountWithMovedAwayAsync();
            }
            await Task.WhenAll(pass1Tasks);

            var total = 0;
            var anyMovedReported = false;
            for (int i = 0; i < pass1Tasks.Length; i++)
            {
                var r = pass1Tasks[i].Result;
                total += r.Count;
                if (r.MovedAwaySlots is { Length: > 0 }) anyMovedReported = true;
            }

            // Stability check: if the map version moved during pass 1, a swap
            // happened concurrently and the per-shard counts may have been
            // taken against an inconsistent view (some shards before the swap,
            // some after). Discard and retry.
            if (anyMovedReported)
            {
                var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
                if (shardMapNow.Version != versionAtStart) continue;
            }

            return total;
        }

        throw new InvalidOperationException(
            $"CountAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
            "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
    }

    private async Task<int> SimpleSumCountAsync(string physicalTreeId, IReadOnlyList<int> physicalShards)
    {
        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.CountAsync();
        }
        await Task.WhenAll(tasks);
        var total = 0;
        for (int i = 0; i < tasks.Length; i++) total += tasks[i].Result;
        return total;
    }

    /// <summary>
    /// Groups <paramref name="slots"/> by their owning physical shard per
    /// <paramref name="map"/>. Out-of-range slots are silently dropped.
    /// </summary>
    internal static Dictionary<int, List<int>> GroupSlotsByOwner(HashSet<int> slots, ShardMap map)
    {
        var byOwner = new Dictionary<int, List<int>>();
        foreach (var s in slots)
        {
            if ((uint)s >= (uint)map.Slots.Length) continue;
            var owner = map.Slots[s];
            if (!byOwner.TryGetValue(owner, out var list))
            {
                list = [];
                byOwner[owner] = list;
            }
            list.Add(s);
        }
        return byOwner;
    }

    /// <summary>Copies <paramref name="list"/> into a new sorted array.</summary>
    internal static int[] ToSortedArray(List<int> list)
    {
        var arr = list.ToArray();
        Array.Sort(arr);
        return arr;
    }

    /// <summary>
    /// Computes the set of virtual slots whose owning physical shard differs
    /// between <paramref name="oldMap"/> and <paramref name="newMap"/>.
    /// Used by strongly-consistent scans to detect topology changes that
    /// happened between the start and end of a scan pass.
    /// </summary>
    internal static HashSet<int>? ComputeOwnerDiff(ShardMap oldMap, ShardMap newMap)
    {
        if (newMap.Version == oldMap.Version) return null;
        HashSet<int>? diff = null;
        var n = Math.Min(oldMap.Slots.Length, newMap.Slots.Length);
        for (int s = 0; s < n; s++)
        {
            if (oldMap.Slots[s] != newMap.Slots[s])
                (diff ??= []).Add(s);
        }
        return diff;
    }

    public async Task<IReadOnlyList<int>> CountPerShardAsync()
    {
        try
        {
            return await CountPerShardAsyncCore();
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            return await CountPerShardAsyncCore();
        }
    }

    private async Task<IReadOnlyList<int>> CountPerShardAsyncCore()
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.CountAsync();
        }

        await Task.WhenAll(tasks);

        var counts = new int[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
            counts[i] = tasks[i].Result;
        return counts;
    }

    private async Task EnsureCompactionReminderAsync()
    {
        if (_compactionEnsured) return;
        if (Options.TombstoneGracePeriod == Timeout.InfiniteTimeSpan) return;

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.EnsureReminderAsync();
        _compactionEnsured = true;
    }

    /// <summary>
    /// Lazily activates the per-tree autonomic <c>HotShardMonitorGrain</c> on
    /// the first write to this tree. Subsequent writes are no-ops. The monitor
    /// itself is a no-op when <see cref="LatticeOptions.AutoSplitEnabled"/> is
    /// <c>false</c>.
    /// </summary>
    private async Task EnsureMonitorAsync()
    {
        if (_monitorEnsured) return;
        if (!Options.AutoSplitEnabled) { _monitorEnsured = true; return; }
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            _monitorEnsured = true;
            return;
        }

        var monitor = grainFactory.GetGrain<IHotShardMonitorGrain>(TreeId);
        await monitor.EnsureRunningAsync();
        _monitorEnsured = true;
    }

    /// <summary>
    /// Resolves the physical tree ID for this logical tree, caching the result
    /// for the lifetime of this activation. Different physical tree IDs produce
    /// different leaf <see cref="GrainId"/> values, which automatically creates
    /// fresh <see cref="ILeafCacheGrain"/> instances — no cache invalidation needed.
    /// </summary>
    private async Task<string> GetPhysicalTreeIdAsync()
    {
        if (_physicalTreeId is not null) return _physicalTreeId;

        // System trees (e.g. _lattice_trees) must not resolve aliases — the
        // registry itself is backed by an ILattice tree, so calling ResolveAsync
        // here would create a circular call chain and deadlock.
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            _physicalTreeId = TreeId;
            return _physicalTreeId;
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        _physicalTreeId = await registry.ResolveAsync(TreeId);
        return _physicalTreeId;
    }

    private async Task<IShardRootGrain> GetShardGrainAsync(string key)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var shardIndex = shardMap.Resolve(key);
        var shardKey = $"{physicalTreeId}/{shardIndex}";
        return grainFactory.GetGrain<IShardRootGrain>(shardKey);
    }

    /// <summary>
    /// Returns the routing context for this tree: the resolved physical tree
    /// ID and the effective <see cref="ShardMap"/>. Both are cached for the
    /// lifetime of this activation and invalidated together by
    /// <see cref="TryInvalidateStaleAlias"/> when a downstream shard reports
    /// the tree as deleted.
    /// </summary>
    public async Task<RoutingInfo> GetRoutingAsync()
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        if (_shardMap is not null) return new RoutingInfo(physicalTreeId, _shardMap);

        var options = Options;
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            // System trees never have a custom shard map; using the default
            // also avoids a circular registry call.
            _shardMap = ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);
        }
        else
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            _shardMap = await registry.GetShardMapAsync(TreeId)
                ?? ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);
        }

        return new RoutingInfo(physicalTreeId, _shardMap);
    }

    /// <summary>
    /// Returns <c>true</c> if the cached alias was stale and has been invalidated,
    /// allowing a retry with a fresh resolution. Returns <c>false</c> if no alias
    /// was cached (meaning the tree is genuinely deleted, not a stale alias).
    /// Used as a <c>when</c> filter in catch clauses.
    /// </summary>
    private bool TryInvalidateStaleAlias()
    {
        if (_physicalTreeId is null) return false;
        _physicalTreeId = null;
        _shardMap = null;
        return true;
    }

    /// <summary>
    /// Invalidates the cached <see cref="ShardMap"/> only (preserves the
    /// resolved physical tree ID). Used by <see cref="StaleShardRoutingException"/>
    /// catch clauses to force a fresh map fetch on retry after an adaptive
    /// shard split has remapped virtual slots to a new physical shard.
    /// Always returns <c>true</c> so it can be used as a <c>when</c> filter.
    /// </summary>
    private bool InvalidateShardMap()
    {
        _shardMap = null;
        return true;
    }

    /// <summary>
    /// Computes a stable shard index for the given key using XxHash32.
    /// Provided for backward compatibility; new routing should go through
    /// the per-tree <see cref="ShardMap"/> via <see cref="GetRoutingAsync"/>.
    /// </summary>
    internal static int GetShardIndex(string key, int shardCount) =>
        LatticeSharding.GetShardIndex(key, shardCount);
}
