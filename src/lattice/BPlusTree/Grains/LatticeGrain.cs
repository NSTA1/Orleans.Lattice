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

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetAsync(key);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetAsync(key);
        }
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetWithVersionAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetWithVersionAsync(key);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetWithVersionAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetWithVersionAsync(key);
        }
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.ExistsAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.ExistsAsync(key);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.ExistsAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.ExistsAsync(key);
        }
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await GetManyAsyncCore(keys);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await GetManyAsyncCore(keys);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await GetManyAsyncCore(keys);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
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

    public async Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            await shard.SetAsync(key, value);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value);
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be positive.");
        var nowUtc = DateTimeOffset.UtcNow;
        if (ttl > DateTimeOffset.MaxValue - nowUtc)
            throw new ArgumentOutOfRangeException(nameof(ttl),
                "TTL is too large — absolute expiry would exceed DateTimeOffset.MaxValue.");
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the absolute expiry on the silo handling this call so
        // per-entry lifetimes are not shifted by client-clock skew.
        var expiresAtTicks = nowUtc.Add(ttl).UtcTicks;

        var shard = await GetShardGrainAsync(key);
        try
        {
            await shard.SetAsync(key, value, expiresAtTicks);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value, expiresAtTicks);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value, expiresAtTicks);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            await shard.SetAsync(key, value, expiresAtTicks);
        }
    }

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.GetOrSetAsync(key, value);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetOrSetAsync(key, value);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetOrSetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.GetOrSetAsync(key, value);
        }
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await SetManyAsyncCore(entries);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            await SetManyAsyncCore(entries);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            await SetManyAsyncCore(entries);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
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

    /// <summary>
    /// Atomic multi-key write. Activates a dedicated
    /// <see cref="IAtomicWriteGrain"/> keyed by <c>{treeId}/{operationId}</c>
    /// and awaits saga completion. Duplicate-key and null-value validation is
    /// done inside the saga grain; no routing is needed here because the saga
    /// itself calls back through <see cref="ILattice"/> for each write.
    /// </summary>
    public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return Task.CompletedTask;

        var operationId = Guid.NewGuid().ToString("N");
        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
        return saga.ExecuteAsync(TreeId, entries);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        try
        {
            return await shard.DeleteAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.DeleteAsync(key);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.DeleteAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            return await shard.DeleteAsync(key);
        }
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
    }

    private async Task<int> DeleteRangeAsyncCore(string startInclusive, string endExclusive, CancellationToken cancellationToken)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Fan out to all physical shards in parallel — any may contain keys in the range.
        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.DeleteRangeAsync(startInclusive, endExclusive);
        }

        await Task.WhenAll(tasks);
        cancellationToken.ThrowIfCancellationRequested();

        var total = 0;
        for (int i = 0; i < tasks.Length; i++)
            total += tasks[i].Result;
        return total;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await CountAsyncCore(cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountAsyncCore(cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountAsyncCore(cancellationToken);
        }
    }

    /// <summary>
    /// Strongly-consistent total live key count across all physical shards
    /// of this tree. Tolerates concurrent adaptive shard splits by
    /// asking every physical shard to count only the virtual slots it
    /// currently owns per the authoritative <see cref="ShardMap"/>, then
    /// re-reading the map after the fan-out and retrying on any version
    /// change (FX-012 v2).
    /// <para>
    /// This supersedes the earlier (FX-012 v1) design that fanned out via
    /// <see cref="IShardRootGrain.CountWithMovedAwayAsync"/> and relied on
    /// each source shard filtering its moved-slot keys via
    /// <c>SplitInProgress.Phase</c>. That protocol had a gap: the split
    /// coordinator publishes the new <see cref="ShardMap"/> (including the
    /// target shard) before advancing the source shard's persisted phase to
    /// <c>Reject</c>. During that window the source did not filter
    /// moved-slot keys while the target already held them, and a count
    /// arriving in the window double-counted each migrating-slot key.
    /// Routing per-slot through the current map closes that window by
    /// construction: each virtual slot is counted exactly once, against the
    /// shard the map identifies as its current owner.
    /// </para>
    /// <para>
    /// Bounded by <see cref="LatticeOptions.MaxScanRetries"/>; throws
    /// <see cref="InvalidOperationException"/> when topology changes faster
    /// than the orchestrator can converge. System trees skip reconciliation
    /// (they never participate in adaptive splits and the registry would
    /// deadlock on itself).
    /// </para>
    /// </summary>
    private async Task<int> CountAsyncCore(CancellationToken cancellationToken)
    {
        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
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
            cancellationToken.ThrowIfCancellationRequested();
            if (attempt > 0)
            {
                InvalidateShardMap();
                (physicalTreeId, shardMap0) = await GetRoutingAsync();
                physicalShards = shardMap0.GetPhysicalShardIndices();
            }

            var versionAtStart = shardMap0.Version;
            var virtualShardCount = shardMap0.Slots.Length;

            // Fast path: Version == 0 means the default identity map is in
            // effect — no split has ever been persisted for this tree.
            // ShardMap.Version is monotonically incremented on every persist,
            // so if it is still 0 at the end of the call, no split can have
            // started during our fan-out. Use the cheap leaf.CountAsync()
            // path (O(1) per leaf) and avoid BuildOwnedSlotMap / per-key
            // slot hashing / binary-search entirely.
            if (versionAtStart == 0L)
            {
                var simple = await SimpleSumCountAsync(physicalTreeId, physicalShards);
                var mapAfter = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
                if (mapAfter.Version == 0L) return simple;
                shardMap0 = mapAfter;
                continue;
            }

            // FX-012 v2: partition virtual slots by current owner per the
            // authoritative map and ask each physical shard to count only
            // its owned slots. This makes the result topology-consistent
            // with the observed map snapshot regardless of where each
            // shard is in its per-split phase machine.
            var ownedByShard = BuildOwnedSlotMap(shardMap0);
            var pass1Tasks = new Task<int>[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
            {
                var physicalIdx = physicalShards[i];
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalIdx}");
                if (!ownedByShard.TryGetValue(physicalIdx, out var owned) || owned.Length == 0)
                {
                    // Shard referenced by the map but owning no slots (pathological).
                    pass1Tasks[i] = Task.FromResult(0);
                    continue;
                }
                pass1Tasks[i] = shard.CountForSlotsAsync(owned, virtualShardCount);
            }
            await Task.WhenAll(pass1Tasks);

            var total = 0;
            for (int i = 0; i < pass1Tasks.Length; i++)
                total += pass1Tasks[i].Result;

            // Unconditional stability check: if the shard-map version moved
            // while pass1 was in flight, the per-shard counts may have
            // spanned an inconsistent snapshot. Discard and retry against
            // the fresh map.
            var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
            if (shardMapNow.Version != versionAtStart) continue;

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
    /// Partitions the virtual slots of <paramref name="map"/> by their
    /// currently-owning physical shard, returning a dictionary mapping
    /// physical shard index to its sorted owned-slot array. Used by
    /// <see cref="CountAsyncCore"/> and <see cref="CountPerShardAsyncCore"/>
    /// to route per-slot count requests through the authoritative map.
    /// </summary>
    internal static Dictionary<int, int[]> BuildOwnedSlotMap(ShardMap map)
    {
        // Single-pass build. Slots are iterated in ascending order, so each
        // per-owner array is naturally sorted without a secondary Array.Sort.
        // First pass: count slots per owner so we can allocate each int[]
        // at its final size (no List<int> growth cycle, no copy). Second
        // pass: fill via per-owner write cursors.
        var slots = map.Slots;
        var counts = new Dictionary<int, int>();
        for (int s = 0; s < slots.Length; s++)
        {
            var owner = slots[s];
            counts.TryGetValue(owner, out var c);
            counts[owner] = c + 1;
        }

        var result = new Dictionary<int, int[]>(counts.Count);
        var cursors = new Dictionary<int, int>(counts.Count);
        foreach (var kv in counts)
        {
            result[kv.Key] = new int[kv.Value];
            cursors[kv.Key] = 0;
        }

        for (int s = 0; s < slots.Length; s++)
        {
            var owner = slots[s];
            var pos = cursors[owner];
            result[owner][pos] = s;
            cursors[owner] = pos + 1;
        }
        return result;
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

    public async Task<IReadOnlyList<int>> CountPerShardAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await CountPerShardAsyncCore(cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountPerShardAsyncCore(cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountPerShardAsyncCore(cancellationToken);
        }
    }

    private async Task<IReadOnlyList<int>> CountPerShardAsyncCore(CancellationToken cancellationToken)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // System trees never participate in adaptive splits — fast path.
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var simpleTasks = new Task<int>[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
            {
                var sh = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
                simpleTasks[i] = sh.CountAsync();
            }
            await Task.WhenAll(simpleTasks);
            var simple = new int[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++) simple[i] = simpleTasks[i].Result;
            return simple;
        }

        // FX-012 v2: mirror CountAsyncCore's per-slot routing so per-shard
        // counts are also topology-consistent with the observed map. Without
        // this, a split mid-call would surface as a target-shard count
        // inflated by double-counted migrating slots.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var maxRetries = Math.Max(1, Options.MaxScanRetries);

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (attempt > 0)
            {
                InvalidateShardMap();
                (physicalTreeId, shardMap) = await GetRoutingAsync();
                physicalShards = shardMap.GetPhysicalShardIndices();
            }

            var versionAtStart = shardMap.Version;
            var virtualShardCount = shardMap.Slots.Length;

            // Fast path: Version == 0 means no split has ever been persisted
            // for this tree. Use the cheap per-shard CountAsync() path and
            // confirm the map is still at Version 0 after fan-out.
            if (versionAtStart == 0L)
            {
                var fastTasks = new Task<int>[physicalShards.Count];
                for (int i = 0; i < physicalShards.Count; i++)
                {
                    var sh = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
                    fastTasks[i] = sh.CountAsync();
                }
                await Task.WhenAll(fastTasks);
                var mapAfter = await registry.GetShardMapAsync(TreeId) ?? shardMap;
                if (mapAfter.Version == 0L)
                {
                    var fastCounts = new int[physicalShards.Count];
                    for (int i = 0; i < physicalShards.Count; i++) fastCounts[i] = fastTasks[i].Result;
                    return fastCounts;
                }
                shardMap = mapAfter;
                continue;
            }

            var ownedByShard = BuildOwnedSlotMap(shardMap);
            var tasks = new Task<int>[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
            {
                var physicalIdx = physicalShards[i];
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalIdx}");
                if (!ownedByShard.TryGetValue(physicalIdx, out var owned) || owned.Length == 0)
                {
                    tasks[i] = Task.FromResult(0);
                    continue;
                }
                tasks[i] = shard.CountForSlotsAsync(owned, virtualShardCount);
            }
            await Task.WhenAll(tasks);

            var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap;
            if (shardMapNow.Version != versionAtStart) continue;

            var counts = new int[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
                counts[i] = tasks[i].Result;
            return counts;
        }

        throw new InvalidOperationException(
            $"CountPerShardAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
            "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
    }

    /// <summary>
    /// Lazily ensures the tree's <c>TombstoneCompactionGrain</c> has a
    /// registered reminder, on the first write to this tree. Subsequent
    /// writes are no-ops.
    /// </summary>
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
    public async Task<RoutingInfo> GetRoutingAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
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
