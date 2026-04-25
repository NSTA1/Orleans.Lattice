using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
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
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    IServiceProvider services,
    ILogger<LatticeGrain> logger) : ILattice, ISystemLattice
{
    private string? _treeIdCache;
    private string TreeId => _treeIdCache ??= context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    private bool _compactionEnsured;
    private bool _monitorEnsured;
    private string? _physicalTreeId;
    private ShardMap? _shardMap;
    private readonly PublishEventsGate _eventsGate = new();

    /// <summary>
    /// Rejects any public <see cref="ILattice"/> call targeting a reserved
    /// system-tree name (any id starting with
    /// <see cref="LatticeConstants.SystemTreePrefix"/>, which includes the
    /// registry tree and the replog prefix). Internal library code that
    /// legitimately addresses system trees resolves
    /// <see cref="ISystemLattice"/> instead and bypasses this guard via
    /// explicit interface implementation.
    /// </summary>
    private void ThrowIfSystemTree()
    {
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Tree ID '{TreeId}' is reserved for internal Lattice system trees and cannot be addressed via the public ILattice surface. Choose a tree name that does not start with '{LatticeConstants.SystemTreePrefix}'.");
    }

    private async Task PublishEventAsync(LatticeTreeEventKind kind, string? key = null, int? shardIndex = null)
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(kind, TreeId, key, shardIndex);
        await LatticeEventPublisher.PublishAsync(services, opts, evt, logger);
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        return GetAsyncCore(key, cancellationToken);
    }

    async Task<byte[]?> ISystemLattice.GetAsync(string key, CancellationToken cancellationToken)
        => await GetAsyncCore(key, cancellationToken);

    private async Task<byte[]?> GetAsyncCore(string key, CancellationToken cancellationToken)
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
        ThrowIfSystemTree();
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

    public Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        return ExistsAsyncCore(key, cancellationToken);
    }

    async Task<bool> ISystemLattice.ExistsAsync(string key, CancellationToken cancellationToken)
        => await ExistsAsyncCore(key, cancellationToken);

    private async Task<bool> ExistsAsyncCore(string key, CancellationToken cancellationToken)
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
        ThrowIfSystemTree();
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

    public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        return SetAsyncCore(key, value, cancellationToken);
    }

    async Task ISystemLattice.SetAsync(string key, byte[] value, CancellationToken cancellationToken)
        => await SetAsyncCore(key, value, cancellationToken);

    private async Task SetAsyncCore(string key, byte[] value, CancellationToken cancellationToken)
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
        await PublishEventAsync(LatticeTreeEventKind.Set, key);
    }

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
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
        await PublishEventAsync(LatticeTreeEventKind.Set, key);
    }

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        bool applied;
        try
        {
            applied = await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            applied = await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            applied = await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            applied = await shard.SetIfVersionAsync(key, value, expectedVersion);
        }
        if (applied) await PublishEventAsync(LatticeTreeEventKind.Set, key);
        return applied;
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        byte[]? existing;
        try
        {
            existing = await shard.GetOrSetAsync(key, value);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            existing = await shard.GetOrSetAsync(key, value);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            existing = await shard.GetOrSetAsync(key, value);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            existing = await shard.GetOrSetAsync(key, value);
        }
        // Publish only when a new value was actually written (existing was null).
        if (existing is null) await PublishEventAsync(LatticeTreeEventKind.Set, key);
        return existing;
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
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

        // Publish one Set event per entry. Emitted only after all shard writes
        // have committed so subscribers never observe a Set for a key that
        // failed to persist (we'd have thrown above). Skipped entirely when
        // publishing is disabled to avoid walking the entry list.
        if (entries.Count > 0 && await _eventsGate.IsEnabledAsync(grainFactory, TreeId, Options))
        {
            foreach (var entry in entries)
            {
                await PublishEventAsync(LatticeTreeEventKind.Set, entry.Key);
            }
        }
    }

    private async Task SetManyAsyncCore(List<KeyValuePair<string, byte[]>> entries)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();

        // Group entries by shard.
        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>> ();
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
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return Task.CompletedTask;

        var operationId = Guid.NewGuid().ToString("N");
        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
        return saga.ExecuteAsync(TreeId, entries);
    }

    /// <summary>
    /// Caller-supplied idempotency-key overload. Validates the
    /// <paramref name="operationId"/> shape, then dispatches to the saga
    /// grain keyed by <c>{TreeId}/{operationId}</c>. Resubmissions with
    /// the same id re-attach to the original saga and inherit its
    /// completion outcome.
    /// </summary>
    public Task SetManyAtomicAsync(
        List<KeyValuePair<string, byte[]>> entries,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(entries);
        ValidateOperationId(operationId);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return Task.CompletedTask;

        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
        return saga.ExecuteAsync(TreeId, entries);
    }

    private static void ValidateOperationId(string operationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        if (operationId.Contains('/'))
            throw new ArgumentException(
                "operationId must not contain '/' (reserved as the grain-key separator).",
                nameof(operationId));
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        return DeleteAsyncCore(key, cancellationToken);
    }

    async Task<bool> ISystemLattice.DeleteAsync(string key, CancellationToken cancellationToken)
        => await DeleteAsyncCore(key, cancellationToken);

    private async Task<bool> DeleteAsyncCore(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var shard = await GetShardGrainAsync(key);
        bool existed;
        try
        {
            existed = await shard.DeleteAsync(key);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            existed = await shard.DeleteAsync(key);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            existed = await shard.DeleteAsync(key);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            shard = await GetShardGrainAsync(key);
            existed = await shard.DeleteAsync(key);
        }
        if (existed) await PublishEventAsync(LatticeTreeEventKind.Delete, key);
        return existed;
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        int deleted;
        try
        {
            deleted = await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            deleted = await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            deleted = await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            deleted = await DeleteRangeAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        if (deleted > 0)
            await PublishEventAsync(LatticeTreeEventKind.DeleteRange, $"{startInclusive}..{endExclusive}");
        return deleted;
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
        ThrowIfSystemTree();
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
    /// change.
    /// <para>
    /// This supersedes an earlier design that fanned out via
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

            // Partition virtual slots by current owner per the
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
        ThrowIfSystemTree();
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

        // Mirror CountAsyncCore's per-slot routing so per-shard
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
        // NOTE: intentionally NOT guarded — `GetRoutingAsync` is called by the
        // library's own internal coordinator grains (saga compensation, stats,
        // cursor) which sometimes resolve routing for their owning tree before
        // dispatching further internal calls. It does not read or mutate user
        // data; the shard grains enforce the real boundary on reads/writes.
        cancellationToken.ThrowIfCancellationRequested();
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        if (_shardMap is not null) return new RoutingInfo(physicalTreeId, _shardMap);

        var resolved = await optionsResolver.ResolveAsync(TreeId);
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            // System trees never have a custom shard map; using the default
            // also avoids a circular registry call.
            _shardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        }
        else
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            _shardMap = await registry.GetShardMapAsync(TreeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        }

        return new RoutingInfo(physicalTreeId, _shardMap);
    }

    /// <inheritdoc />
    public Task<TreeDiagnosticReport> DiagnoseAsync(bool deep = false, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var stats = grainFactory.GetGrain<ILatticeStats>(TreeId);
        return stats.GetReportAsync(deep, cancellationToken);
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
