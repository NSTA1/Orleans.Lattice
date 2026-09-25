namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Tree-wide operations over a sharded saga decision registry (issue #3501).
/// Each method addresses every registry grain key returned by
/// <see cref="TxRegistryRouting.EnumerateKeys"/> (every shard plus the legacy
/// bare-tree-id registry) and folds the per-key answers into the single-registry
/// shape the callers already consume. Per-txid operations are routed by
/// <see cref="TxRegistryRouting.GetRegistry"/> instead.
/// <para>
/// Aggregation rules:
/// </para>
/// <list type="bullet">
/// <item><description>Decisions are unioned. A txid lives on exactly one key, so the union never has to choose between two readings.</description></item>
/// <item><description>The decisions revision is the sum of the per-key revisions. Each per-key revision is non-decreasing, so the sum is equal across two reads only when every per-key revision is, which is exactly the equality the reader stability gate relies on.</description></item>
/// <item><description>The cross-tree in-flight count, unresolvable count and registration epoch are summed. The epoch sum is monotonic by the same argument as the revision.</description></item>
/// </list>
/// <para>
/// Every method throws when any key's call throws, so a caller never mistakes a
/// partial view for a whole one; the existing callers already treat a registry
/// failure as "no snapshot" and fall back to their per-leaf path.
/// </para>
/// </summary>
internal static class TxRegistryFanOut
{
    /// <summary>
    /// Returns a self-consistent decisions snapshot and revision across every
    /// registry key of <paramref name="treeId"/>. Each key's pair is captured
    /// atomically in one registry turn.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <returns>The unioned decisions and summed revision.</returns>
    public static async Task<TxRegistrySnapshot> SnapshotWithRevisionAsync(
        IGrainFactory grainFactory, string treeId, int shardCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var keys = TxRegistryRouting.EnumerateKeys(treeId, shardCount);
        if (keys.Length == 1)
        {
            return await grainFactory.GetGrain<ITxRegistryGrain>(keys[0]).SnapshotWithRevisionAsync();
        }

        var tasks = new Task<TxRegistrySnapshot>[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            tasks[i] = grainFactory.GetGrain<ITxRegistryGrain>(keys[i]).SnapshotWithRevisionAsync();
        }

        var parts = await Task.WhenAll(tasks);
        var total = 0;
        long revision = 0;
        foreach (var part in parts)
        {
            total += part.Decisions?.Count ?? 0;
            revision += part.Revision;
        }

        var decisions = new Dictionary<Guid, TxStatus>(total);
        foreach (var part in parts)
        {
            if (part.Decisions is null) continue;
            foreach (var (txid, status) in part.Decisions)
            {
                decisions[txid] = status;
            }
        }

        return new TxRegistrySnapshot { Decisions = decisions, Revision = revision };
    }

    /// <summary>
    /// Returns the summed decisions revision across every registry key of
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <returns>The summed revision.</returns>
    public static async Task<long> GetDecisionsRevisionAsync(
        IGrainFactory grainFactory, string treeId, int shardCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var keys = TxRegistryRouting.EnumerateKeys(treeId, shardCount);
        if (keys.Length == 1)
        {
            return await grainFactory.GetGrain<ITxRegistryGrain>(keys[0]).GetDecisionsRevisionAsync();
        }

        var tasks = new Task<long>[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            tasks[i] = grainFactory.GetGrain<ITxRegistryGrain>(keys[i]).GetDecisionsRevisionAsync();
        }

        var parts = await Task.WhenAll(tasks);
        long revision = 0;
        foreach (var part in parts)
        {
            revision += part;
        }

        return revision;
    }

    /// <summary>
    /// Returns the unioned decisions snapshot across every registry key of
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <returns>The unioned decisions.</returns>
    public static async Task<Dictionary<Guid, TxStatus>> SnapshotAsync(
        IGrainFactory grainFactory, string treeId, int shardCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var keys = TxRegistryRouting.EnumerateKeys(treeId, shardCount);
        if (keys.Length == 1)
        {
            return await grainFactory.GetGrain<ITxRegistryGrain>(keys[0]).SnapshotAsync();
        }

        var tasks = new Task<Dictionary<Guid, TxStatus>>[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            tasks[i] = grainFactory.GetGrain<ITxRegistryGrain>(keys[i]).SnapshotAsync();
        }

        var parts = await Task.WhenAll(tasks);
        var total = 0;
        foreach (var part in parts)
        {
            total += part?.Count ?? 0;
        }

        var decisions = new Dictionary<Guid, TxStatus>(total);
        foreach (var part in parts)
        {
            if (part is null) continue;
            foreach (var (txid, status) in part)
            {
                decisions[txid] = status;
            }
        }

        return decisions;
    }

    /// <summary>
    /// The number of revision-bracketed attempts <see cref="StableSnapshotAsync"/>
    /// makes before settling for the most recent union.
    /// </summary>
    public const int StableSnapshotAttempts = 4;

    /// <summary>
    /// Returns a decisions snapshot across every registry key of
    /// <paramref name="treeId"/> that is, whenever it can be established, a
    /// consistent cut. A single key's snapshot is one registry turn and so is
    /// always a cut. Across several keys the parallel reads land at different
    /// instants, so each attempt brackets the union with a second revision read:
    /// equal sums mean no key changed between its two reads, and every such
    /// interval contains the instant between the last first read and the first
    /// second read, so the union is the registry state at that instant. After
    /// <see cref="StableSnapshotAttempts"/> unstable attempts the most recent
    /// union is returned; it is still exact per saga (a txid lives on one key),
    /// so per-saga all-or-nothing visibility is unaffected.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <returns>The unioned decisions.</returns>
    public static async Task<Dictionary<Guid, TxStatus>> StableSnapshotAsync(
        IGrainFactory grainFactory, string treeId, int shardCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        if (TxRegistryRouting.EnumerateKeys(treeId, shardCount).Length == 1)
        {
            return await SnapshotAsync(grainFactory, treeId, shardCount);
        }

        TxRegistrySnapshot snapshot = default;
        for (var attempt = 0; attempt < StableSnapshotAttempts; attempt++)
        {
            snapshot = await SnapshotWithRevisionAsync(grainFactory, treeId, shardCount);
            var after = await GetDecisionsRevisionAsync(grainFactory, treeId, shardCount);
            if (after == snapshot.Revision)
            {
                break;
            }
        }

        return snapshot.Decisions ?? new Dictionary<Guid, TxStatus>();
    }

    /// <summary>
    /// Returns the cross-tree in-flight observation summed across every registry
    /// key of <paramref name="treeId"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <returns>The summed observation.</returns>
    public static async Task<CrossTreeInFlightObservation> ObserveCrossTreeInFlightAsync(
        IGrainFactory grainFactory, string treeId, int shardCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var keys = TxRegistryRouting.EnumerateKeys(treeId, shardCount);
        if (keys.Length == 1)
        {
            return await grainFactory.GetGrain<ITxRegistryGrain>(keys[0]).ObserveCrossTreeInFlightAsync();
        }

        var tasks = new Task<CrossTreeInFlightObservation>[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            tasks[i] = grainFactory.GetGrain<ITxRegistryGrain>(keys[i]).ObserveCrossTreeInFlightAsync();
        }

        var parts = await Task.WhenAll(tasks);
        var inFlight = 0;
        var unresolvable = 0;
        long epoch = 0;
        foreach (var part in parts)
        {
            inFlight += part.InFlightCount;
            unresolvable += part.UnresolvableCount;
            epoch += part.RegistrationEpoch;
        }

        return new CrossTreeInFlightObservation(inFlight, epoch, unresolvable);
    }

    /// <summary>
    /// Resolves the recorded status of every txid in <paramref name="txids"/>,
    /// grouping the ids by owning registry key so each key is asked once.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <param name="txids">The transaction ids to resolve.</param>
    /// <returns>The merged per-txid status map.</returns>
    public static async Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(
        IGrainFactory grainFactory, string treeId, int shardCount, IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(txids);

        var groups = GroupByKey(treeId, shardCount, txids);
        if (groups.Count == 1)
        {
            foreach (var (key, group) in groups)
            {
                return await grainFactory.GetGrain<ITxRegistryGrain>(key).GetStatusManyAsync(group);
            }
        }

        var tasks = new List<Task<Dictionary<Guid, TxStatus>>>(groups.Count);
        foreach (var (key, group) in groups)
        {
            tasks.Add(grainFactory.GetGrain<ITxRegistryGrain>(key).GetStatusManyAsync(group));
        }

        var parts = await Task.WhenAll(tasks);
        var merged = new Dictionary<Guid, TxStatus>(txids.Count);
        foreach (var part in parts)
        {
            if (part is null) continue;
            foreach (var (txid, status) in part)
            {
                merged[txid] = status;
            }
        }

        return merged;
    }

    /// <summary>
    /// Pins <paramref name="txids"/> under <paramref name="pinId"/> on each
    /// registry key that owns at least one of them. The
    /// <see cref="LatticeOptions.MaxPinnedSagaDecisions"/> cap is therefore
    /// enforced per shard.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <param name="pinId">The pin id.</param>
    /// <param name="txids">The transaction ids to pin.</param>
    /// <param name="ttl">The requested pin lifetime.</param>
    /// <returns>A task that completes when every owning key has recorded the pin.</returns>
    public static Task PinSnapshotAsync(
        IGrainFactory grainFactory, string treeId, int shardCount, Guid pinId, IReadOnlyCollection<Guid> txids, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(txids);

        var groups = GroupByKey(treeId, shardCount, txids);
        var tasks = new List<Task>(groups.Count);
        foreach (var (key, group) in groups)
        {
            tasks.Add(grainFactory.GetGrain<ITxRegistryGrain>(key).PinSnapshotAsync(pinId, group, ttl));
        }

        return tasks.Count == 1 ? tasks[0] : Task.WhenAll(tasks);
    }

    /// <summary>
    /// Refreshes the pin <paramref name="pinId"/> on each registry key that owns
    /// at least one of <paramref name="txids"/> (the set the pin was installed
    /// with). Returns <see langword="true"/> only when every owning key still
    /// holds the pin, so the eviction of any part of a snapshot's retention is
    /// reported.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <param name="pinId">The pin id.</param>
    /// <param name="txids">The transaction ids the pin was installed with.</param>
    /// <param name="ttl">The requested pin lifetime.</param>
    /// <returns>Whether every owning key refreshed the pin.</returns>
    public static async Task<bool> RefreshPinAsync(
        IGrainFactory grainFactory, string treeId, int shardCount, Guid pinId, IReadOnlyCollection<Guid> txids, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(txids);

        var groups = GroupByKey(treeId, shardCount, txids);
        if (groups.Count == 0)
        {
            // Nothing was pinned from this set, so fall back to the legacy key,
            // which is where a pin recorded before sharding lives.
            return await grainFactory.GetGrain<ITxRegistryGrain>(treeId).RefreshPinAsync(pinId, ttl);
        }

        var tasks = new List<Task<bool>>(groups.Count);
        foreach (var (key, _) in groups)
        {
            tasks.Add(grainFactory.GetGrain<ITxRegistryGrain>(key).RefreshPinAsync(pinId, ttl));
        }

        var results = await Task.WhenAll(tasks);
        foreach (var refreshed in results)
        {
            if (!refreshed) return false;
        }

        return true;
    }

    /// <summary>
    /// Removes the pin <paramref name="pinId"/> from every registry key of
    /// <paramref name="treeId"/>. Unpinning a key that never held the pin is a
    /// no-op, so the release does not depend on knowing which keys were pinned.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <param name="pinId">The pin id.</param>
    /// <returns>A task that completes when every key has released the pin.</returns>
    public static Task UnpinSnapshotAsync(IGrainFactory grainFactory, string treeId, int shardCount, Guid pinId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var keys = TxRegistryRouting.EnumerateKeys(treeId, shardCount);
        if (keys.Length == 1)
        {
            return grainFactory.GetGrain<ITxRegistryGrain>(keys[0]).UnpinSnapshotAsync(pinId);
        }

        var tasks = new Task[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            tasks[i] = grainFactory.GetGrain<ITxRegistryGrain>(keys[i]).UnpinSnapshotAsync(pinId);
        }

        return Task.WhenAll(tasks);
    }

    /// <summary>
    /// Groups <paramref name="txids"/> by owning registry grain key.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardCount">The configured shard count.</param>
    /// <param name="txids">The transaction ids.</param>
    /// <returns>The owning key to txid-list map.</returns>
    internal static Dictionary<string, List<Guid>> GroupByKey(string treeId, int shardCount, IEnumerable<Guid> txids)
    {
        var groups = new Dictionary<string, List<Guid>>(StringComparer.Ordinal);
        foreach (var txid in txids)
        {
            var key = TxRegistryRouting.ShardKey(treeId, txid, shardCount);
            if (!groups.TryGetValue(key, out var list))
            {
                list = [];
                groups[key] = list;
            }

            list.Add(txid);
        }

        return groups;
    }
}
