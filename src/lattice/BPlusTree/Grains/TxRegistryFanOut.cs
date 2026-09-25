namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Tree-wide operations over a sharded saga decision registry (issue #3501).
/// Each method addresses every registry grain key returned by
/// <see cref="TxRegistryRouting.EnumerateKeys"/> for the tree's durable shard
/// high-water mark (every shard that may hold a decision, plus the legacy
/// bare-tree-id registry) and folds the per-key answers into the
/// single-registry shape the callers already consume. Per-txid operations are
/// routed by <see cref="TxRegistryRouting.GetRegistry"/> instead.
/// <para>
/// <b>Key-set resolution.</b> The key set never depends on the calling silo's
/// configured <see cref="LatticeOptions.TxRegistryShardCount"/>. A read starts
/// from the per-silo <see cref="TxRegistryHighWaterCache"/> entry and, in
/// parallel with its fan-out, reads the durable
/// <see cref="ITxRegistryHighWaterGrain"/> mark. When the mark exceeds the key
/// set the read used, the whole read is discarded and re-run over the wider
/// set; the mark only grows and is bounded by
/// <see cref="LatticeOptions.MaxTxRegistryShardCount"/>, so this terminates.
/// A shard raises the mark durably before its first write, so a mark read that
/// does not cover a shard happened before that shard held anything: the
/// shard is exactly as empty as if the read had reached it at that instant.
/// Every answer of one call is therefore computed over one key set.
/// </para>
/// <para>
/// Aggregation rules:
/// </para>
/// <list type="bullet">
/// <item><description>Decisions are unioned. A txid lives on exactly one key, so the union never has to choose between two readings.</description></item>
/// <item><description>The decisions revision is the sum of the per-key revisions. Each per-key revision is non-decreasing and widening the key set only adds non-negative terms, so the sum is non-decreasing across calls, and equal across two reads over one key set only when every per-key revision is, which is exactly the equality the reader stability gate relies on.</description></item>
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
    /// The number of revision-bracketed attempts <see cref="StableSnapshotAsync"/>
    /// makes before settling for the most recent union.
    /// </summary>
    public const int StableSnapshotAttempts = 4;

    /// <summary>
    /// Returns a self-consistent decisions snapshot and revision across every
    /// registry key of <paramref name="treeId"/>. Each key's pair is captured
    /// atomically in one registry turn.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <returns>The unioned decisions and summed revision.</returns>
    public static async Task<TxRegistrySnapshot> SnapshotWithRevisionAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var (snapshot, _) = await SnapshotWithRevisionCoveringAsync(grainFactory, treeId, TxRegistryHighWaterCache.Get(grainFactory, treeId));
        return snapshot;
    }

    /// <summary>
    /// Returns the summed decisions revision across every registry key of
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <returns>The summed revision.</returns>
    public static async Task<long> GetDecisionsRevisionAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var (parts, _) = await FanOutAsync(
            grainFactory, treeId, TxRegistryHighWaterCache.Get(grainFactory, treeId), static registry => registry.GetDecisionsRevisionAsync());
        return Sum(parts);
    }

    /// <summary>
    /// Returns the unioned decisions snapshot across every registry key of
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <returns>The unioned decisions.</returns>
    public static async Task<Dictionary<Guid, TxStatus>> SnapshotAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var (parts, _) = await FanOutAsync(
            grainFactory, treeId, TxRegistryHighWaterCache.Get(grainFactory, treeId), static registry => registry.SnapshotAsync());
        if (parts.Length == 1)
        {
            return parts[0] ?? new Dictionary<Guid, TxStatus>();
        }

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
    /// Returns a decisions snapshot across every registry key of
    /// <paramref name="treeId"/> that is, whenever it can be established, a
    /// consistent cut. A single key's snapshot is one registry turn and so is
    /// always a cut. Across several keys the parallel reads land at different
    /// instants, so each attempt brackets the union with a second revision read
    /// over the same key set (and a second high-water read): equal sums and an
    /// unwidened mark mean no key changed between its two reads and no uncovered
    /// shard held anything, and every such interval contains the instant between
    /// the last first read and the first second read, so the union is the
    /// registry state at that instant. After <see cref="StableSnapshotAttempts"/>
    /// unstable attempts the most recent union is returned; it is still exact
    /// per saga (a txid lives on one key), so per-saga all-or-nothing visibility
    /// is unaffected.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <returns>The unioned decisions.</returns>
    public static async Task<Dictionary<Guid, TxStatus>> StableSnapshotAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var highWater = TxRegistryHighWaterCache.Get(grainFactory, treeId);
        if (highWater <= 0)
        {
            // Legacy key only as far as this silo knows: one registry turn is
            // already a cut, exactly the pre-sharding read. Only when the durable
            // mark shows shards does the bracketed union below take over.
            var (legacy, covered) = await FanOutAsync(
                grainFactory, treeId, highWater, static registry => registry.SnapshotAsync());
            if (covered <= 0)
            {
                return legacy[0];
            }

            highWater = covered;
        }

        TxRegistrySnapshot snapshot = default;
        for (var attempt = 0; attempt < StableSnapshotAttempts; attempt++)
        {
            (snapshot, highWater) = await SnapshotWithRevisionCoveringAsync(grainFactory, treeId, highWater);
            if (highWater <= 0)
            {
                // Legacy key only: one registry turn is already a cut.
                break;
            }

            var (after, widened) = await FanOutOnceAsync(
                grainFactory, treeId, highWater, static registry => registry.GetDecisionsRevisionAsync());
            if (widened > highWater)
            {
                highWater = widened;
                continue;
            }

            if (Sum(after) == snapshot.Revision)
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
    /// <returns>The summed observation.</returns>
    public static async Task<CrossTreeInFlightObservation> ObserveCrossTreeInFlightAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var (parts, _) = await FanOutAsync(
            grainFactory, treeId, TxRegistryHighWaterCache.Get(grainFactory, treeId), static registry => registry.ObserveCrossTreeInFlightAsync());
        if (parts.Length == 1)
        {
            return parts[0];
        }

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
    /// <param name="txids">The transaction ids to resolve.</param>
    /// <returns>The merged per-txid status map.</returns>
    public static async Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(
        IGrainFactory grainFactory, string treeId, IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(txids);

        var groups = GroupByKey(treeId, txids);
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
    /// <param name="pinId">The pin id.</param>
    /// <param name="txids">The transaction ids to pin.</param>
    /// <param name="ttl">The requested pin lifetime.</param>
    /// <returns>A task that completes when every owning key has recorded the pin.</returns>
    public static Task PinSnapshotAsync(
        IGrainFactory grainFactory, string treeId, Guid pinId, IReadOnlyCollection<Guid> txids, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(txids);

        var groups = GroupByKey(treeId, txids);
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
    /// <param name="pinId">The pin id.</param>
    /// <param name="txids">The transaction ids the pin was installed with.</param>
    /// <param name="ttl">The requested pin lifetime.</param>
    /// <returns>Whether every owning key refreshed the pin.</returns>
    public static async Task<bool> RefreshPinAsync(
        IGrainFactory grainFactory, string treeId, Guid pinId, IReadOnlyCollection<Guid> txids, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(txids);

        var groups = GroupByKey(treeId, txids);
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
    /// <param name="pinId">The pin id.</param>
    /// <returns>A task that completes when every key has released the pin.</returns>
    public static async Task UnpinSnapshotAsync(IGrainFactory grainFactory, string treeId, Guid pinId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        await FanOutAsync(
            grainFactory,
            treeId,
            TxRegistryHighWaterCache.Get(grainFactory, treeId),
            async registry =>
            {
                await registry.UnpinSnapshotAsync(pinId);
                return true;
            });
    }

    /// <summary>
    /// Groups <paramref name="txids"/> by owning registry grain key. Routing is a
    /// pure function of each id, so no shard count takes part.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="txids">The transaction ids.</param>
    /// <returns>The owning key to txid-list map.</returns>
    internal static Dictionary<string, List<Guid>> GroupByKey(string treeId, IEnumerable<Guid> txids)
    {
        var groups = new Dictionary<string, List<Guid>>(StringComparer.Ordinal);
        foreach (var txid in txids)
        {
            var key = TxRegistryRouting.ShardKey(treeId, txid);
            if (!groups.TryGetValue(key, out var list))
            {
                list = [];
                groups[key] = list;
            }

            list.Add(txid);
        }

        return groups;
    }

    /// <summary>
    /// Snapshot-with-revision over the keys covering at least
    /// <paramref name="highWater"/>, widened to the durable mark. Returns the
    /// high-water the answer was computed over.
    /// </summary>
    private static async Task<(TxRegistrySnapshot Snapshot, int HighWater)> SnapshotWithRevisionCoveringAsync(
        IGrainFactory grainFactory, string treeId, int highWater)
    {
        var (parts, covered) = await FanOutAsync(
            grainFactory, treeId, highWater, static registry => registry.SnapshotWithRevisionAsync());
        if (parts.Length == 1)
        {
            return (parts[0], covered);
        }

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

        return (new TxRegistrySnapshot { Decisions = decisions, Revision = revision }, covered);
    }

    /// <summary>
    /// Runs <paramref name="call"/> on every registry key covering
    /// <paramref name="highWater"/>, re-running over a wider key set until the
    /// durable mark read alongside the fan-out no longer exceeds it. Returns the
    /// per-key answers and the high-water they cover.
    /// </summary>
    private static async Task<(T[] Parts, int HighWater)> FanOutAsync<T>(
        IGrainFactory grainFactory, string treeId, int highWater, Func<ITxRegistryGrain, Task<T>> call)
    {
        while (true)
        {
            var (parts, mark) = await FanOutOnceAsync(grainFactory, treeId, highWater, call);
            if (mark <= highWater)
            {
                return (parts, highWater);
            }

            highWater = mark;
        }
    }

    /// <summary>
    /// One fan-out of <paramref name="call"/> over the keys covering
    /// <paramref name="highWater"/>, in parallel with a read of the durable
    /// mark. Returns the per-key answers and the mark read (recorded in the
    /// per-silo cache).
    /// </summary>
    private static async Task<(T[] Parts, int Mark)> FanOutOnceAsync<T>(
        IGrainFactory grainFactory, string treeId, int highWater, Func<ITxRegistryGrain, Task<T>> call)
    {
        var keys = TxRegistryRouting.EnumerateKeys(treeId, highWater);
        var markTask = grainFactory.GetGrain<ITxRegistryHighWaterGrain>(treeId).GetShardHighWaterAsync();
        var tasks = new Task<T>[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            tasks[i] = call(grainFactory.GetGrain<ITxRegistryGrain>(keys[i]));
        }

        var parts = await Task.WhenAll(tasks);
        var mark = TxRegistryHighWaterCache.Observe(grainFactory, treeId, await markTask);
        return (parts, mark);
    }

    private static long Sum(long[] parts)
    {
        long total = 0;
        foreach (var part in parts)
        {
            total += part;
        }

        return total;
    }
}
