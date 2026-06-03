using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ICommitLogWriter"/> registered by
/// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, System.Action{LatticeOptions}?)"/>.
/// Routes a producer-built <see cref="WalRecord"/> to the per-shard
/// <see cref="IWalShardGrain.AppendAsync"/> entry point so the caller
/// observes the per-shard sequence number.
/// <para>
/// Producer call sites (the leaf grain's foreground commit path, the
/// shard-root saga terminal path) construct the <see cref="WalRecord"/>
/// directly and forward it through this adapter. The adapter applies
/// two producer-side stamps that are uniform across every leaf in the
/// tree and therefore live on the adapter rather than at every call
/// site:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="WalRecord.Mode"/> from the registered
///   <see cref="ILatticeMergeModeResolver"/>; defaults to
///   <see cref="LatticeMergeMode.LwwRegister"/> when no resolver is
///   registered (single-cluster deployments).</description></item>
///   <item><description><see cref="WalRecord.OriginClusterId"/>
///   fallback from the registered
///   <see cref="ILatticeOriginClusterIdResolver"/> when the producer
///   did not stamp an origin (a remote replay's origin already wins
///   when present, mirroring the historical converter behaviour).</description></item>
/// </list>
/// <para>
/// Bypasses <c>IReplogSink</c> by design - the replication-package sink
/// seam returns <see cref="System.Threading.Tasks.Task"/> rather than
/// <see cref="System.Threading.Tasks.Task{Long}"/>, and the leaf
/// commit path needs the assigned offset to drive replay coordination
/// after a leaf reactivation.
/// </para>
/// <para>
/// A complementary short-circuit on the replication mutation observer
/// suppresses double WAL appends from the post-commit observer dispatch
/// when the foreground commit path has already appended the same
/// mutation.
/// </para>
/// </summary>
internal sealed class WalCommitLogWriter(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> options,
    LatticeOptionsResolver optionsResolver,
    ILatticeMergeModeResolver modeResolver,
    ILatticeOriginClusterIdResolver clusterIdResolver) : ICommitLogWriter
{
    /// <inheritdoc />
    public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var (stamped, partition, walPartitions, perTree) = await RouteAsync(entry).ConfigureAwait(false);
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{stamped.TreeId}/{partition}");

        // A2 cross-grain dispatch attribution: clock the awaited grain
        // RPC on the caller side so the Orleans turn-queue wait at the
        // target WalShardGrain activation becomes visible. Subtracting
        // WalAppendTurnWait (the WAL grain's own self-clock) from this
        // histogram isolates the scheduling tax on the single WAL
        // activation per partition - the dominant cost under the
        // legacy WalPartitions = 1 shape.
        var dispatchStartTicks = Stopwatch.GetTimestamp();
        try
        {
            // Writer-side dispatch deadline. The outbound
            // IWalShardGrain RPC is the outermost observable seam on the
            // write pipeline; without a writer-side bound a wedged shard
            // activation holds every caller's dispatch parked until the
            // Orleans response timeout (default 3 minutes) expires.
            // Bounding the dispatch converts that blind hang into a
            // structured TimeoutException with per-shard counter
            // attribution (WalAppendDispatchTimeouts), so the request
            // pipeline releases its slot immediately and the wedged
            // shard is identified in O(WalAppendDispatchTimeout) time.
            var dispatchTimeout = perTree.WalAppendDispatchTimeout;
            var grainCall = grain.AppendAsync(stamped, cancellationToken);
            if (dispatchTimeout == Timeout.InfiniteTimeSpan)
            {
                return await grainCall.ConfigureAwait(false);
            }
            try
            {
                return await grainCall.WaitAsync(dispatchTimeout, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Empirical diagnostic (paired with the metric below): on prior
                // cohort runs the WalAppendDispatchTimeouts counter read zero even
                // though every shipped deadline on the path had elapsed many times
                // over against the parked dispatches. The Console.WriteLine here
                // distinguishes "catch never entered" (line absent from silo log)
                // from "counter silently dropped" (line present, counter still 0).
                // Loud prefix matches the existing [silo] / [stall-watchdog] log
                // conventions on the azure-throughput silo so a single grep over
                // the silo log surfaces every trip.
                System.Console.WriteLine($"[wal-dispatch-timeout] tree={stamped.TreeId} shard={partition} entries=1 timeout={dispatchTimeout}");
                LatticeMetrics.WalAppendDispatchTimeouts.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, stamped.TreeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition));
                throw new TimeoutException(
                    $"WAL append dispatch to shard {partition} of tree '{stamped.TreeId}' "
                    + $"exceeded the {dispatchTimeout} dispatch deadline "
                    + $"({nameof(LatticeOptions.WalAppendDispatchTimeout)}); the target "
                    + $"WalShardGrain activation did not return within the deadline, "
                    + $"indicating a wedged shard.");
            }
        }
        finally
        {
            RecordDispatchOutcome(stamped.TreeId, partition, walPartitions, perTree, entryCount: 1, dispatchStartTicks);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        var count = entries.Count;
        if (count == 0)
        {
            return Array.Empty<long>();
        }

        // Fast path: single entry collapses to the per-entry overload
        // so the per-call allocation cost matches AppendAsync for the
        // dominant SetMany([single]) case.
        if (count == 1)
        {
            var offset = await AppendAsync(entries[0], cancellationToken).ConfigureAwait(false);
            return new[] { offset };
        }

        // Group by (treeId, partition) while preserving the caller's
        // input order via per-entry reverse-indexes. Most batches
        // share a single treeId, but the grouping key includes it so
        // a hand-constructed cross-tree batch still routes correctly.
        var partitionEntries = new Dictionary<string, List<WalRecord>>(StringComparer.Ordinal);
        var partitionReverse = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        // Captured alongside partitionEntries so the per-partition
        // dispatch histogram (A2) can tag the tree id / partition /
        // WalPartitions / WalMaxPendingBatches without re-resolving
        // the options on the metric path.
        var partitionMeta = new Dictionary<string, (string TreeId, int Partition, int WalPartitions, LatticeOptions PerTree)>(StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var (stamped, partition, walPartitions, perTree) = await RouteAsync(entries[i]).ConfigureAwait(false);
            var grainKey = $"{stamped.TreeId}/{partition}";
            if (!partitionEntries.TryGetValue(grainKey, out var list))
            {
                list = new List<WalRecord>();
                partitionEntries[grainKey] = list;
                partitionReverse[grainKey] = new List<int>();
                partitionMeta[grainKey] = (stamped.TreeId, partition, walPartitions, perTree);
            }
            list.Add(stamped);
            partitionReverse[grainKey].Add(i);
        }

        var offsets = new long[count];
        // Dispatch every partition's batch in parallel; per-partition
        // ordering inside each grain call is preserved by AppendBatchAsync's
        // contract. Using Task.WhenAll keeps the cross-partition fan-out
        // independent so a slow partition does not serialise the others.
        var tasks = new Task<KeyValuePair<string, IReadOnlyList<long>>>[partitionEntries.Count];
        var t = 0;
        foreach (var (grainKey, list) in partitionEntries)
        {
            var grain = grainFactory.GetGrain<IWalShardGrain>(grainKey);
            var meta = partitionMeta[grainKey];
            tasks[t++] = AppendForPartitionAsync(grainKey, grain, list, meta.TreeId, meta.Partition, meta.WalPartitions, meta.PerTree, cancellationToken);
        }
        var partitionResults = await Task.WhenAll(tasks).ConfigureAwait(false);

        // Stitch the per-partition offsets back into the caller's
        // input order.
        foreach (var kv in partitionResults)
        {
            var indexes = partitionReverse[kv.Key];
            var partitionOffsets = kv.Value;
            for (var i = 0; i < indexes.Count; i++)
            {
                offsets[indexes[i]] = partitionOffsets[i];
            }
        }
        return offsets;
    }

    private static async Task<KeyValuePair<string, IReadOnlyList<long>>> AppendForPartitionAsync(
        string grainKey,
        IWalShardGrain grain,
        IReadOnlyList<WalRecord> entries,
        string treeId,
        int partition,
        int walPartitions,
        LatticeOptions perTree,
        CancellationToken cancellationToken)
    {
        // A2 cross-grain dispatch attribution on the batched path:
        // mirrors the single-entry overload so AppendBatchAsync's
        // per-partition fan-out is attributable too. Each partition
        // gets one observation per AppendManyAsync call.
        var dispatchStartTicks = Stopwatch.GetTimestamp();
        try
        {
            // Writer-side dispatch deadline (batched path); see
            // AppendAsync above for the rationale. Held on the per-tree
            // perTree.WalAppendDispatchTimeout so per-tree overrides
            // apply uniformly to the single-entry and batched dispatches.
            var dispatchTimeout = perTree.WalAppendDispatchTimeout;
            var grainCall = grain.AppendBatchAsync(entries, cancellationToken);
            IReadOnlyList<long> offsets;
            if (dispatchTimeout == Timeout.InfiniteTimeSpan)
            {
                offsets = await grainCall.ConfigureAwait(false);
            }
            else
            {
                try
                {
                    offsets = await grainCall.WaitAsync(dispatchTimeout, cancellationToken).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    // Empirical diagnostic: see the single-entry AppendAsync
                    // catch above for the rationale. Same conventions.
                    System.Console.WriteLine($"[wal-dispatch-timeout] tree={treeId} shard={partition} entries={entries.Count} timeout={dispatchTimeout}");
                    LatticeMetrics.WalAppendDispatchTimeouts.Add(
                        1,
                        new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                        new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition));
                    throw new TimeoutException(
                        $"WAL append-batch dispatch to shard {partition} of tree '{treeId}' "
                        + $"({entries.Count} entries) exceeded the {dispatchTimeout} dispatch deadline "
                        + $"({nameof(LatticeOptions.WalAppendDispatchTimeout)}); the target "
                        + $"WalShardGrain activation did not return within the deadline, "
                        + $"indicating a wedged shard.");
                }
            }
            return new KeyValuePair<string, IReadOnlyList<long>>(grainKey, offsets);
        }
        finally
        {
            RecordDispatchOutcome(treeId, partition, walPartitions, perTree, entryCount: entries.Count, dispatchStartTicks);
        }
    }

    private static void RecordDispatchOutcome(string treeId, int partition, int walPartitions, LatticeOptions perTree, int entryCount, long startTicks)
    {
        var elapsedMs = Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition);
        // walPartitions tag must reflect the tree-registry pinned value
        // (routing-truth) rather than the live IOptionsMonitor value,
        // so the metric attribution matches the writer-side routing
        // shape exactly.
        var walPartitionsTag = new KeyValuePair<string, object?>(LatticeMetrics.TagWalPartitions, walPartitions);
        var walMaxPendingTag = new KeyValuePair<string, object?>(LatticeMetrics.TagWalMaxPendingBatches, perTree.WalMaxPendingBatches);
        LatticeMetrics.WalShardDispatchDuration.Record(elapsedMs, treeTag, shardTag, walPartitionsTag, walMaxPendingTag);
        LatticeMetrics.WalShardDispatchEntries.Record(entryCount, treeTag, shardTag, walPartitionsTag, walMaxPendingTag);
    }

    /// <summary>
    /// Stamps the producer-side <see cref="WalRecord.Mode"/> and
    /// fallback <see cref="WalRecord.OriginClusterId"/> on
    /// <paramref name="entry"/> and computes the WAL partition the
    /// entry lands on. Pulled out so the single-entry and batched
    /// overloads share the same routing semantics by construction; the
    /// saga-terminal shard-index-in-key contract therefore applies
    /// identically to both paths. Returns the resolved per-tree
    /// options alongside the routed entry so the cross-grain dispatch
    /// histogram (A2 attribution) can tag <c>WalPartitions</c> /
    /// <c>WalMaxPendingBatches</c> without a second
    /// <c>IOptionsMonitor.Get</c> on the metric path.
    /// <para>
    /// <see cref="LatticeOptions.WalPartitions"/> is sourced from the
    /// tree-registry pin via <see cref="LatticeOptionsResolver"/>, not
    /// from <see cref="IOptionsMonitor{TOptions}"/>, so the writer-side
    /// routing and the activation-time materialiser always agree on
    /// the partition fan-out shape for the lifetime of the tree -
    /// flipping the silo's live <see cref="LatticeOptions.WalPartitions"/>
    /// value after the tree has accepted writes cannot silently
    /// re-route new writes into partitions the materialiser is not
    /// configured to read from.
    /// </para>
    /// </summary>
    private async Task<(WalRecord Entry, int Partition, int WalPartitions, LatticeOptions PerTree)> RouteAsync(WalRecord entry)
    {
        // Resolve WalPartitions through the resolver's per-tree fast-path
        // cache so the foreground commit path does not pay a
        // ILatticeRegistry grain RPC per append. The pin is established
        // at first RegisterAsync and is tree-immutable thereafter, so
        // the cache is correct by construction (see the resolver's
        // GetWalPartitionsAsync docstring); the resolver's
        // GetWalPartitionsAsync returns a synchronously-completed
        // ValueTask on a cache hit and falls back to the registry only
        // on a cold tree's first hit.
        //
        // Other per-tree options not covered by the registry pin
        // (WalMaxPendingBatches and friends used by the dispatch
        // histogram below) are still read from the live IOptionsMonitor
        // here - they are dynamic-tunable by design.
        var partitions = await optionsResolver.GetWalPartitionsAsync(entry.TreeId).ConfigureAwait(false);
        var perTree = options.Get(entry.TreeId);

        var resolvedMode = modeResolver.Resolve(entry.TreeId) ?? LatticeMergeMode.LwwRegister;

        var resolvedOrigin = string.IsNullOrEmpty(entry.OriginClusterId)
            ? clusterIdResolver.Resolve(entry.TreeId)
            : entry.OriginClusterId;

        // Defensive snapshot of the producer-side frontier - the
        // historical WalRecordConverter.ToWalRecord cloned here so a
        // post-emit advance of the leaf-side VersionVector reference
        // could not mutate the captured WAL entry. Clone once and alias
        // it into both VectorClock and DependencySummary so receivers
        // that read either slot observe the same frontier (matches the
        // pre-builder wire shape produced by the converter).
        var capturedFrontier = entry.VectorClock?.Clone();

        var stamped = entry with
        {
            Key = entry.Key ?? string.Empty,
            Mode = resolvedMode,
            OriginClusterId = resolvedOrigin,
            VectorClock = capturedFrontier,
            DependencySummary = capturedFrontier,
        };

        int partition;
        if (stamped.Op is MutationKind.TxCommit or MutationKind.TxAbort)
        {
            if (!int.TryParse(stamped.Key, NumberStyles.Integer, CultureInfo.InvariantCulture, out var shardIndex))
            {
                throw new InvalidOperationException(
                    $"Saga terminal entry must carry the shard index in entry.Key as a base-10 integer; got '{stamped.Key}'.");
            }
            if (shardIndex < 0)
            {
                throw new InvalidOperationException(
                    $"Saga terminal entry shard index {shardIndex} is negative for tree '{stamped.TreeId}'.");
            }
            partition = shardIndex % partitions;
        }
        else
        {
            partition = WalPartitionHash.Compute(stamped.Key, partitions);
        }
        return (stamped, partition, partitions, perTree);
    }
}
