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
    ILatticeMergeModeResolver modeResolver,
    ILatticeOriginClusterIdResolver clusterIdResolver) : ICommitLogWriter
{
    /// <inheritdoc />
    public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var (stamped, partition, perTree) = Route(entry);
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{stamped.TreeId}/{partition}");

        // A2 cross-grain dispatch attribution: clock the awaited grain
        // RPC on the caller side so the Orleans turn-queue wait at the
        // target WalShardGrain activation becomes visible. Subtracting
        // WalAppendTurnWait (the WAL grain's own self-clock) from this
        // histogram isolates the scheduling tax on the single WAL
        // activation per partition - the dominant cost under the
        // default WalPartitions = 1.
        var dispatchStartTicks = Stopwatch.GetTimestamp();
        try
        {
            return await grain.AppendAsync(stamped, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            RecordDispatchOutcome(stamped.TreeId, partition, perTree, entryCount: 1, dispatchStartTicks);
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
        var partitionMeta = new Dictionary<string, (string TreeId, int Partition, LatticeOptions PerTree)>(StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var (stamped, partition, perTree) = Route(entries[i]);
            var grainKey = $"{stamped.TreeId}/{partition}";
            if (!partitionEntries.TryGetValue(grainKey, out var list))
            {
                list = new List<WalRecord>();
                partitionEntries[grainKey] = list;
                partitionReverse[grainKey] = new List<int>();
                partitionMeta[grainKey] = (stamped.TreeId, partition, perTree);
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
            tasks[t++] = AppendForPartitionAsync(grainKey, grain, list, meta.TreeId, meta.Partition, meta.PerTree, cancellationToken);
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
            var offsets = await grain.AppendBatchAsync(entries, cancellationToken).ConfigureAwait(false);
            return new KeyValuePair<string, IReadOnlyList<long>>(grainKey, offsets);
        }
        finally
        {
            RecordDispatchOutcome(treeId, partition, perTree, entryCount: entries.Count, dispatchStartTicks);
        }
    }

    private static void RecordDispatchOutcome(string treeId, int partition, LatticeOptions perTree, int entryCount, long startTicks)
    {
        var elapsedMs = Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition);
        var walPartitionsTag = new KeyValuePair<string, object?>(LatticeMetrics.TagWalPartitions, perTree.WalPartitions);
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
    /// </summary>
    private (WalRecord Entry, int Partition, LatticeOptions PerTree) Route(WalRecord entry)
    {
        var perTree = options.Get(entry.TreeId);
        var partitions = perTree.WalPartitions;

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
        return (stamped, partition, perTree);
    }
}
