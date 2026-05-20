using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ICommitLogWriter"/> registered by
/// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, System.Action{LatticeOptions}?)"/>.
/// Translates a core <see cref="LatticeMutation"/> to a wire-shaped
/// <see cref="WalRecord"/> via <see cref="WalRecordConverter"/> and
/// dispatches it to the per-shard
/// <see cref="IWalShardGrain.AppendAsync"/> entry point so the
/// caller observes the per-shard sequence number.
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
    public async Task<long> AppendAsync(LatticeMutation mutation, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var (entry, partition) = Route(mutation);
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{entry.TreeId}/{partition}");
        return await grain.AppendAsync(entry, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<LatticeMutation> mutations, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mutations);
        cancellationToken.ThrowIfCancellationRequested();

        var count = mutations.Count;
        if (count == 0)
        {
            return Array.Empty<long>();
        }

        // Fast path: single mutation collapses to the per-mutation
        // overload so the per-call allocation cost matches AppendAsync
        // for the dominant SetMany([single]) case.
        if (count == 1)
        {
            var offset = await AppendAsync(mutations[0], cancellationToken).ConfigureAwait(false);
            return new[] { offset };
        }

        // Group by (treeId, partition) while preserving the caller's
        // input order via per-mutation reverse-indexes. Most batches
        // share a single treeId, but the grouping key includes it so
        // a hand-constructed cross-tree batch still routes correctly.
        var partitionEntries = new Dictionary<string, List<WalRecord>>(StringComparer.Ordinal);
        var partitionReverse = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var (entry, partition) = Route(mutations[i]);
            var grainKey = $"{entry.TreeId}/{partition}";
            if (!partitionEntries.TryGetValue(grainKey, out var list))
            {
                list = new List<WalRecord>();
                partitionEntries[grainKey] = list;
                partitionReverse[grainKey] = new List<int>();
            }
            list.Add(entry);
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
            tasks[t++] = AppendForPartitionAsync(grainKey, grain, list, cancellationToken);
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
        CancellationToken cancellationToken)
    {
        var offsets = await grain.AppendBatchAsync(entries, cancellationToken).ConfigureAwait(false);
        return new KeyValuePair<string, IReadOnlyList<long>>(grainKey, offsets);
    }

    /// <summary>
    /// Maps a foreground <see cref="LatticeMutation"/> to its on-wire
    /// <see cref="WalRecord"/> and the WAL partition it lands on.
    /// Pulled out so the single-mutation and batched overloads share
    /// the same routing semantics by construction; the saga-terminal
    /// shard-index-in-key contract therefore applies identically to
    /// both paths.
    /// </summary>
    private (WalRecord Entry, int Partition) Route(LatticeMutation mutation)
    {
        var perTree = options.Get(mutation.TreeId);
        var partitions = perTree.WalPartitions;

        var mode = modeResolver.Resolve(mutation.TreeId) ?? LatticeMergeMode.LwwRegister;

        var entry = WalRecordConverter.ToWalRecord(
            mutation,
            mode,
            originClusterId: clusterIdResolver.Resolve(mutation.TreeId));

        int partition;
        if (mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort)
        {
            if (!int.TryParse(entry.Key, NumberStyles.Integer, CultureInfo.InvariantCulture, out var shardIndex))
            {
                throw new InvalidOperationException(
                    $"Saga terminal mutation must carry the shard index in mutation.Key as a base-10 integer; got '{entry.Key}'.");
            }
            if (shardIndex < 0)
            {
                throw new InvalidOperationException(
                    $"Saga terminal mutation shard index {shardIndex} is negative for tree '{mutation.TreeId}'.");
            }
            partition = shardIndex % partitions;
        }
        else
        {
            partition = WalPartitionHash.Compute(entry.Key, partitions);
        }
        return (entry, partition);
    }
}
