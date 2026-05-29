namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Warm-up surface for <see cref="LatticeGrain"/>. Pre-activates every
/// physical <see cref="IShardRootGrain"/> for this tree before the first
/// hot-path write lands, so the placement-directory and grain-storage
/// first-touch cost is absorbed while the silo is idle rather than against
/// producer-driven flush concurrency. The dedicated partial keeps the
/// startup-helper surface readable on its own and matches the file-per-
/// concern convention the rest of the grain follows.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Upper bound on the warm-up fan-out's degree of parallelism. The
    /// per-tree value is <c>min(physicalShardCount, MaxWarmUpParallelism)</c>;
    /// trees with very few shards naturally cap below this number. Bounded
    /// to keep the warm-up itself from becoming a self-inflicted activation
    /// storm.
    /// </summary>
    private const int MaxWarmUpParallelism = 32;

    /// <inheritdoc />
    public async Task WarmUpAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var (physicalTreeId, shardMap) = await GetRoutingAsync(cancellationToken);
        var physicalIndices = shardMap.GetPhysicalShardIndices();
        var shardCount = physicalIndices.Count;
        if (shardCount == 0)
        {
            sw.Stop();
            return;
        }

        var parallelism = Math.Min(shardCount, MaxWarmUpParallelism);
        using var gate = new SemaphoreSlim(parallelism, parallelism);
        var probes = new List<Task>(shardCount);
        foreach (var idx in physicalIndices)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await gate.WaitAsync(cancellationToken);
            probes.Add(ProbeShardAsync(this, physicalTreeId, idx, gate));
        }

        try
        {
            await Task.WhenAll(probes);
        }
        finally
        {
            sw.Stop();
            LatticeMetrics.WarmUpInvocations.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
            LatticeMetrics.WarmUpDurationMs.Record(
                sw.Elapsed.TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>("shard_count", shardCount));
        }

        static async Task ProbeShardAsync(LatticeGrain self, string physicalTreeId, int shardIndex, SemaphoreSlim gate)
        {
            try
            {
                var shard = self.GetShardGrainByIndex(physicalTreeId, shardIndex);
                // IShardRootGrain.WarmUpAsync is the dedicated read-only
                // probe contract: it activates the shard root AND pre-
                // activates the shard's current root-node grain (root
                // leaf when the tree is flat, root internal node
                // otherwise). For the throughput benchmark's empty-tree
                // cold start, this means every shard's root leaf is
                // activated before producers ever connect - the only
                // grain reachable before traffic that the first writes
                // must touch on the traversal path.
                await shard.WarmUpAsync();
            }
            finally
            {
                gate.Release();
            }
        }
    }
}
