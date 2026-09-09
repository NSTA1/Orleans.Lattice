namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Warm-up surface for <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeGrain"/>. Pre-activates every
/// physical <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain"/> for this tree before the first
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

        // Warm-up pre-activates every physical shard root and each shard's
        // root-node grain, up to MaxWarmUpParallelism at a time and with retry, so
        // one cheap call costs the target tree a cluster-wide fan-out. It
        // previously performed no gate call at all, which made it both an
        // existence/topology oracle for an arbitrary tree id and - far worse than
        // the metadata verbs beside it - a cross-tenant activation storm: tenant
        // isolation is composed inside the gate, so a verb that never calls the
        // gate never reaches the tenant enforcer, and any in-cluster caller could
        // force that fan-out against another tenant's tree.
        //
        // Gated at whole-tree Read, matching its true siblings DiagnoseAsync and
        // GetStorageUsageAsync: operational verbs that return no key data. Read is
        // the minimum authority that closes the hole, and deliberately not Admin,
        // because warm-up exists to be called by ordinary data-plane producers
        // ahead of their first write - requiring admin rights would break its
        // documented purpose on any auth-enabled cluster.
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);

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
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                LatticeTenantLabel.ForTree(TreeId));
            LatticeMetrics.WarmUpDurationMs.Record(
                sw.Elapsed.TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>("shard_count", shardCount),
                LatticeTenantLabel.ForTree(TreeId));
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
                await ShardActivationRetry.RunAsync(
                    () => shard.WarmUpAsync(),
                    CancellationToken.None);
            }
            finally
            {
                gate.Release();
            }
        }
    }
}
