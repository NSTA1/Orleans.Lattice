using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree byte-accurate storage-usage aggregator. Normal single-activation
/// grain keyed by <c>treeId</c>. On cache-miss fans out to every physical
/// shard root (leaf-state + snapshot bytes) and every WAL partition (retained
/// WAL bytes), caching the assembled <see cref="TreeStorageUsageReport"/> for
/// <see cref="LatticeOptions.StorageUsageCacheTtl"/>.
/// </summary>
internal sealed class LatticeStorageUsageGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver,
    LatticeStorageUsageMetrics metrics,
    ILogger<LatticeStorageUsageGrain> logger) : ILatticeStorageUsage
{
    private string TreeId => context.GrainId.Key.ToString()!;

    private TreeStorageUsageReport? _cached;

    /// <inheritdoc />
    public async Task<TreeStorageUsageReport> GetReportAsync(bool forceRefresh, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var options = await optionsResolver.ResolveAsync(TreeId);
        var ttl = options.StorageUsageCacheTtl;
        var now = DateTimeOffset.UtcNow;

        if (!forceRefresh && _cached is { } c && ttl > TimeSpan.Zero && (now - c.SampledAt) < ttl)
        {
            PublishToMetrics(c, options);
            return c;
        }

        var report = await BuildReportAsync(cancellationToken);
        _cached = report;
        PublishToMetrics(report, options);
        return report;
    }

    /// <summary>
    /// Pushes the freshly served report to the observable-gauge sink so a
    /// subsequent meter scrape reflects it without re-fanning out. When the
    /// byte-pressure policy is enabled for the tree
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/> set and positive) and
    /// the WAL surface is supported (not <see cref="TreeStorageUsageReport.Partial"/>),
    /// the over-threshold gauge is also updated from the freshly sampled
    /// retained bytes. A partial or policy-disabled tree leaves the
    /// over-threshold gauge untouched so it does not publish a misleading zero.
    /// </summary>
    private void PublishToMetrics(TreeStorageUsageReport report, LatticeOptions options)
    {
        metrics.Publish(report);
        if (options.WalMaxRetainedBytes is { } ceiling && ceiling > 0 && !report.Partial)
        {
            metrics.PublishOverThreshold(report.TreeId, report.WalRetainedBytes > ceiling);
        }
    }

    private async Task<TreeStorageUsageReport> BuildReportAsync(CancellationToken cancellationToken)
    {
        // Resolve routing (physical tree ID + shard map) via the public
        // entry point so registry-alias resolution is handled uniformly.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        var routing = await lattice.GetRoutingAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        var physicalShardIndices = routing.Map.GetPhysicalShardIndices();

        // Shard fan-out: leaf-state + snapshot bytes per shard root.
        var shardTasks = new Task<ShardStorageUsage>[physicalShardIndices.Count];
        for (var i = 0; i < physicalShardIndices.Count; i++)
        {
            var shardIndex = physicalShardIndices[i];
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
            shardTasks[i] = GetShardUsageAsync(shard, shardIndex, cancellationToken);
        }

        // WAL fan-out: retained bytes per partition. The WAL grain key is
        // {physicalTreeId}/{partition}; a single partition is the default.
        var walPartitions = await optionsResolver.GetWalPartitionsAsync(routing.PhysicalTreeId);
        var walTasks = new Task<long>[walPartitions];
        for (var partition = 0; partition < walPartitions; partition++)
        {
            var wal = grainFactory.GetGrain<IWalShardGrain>($"{routing.PhysicalTreeId}/{partition}");
            walTasks[partition] = GetWalRetainedBytesAsync(wal, partition, cancellationToken);
        }

        var shardUsages = await Task.WhenAll(shardTasks);
        var walRetained = await Task.WhenAll(walTasks);
        cancellationToken.ThrowIfCancellationRequested();

        long leafStateBytes = 0;
        long snapshotBytes = 0;
        foreach (var usage in shardUsages)
        {
            leafStateBytes += usage.LeafStateBytes;
            snapshotBytes += usage.SnapshotBytes;
        }

        long walRetainedBytes = 0;
        var partial = false;
        foreach (var bytes in walRetained)
        {
            if (bytes < 0)
            {
                // -1 sentinel: the provider does not support byte
                // accounting. The surface contributes 0 and the report is
                // flagged Partial so a consumer renders it as "no data".
                partial = true;
            }
            else
            {
                walRetainedBytes += bytes;
            }
        }

        var total = walRetainedBytes + snapshotBytes + leafStateBytes;

        return new TreeStorageUsageReport
        {
            TreeId = TreeId,
            WalRetainedBytes = walRetainedBytes,
            SnapshotBytes = snapshotBytes,
            LeafStateBytes = leafStateBytes,
            TotalBytes = total,
            Partial = partial,
            SampledAt = DateTimeOffset.UtcNow,
        };
    }

    private async Task<ShardStorageUsage> GetShardUsageAsync(IShardRootGrain shard, int shardIndex, CancellationToken cancellationToken)
    {
        try
        {
            return await shard.GetStorageUsageAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Storage-usage fan-out failed for shard {ShardIndex} in tree {TreeId}", shardIndex, TreeId);
            return default;
        }
    }

    private async Task<long> GetWalRetainedBytesAsync(IWalShardGrain wal, int partition, CancellationToken cancellationToken)
    {
        try
        {
            return await wal.GetRetainedByteSizeAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "WAL retained-byte fan-out failed for partition {Partition} in tree {TreeId}", partition, TreeId);
            // Treat a transient WAL fan-out failure as "no data" rather than
            // a wrong zero so the report is flagged Partial by the caller.
            return -1;
        }
    }
}
