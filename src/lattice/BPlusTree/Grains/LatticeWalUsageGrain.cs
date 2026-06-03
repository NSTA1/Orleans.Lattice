using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree WAL-only storage-usage aggregator. Cheap counterpart to
/// <see cref="ILatticeStorageUsage"/>: fans out only to this tree's WAL
/// partition grains, so the cluster-wide background poller can refresh the
/// <c>storage.wal_bytes</c> gauge and drive byte-pressure WAL retention
/// without ever activating a leaf, internal node, snapshot storage grain,
/// or shard root. This is the activation-free path that
/// replaces the leaf-walk fan-out that
/// <see cref="LatticeStorageUsageGrain"/> still performs on demand.
/// </summary>
internal sealed class LatticeWalUsageGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver,
    LatticeStorageUsageMetrics metrics,
    ILogger<LatticeWalUsageGrain> logger) : ILatticeWalUsage
{
    private string TreeId => context.GrainId.Key.ToString()!;

    /// <inheritdoc />
    public async Task<TreeWalUsageReport> GetWalUsageAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve routing via the public entry point so registry-alias
        // resolution is handled uniformly. GetRoutingAsync is a single
        // ILattice activation; it does not fan out to shards or leaves.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        var routing = await lattice.GetRoutingAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        var walPartitions = await optionsResolver.GetWalPartitionsAsync(routing.PhysicalTreeId);
        var walTasks = new Task<long>[walPartitions];
        for (var partition = 0; partition < walPartitions; partition++)
        {
            var wal = grainFactory.GetGrain<IWalShardGrain>($"{routing.PhysicalTreeId}/{partition}");
            walTasks[partition] = GetRetainedBytesAsync(wal, partition, cancellationToken);
        }

        var walRetained = await Task.WhenAll(walTasks);
        cancellationToken.ThrowIfCancellationRequested();

        long walRetainedBytes = 0;
        var partial = false;
        foreach (var bytes in walRetained)
        {
            if (bytes < 0)
            {
                partial = true;
            }
            else
            {
                walRetainedBytes += bytes;
            }
        }

        var options = await optionsResolver.ResolveAsync(TreeId);
        var report = new TreeWalUsageReport
        {
            TreeId = TreeId,
            WalRetainedBytes = walRetainedBytes,
            Partial = partial,
            SampledAt = DateTimeOffset.UtcNow,
        };

        PublishToMetrics(report, options);
        return report;
    }

    /// <summary>
    /// Publishes the freshly-served WAL report to the observable-gauge sink.
    /// Only the WAL-bytes series and the over-threshold flag are touched
    /// here; leaf-state, snapshot, and total bytes are owned by the deep
    /// path (<see cref="LatticeStorageUsageGrain"/>) so a sibling silo's
    /// poll cannot accidentally republish a stale leaf/snapshot figure for
    /// the same tree.
    /// </summary>
    private void PublishToMetrics(TreeWalUsageReport report, LatticeOptions options)
    {
        metrics.PublishWal(report);
        if (options.WalMaxRetainedBytes is { } ceiling && ceiling > 0 && !report.Partial)
        {
            metrics.PublishOverThreshold(report.TreeId, report.WalRetainedBytes > ceiling);
        }
    }

    private async Task<long> GetRetainedBytesAsync(IWalShardGrain wal, int partition, CancellationToken cancellationToken)
    {
        try
        {
            return await wal.GetRetainedByteSizeAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "WAL retained-byte fan-out failed for partition {Partition} in tree {TreeId}", partition, TreeId);
            return -1;
        }
    }
}
