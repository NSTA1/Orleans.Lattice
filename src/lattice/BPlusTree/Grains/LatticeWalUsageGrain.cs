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

    // Per-activation cached report + single-flight in-flight task. Both are
    // gated by the tree's StorageUsageCacheTtl (default 10s). Concurrent
    // callers within the TTL window see the cached report; concurrent
    // callers that miss the cache share the same in-flight fan-out so the
    // WAL provider's connection pool never carries duplicate
    // GetRetainedByteSizeAsync queries against the same partition during
    // a single poll window. Critical for the Azure Table WAL path where
    // each per-partition query is a manifest scan against the same table
    // the foreground appends hit; uncoalesced poll storms otherwise pile
    // up on the same connection pool and starve foreground ingest.
    private TreeWalUsageReport? _cached;
    private Task<TreeWalUsageReport>? _inFlight;

    /// <inheritdoc />
    public async Task<TreeWalUsageReport> GetWalUsageAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var now = DateTimeOffset.UtcNow;

        // Cache hit: serve the most recent report and re-publish it to
        // the metrics sink so a sibling silo's poller can keep the gauge
        // alive without doing redundant Azure Table work.
        if (_cached is { } cached
            && resolved.StorageUsageCacheTtl > TimeSpan.Zero
            && (now - cached.SampledAt) < resolved.StorageUsageCacheTtl)
        {
            PublishToMetrics(cached, resolved);
            return cached;
        }

        // Single-flight: a concurrent caller arriving while a fan-out is
        // already in progress shares the in-flight task rather than
        // launching a parallel one.
        if (_inFlight is { } pending)
        {
            return await pending;
        }

        _inFlight = BuildReportAsync(cancellationToken);
        return await _inFlight;
    }

    private async Task<TreeWalUsageReport> BuildReportAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Resolve the physical tree id directly via the registry rather
            // than through ILattice.GetRoutingAsync. The public ILattice
            // grain sits in the producer's hot path (every SetAsync /
            // SetManyAsync routes through it), so polling it on the
            // storage-usage cadence forces a sync point on a non-reentrant
            // activation that is otherwise saturated with foreground
            // mutations. The registry is the source of truth for alias
            // resolution and is not in the per-write path.
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var entry = await registry.GetEntryAsync(TreeId);
            var physicalTreeId = entry?.PhysicalTreeId ?? TreeId;
            cancellationToken.ThrowIfCancellationRequested();

            var walPartitions = await optionsResolver.GetWalPartitionsAsync(physicalTreeId);
            var walTasks = new Task<long>[walPartitions];
            for (var partition = 0; partition < walPartitions; partition++)
            {
                var wal = grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");
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

            _cached = report;
            PublishToMetrics(report, options);
            return report;
        }
        finally
        {
            _inFlight = null;
        }
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
