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
            var registry = grainFactory.GetLatticeRegistry();
            var entry = await registry.GetEntryAsync(TreeId);
            var physicalTreeId = entry?.PhysicalTreeId ?? TreeId;
            cancellationToken.ThrowIfCancellationRequested();

            var walPartitions = await optionsResolver.GetWalPartitionsAsync(physicalTreeId);
            var walTasks = new Task<(long Retained, long Physical)>[walPartitions];
            for (var partition = 0; partition < walPartitions; partition++)
            {
                var wal = grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");
                walTasks[partition] = GetByteSizesAsync(wal, partition, cancellationToken);
            }

            var walSizes = await Task.WhenAll(walTasks);
            cancellationToken.ThrowIfCancellationRequested();

            long walRetainedBytes = 0;
            long walPhysicalBytes = 0;
            var partial = false;
            foreach (var (retained, physical) in walSizes)
            {
                if (retained < 0)
                {
                    partial = true;
                }
                else
                {
                    walRetainedBytes += retained;
                }

                if (physical >= 0)
                {
                    walPhysicalBytes += physical;
                }
            }

            var options = await optionsResolver.ResolveAsync(TreeId);
            var report = new TreeWalUsageReport
            {
                TreeId = TreeId,
                WalRetainedBytes = walRetainedBytes,
                WalPhysicalBytes = walPhysicalBytes,
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
    /// <para>
    /// The series is keyed by the <i>logical</i> tree
    /// (<see cref="LatticeOptionsResolver.GetMetricTreeId"/>), never by this
    /// activation's own addressed id. The WAL poll fans out to every
    /// <i>registered</i> tree id and a resize registers its
    /// <c>{treeId}/resized/{operationId}</c> copy as a tree in its own right,
    /// so this grain is activated for the copy as well as for the logical tree
    /// it backs. Both activations resolve to - and therefore measure - the same
    /// WAL partitions, so keying by the addressed id published the same bytes
    /// twice under two different <c>tree</c> labels: it leaked the physical id
    /// into the label, grew label cardinality by one value per resize
    /// generation, and broke the poller's documented guarantee that a
    /// cross-silo <c>sum by (tree)</c> counts each tree once (issue #4152).
    /// Keying both by the logical id collapses them onto one series whose value
    /// is identical from either activation, so the duplicate publish is an
    /// idempotent overwrite rather than a second series.
    /// </para>
    /// <para>
    /// The resolve is free here: <see cref="GetWalUsageAsync"/> has already
    /// awaited <see cref="LatticeOptionsResolver.ResolveAsync"/> for this tree
    /// on every path that reaches this method, and that call caches the metric
    /// id, so this is a dictionary hit rather than a registry round trip. The
    /// published <see cref="TreeWalUsageReport"/> the caller receives is left
    /// addressed as it asked for it; only the metric series is re-keyed.
    /// </para>
    /// </summary>
    private void PublishToMetrics(TreeWalUsageReport report, LatticeOptions options)
    {
        var metricTreeId = optionsResolver.GetMetricTreeId(report.TreeId);
        metrics.PublishWal(report with { TreeId = metricTreeId });
        if (options.WalMaxRetainedBytes is { } ceiling && ceiling > 0 && !report.Partial)
        {
            // Physical occupancy, not the logical retained total: the ceiling
            // exists to bound disk and the retained figure omits dead bytes
            // (issue #3107).
            metrics.PublishOverThreshold(metricTreeId, report.WalPhysicalBytes > ceiling);
        }
    }

    /// <summary>
    /// Reads a WAL partition's logical retained total and its physical
    /// occupancy, returning <c>-1</c> for either that is unavailable. Physical
    /// falls back to retained when the provider reports it unsupported, which
    /// is exact for a backend whose trim deletes rows outright.
    /// </summary>
    private async Task<(long Retained, long Physical)> GetByteSizesAsync(
        IWalShardGrain wal, int partition, CancellationToken cancellationToken)
    {
        try
        {
            var physical = await wal.GetPhysicalByteSizeAsync(cancellationToken);
            var retained = await wal.GetRetainedByteSizeAsync(cancellationToken);
            return (retained, physical < 0 ? retained : physical);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "WAL byte-size fan-out failed for partition {Partition} in tree {TreeId}", partition, TreeId);
            return (-1, -1);
        }
    }
}
