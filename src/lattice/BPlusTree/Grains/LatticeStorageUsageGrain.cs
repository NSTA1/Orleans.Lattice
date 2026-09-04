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
    LatticeAdmissionMetrics admissionMetrics,
    ILogger<LatticeStorageUsageGrain> logger) : ILatticeStorageUsage
{
    private string TreeId => context.GrainId.Key.ToString()!;

    private TreeStorageUsageReport? _cached;

    /// <summary>
    /// Whether the WAL surface of <see cref="_cached"/> was fully accounted.
    /// Tracked separately from <see cref="TreeStorageUsageReport.Partial"/>
    /// because <c>Partial</c> now also covers a failed shard fan-out, whereas
    /// the byte-pressure over-threshold gauge is a statement about retained
    /// <i>WAL</i> bytes alone and must not be silenced by an unrelated shard
    /// failure.
    /// </summary>
    private bool _cachedWalComplete;

    /// <inheritdoc />
    public async Task<TreeStorageUsageReport> GetReportAsync(bool forceRefresh, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var options = await optionsResolver.ResolveAsync(TreeId);
        var ttl = options.StorageUsageCacheTtl;
        var now = DateTimeOffset.UtcNow;

        if (!forceRefresh && _cached is { } c && ttl > TimeSpan.Zero && (now - c.SampledAt) < ttl)
        {
            PublishToMetrics(c, options, _cachedWalComplete);
            return c;
        }

        var (report, walComplete) = await BuildReportAsync(forceRefresh, options, cancellationToken);
        _cached = report;
        _cachedWalComplete = walComplete;
        PublishToMetrics(report, options, walComplete);
        return report;
    }

    /// <summary>
    /// Pushes the freshly served report to the observable-gauge sink so a
    /// subsequent meter scrape reflects it without re-fanning out. When the
    /// byte-pressure policy is enabled for the tree
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/> set and positive) and
    /// every WAL partition reported its retained bytes
    /// (<paramref name="walComplete"/>), the over-threshold gauge is also
    /// updated from the freshly sampled retained bytes. A tree whose WAL
    /// surface is unsupported or whose WAL fan-out failed leaves the
    /// over-threshold gauge untouched so it does not publish a misleading
    /// zero. A <i>shard</i> fan-out failure also flags the report
    /// <see cref="TreeStorageUsageReport.Partial"/> but says nothing about
    /// retained WAL bytes, so it deliberately does not suppress this gauge.
    /// </summary>
    private void PublishToMetrics(TreeStorageUsageReport report, LatticeOptions options, bool walComplete)
    {
        metrics.Publish(report);
        if (options.WalMaxRetainedBytes is { } ceiling && ceiling > 0 && walComplete)
        {
            metrics.PublishOverThreshold(report.TreeId, report.WalRetainedBytes > ceiling);
        }

        // Publish the per-tree admission aggregate so the observable admission
        // gauges reflect it on the next scrape and so the LatticeGrain write
        // guard can read the current live-key / estimated-byte figure in O(1)
        // without fanning out. Estimated bytes aliases the total retained-byte
        // figure; a byte surface that reported "unsupported" (Partial) is passed
        // through unchanged (best-effort). The resolved caps ride along so the
        // sink can compute the over-advisory and utilisation gauges at scrape
        // time from a single published record.
        admissionMetrics.Publish(new AdmissionUsageSample
        {
            TreeId = report.TreeId,
            LiveKeys = report.LiveKeys,
            EstimatedBytes = report.TotalBytes,
            MaxLiveKeys = options.MaxLiveKeys,
            MaxEstimatedBytes = options.MaxEstimatedBytes,
            AdvisoryLiveKeys = options.AdmissionAdvisoryLiveKeys,
            AdvisoryBytes = options.AdmissionAdvisoryBytes,
        });
    }

    /// <summary>
    /// Fans out to every physical shard root and WAL partition and assembles the
    /// report. Returns the report alongside whether the WAL surface was fully
    /// accounted (see <see cref="_cachedWalComplete"/>).
    /// <para>
    /// Both surface kinds share one <see cref="BoundedFanOut"/> gate sized by
    /// <see cref="LatticeOptions.MaxConcurrentStorageUsageSurfaces"/>, so a wide
    /// tree never dispatches all of its shard roots at once, and the cluster
    /// roll-up's own per-tree bound multiplies against a bounded - rather than
    /// unbounded - inner level. Running both kinds under a single fan-out also
    /// means every dispatched call is settled by one
    /// <see cref="Task.WhenAll(Task[])"/>: the previous shape awaited the shard
    /// batch and the WAL batch in sequence, so a throw from the first abandoned
    /// the second batch's tasks unobserved.
    /// </para>
    /// </summary>
    private async Task<(TreeStorageUsageReport Report, bool WalComplete)> BuildReportAsync(
        bool forceRefresh, LatticeOptions options, CancellationToken cancellationToken)
    {
        // Resolve routing (physical tree ID + shard map) via the public
        // entry point so registry-alias resolution is handled uniformly.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        var routing = await lattice.GetRoutingAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        var physicalShardIndices = routing.Map.GetPhysicalShardIndices();
        var shardCount = physicalShardIndices.Count;

        // The WAL grain key is {physicalTreeId}/{partition}; a single partition
        // is the default.
        var walPartitions = await optionsResolver.GetWalPartitionsAsync(routing.PhysicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        // A null slot means "this shard did not answer" - distinct from a shard
        // that genuinely holds nothing, which answers with zeroes.
        var shardUsages = new ShardStorageUsage?[shardCount];

        // Seeded with the -1 "did not answer" sentinel rather than left at the
        // default 0, which would read as a genuine "zero bytes retained". The
        // fan-out only returns normally once every slot has settled, so today a
        // partly-filled array is never read; seeding makes that safety a
        // property of the array itself rather than of the caller's control
        // flow, matching how the nullable shard slots above fail safe.
        var walRetained = new long[walPartitions];
        Array.Fill(walRetained, -1L);

        // Slots [0, shardCount) are shard roots; the remainder are WAL
        // partitions. One gate covers both so the per-tree ceiling is a ceiling
        // on outstanding usage reads of any kind.
        await BoundedFanOut.RunAsync(
            shardCount + walPartitions,
            options.MaxConcurrentStorageUsageSurfaces,
            async slot =>
            {
                if (slot < shardCount)
                {
                    var shardIndex = physicalShardIndices[slot];
                    var shard = grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
                    shardUsages[slot] = await GetShardUsageAsync(shard, shardIndex, forceRefresh, cancellationToken);
                }
                else
                {
                    var partition = slot - shardCount;
                    var wal = grainFactory.GetGrain<IWalShardGrain>($"{routing.PhysicalTreeId}/{partition}");
                    walRetained[partition] = await GetWalRetainedBytesAsync(wal, partition, cancellationToken);
                }
            },
            cancellationToken);

        cancellationToken.ThrowIfCancellationRequested();

        long leafStateBytes = 0;
        long snapshotBytes = 0;
        long liveKeys = 0;
        var shardsComplete = true;
        foreach (var usage in shardUsages)
        {
            if (usage is not { } u)
            {
                // The shard did not answer. Contributing its zeroes would
                // understate the tree's real footprint while still presenting
                // the report as complete - a silently wrong answer, which is
                // worse than a flagged one. Contribute nothing and flag the
                // report Partial instead, exactly as the WAL surface does.
                shardsComplete = false;
                continue;
            }

            leafStateBytes += u.LeafStateBytes;
            snapshotBytes += u.SnapshotBytes;
            liveKeys += u.LiveKeys;
        }

        long walRetainedBytes = 0;
        var walComplete = true;
        foreach (var bytes in walRetained)
        {
            if (bytes < 0)
            {
                // -1 sentinel: the provider does not support byte
                // accounting, or the fan-out to it failed. The surface
                // contributes 0 and the report is flagged Partial so a
                // consumer renders it as "no data".
                walComplete = false;
            }
            else
            {
                walRetainedBytes += bytes;
            }
        }

        var total = walRetainedBytes + snapshotBytes + leafStateBytes;

        var report = new TreeStorageUsageReport
        {
            TreeId = TreeId,
            WalRetainedBytes = walRetainedBytes,
            SnapshotBytes = snapshotBytes,
            LeafStateBytes = leafStateBytes,
            TotalBytes = total,
            Partial = !walComplete || !shardsComplete,
            SampledAt = DateTimeOffset.UtcNow,
            LiveKeys = liveKeys,
        };

        return (report, walComplete);
    }

    /// <summary>
    /// Reads one shard root's byte roll-up. Returns <see langword="null"/> when
    /// the shard did not answer, which the caller turns into
    /// <see cref="TreeStorageUsageReport.Partial"/> rather than a silent zero
    /// contribution. One bad shard still never aborts the whole tree.
    /// </summary>
    private async Task<ShardStorageUsage?> GetShardUsageAsync(IShardRootGrain shard, int shardIndex, bool forceRefresh, CancellationToken cancellationToken)
    {
        try
        {
            return forceRefresh
                ? await RefreshShardUsageAsync(shard, cancellationToken)
                : await shard.GetStorageUsageAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The caller asked to stop. Propagate so the fan-out aborts promptly
            // and no report is assembled at all, rather than returning a total
            // that silently omits every shard cancellation raced past.
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Storage-usage fan-out failed for shard {ShardIndex} in tree {TreeId}", shardIndex, TreeId);
            return null;
        }
    }

    /// <summary>
    /// Drives one shard's work-bounded re-anchor batches to completion.
    /// <para>
    /// Each batch sums a bounded number of leaves and then releases the shard,
    /// so an operator-forced refresh no longer holds it for the length of the
    /// whole leaf chain (issue 1972). The running total is threaded back into
    /// each call so the shard can re-anchor its activation-scoped totals from
    /// the whole-chain figure on the final batch - never from a partial sum,
    /// which would leave it under-reporting its own footprint mid-walk.
    /// </para>
    /// </summary>
    private static async Task<ShardStorageUsage> RefreshShardUsageAsync(
        IShardRootGrain shard, CancellationToken cancellationToken)
    {
        var total = default(ShardStorageUsage);
        string? cursor = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await shard.RefreshLeafByteFootprintsBoundedAsync(cursor, total, cancellationToken);
            total = new ShardStorageUsage
            {
                LeafStateBytes = total.LeafStateBytes + page.Usage.LeafStateBytes,
                SnapshotBytes = total.SnapshotBytes + page.Usage.SnapshotBytes,
                LiveKeys = total.LiveKeys + page.Usage.LiveKeys,
            };

            if (page.ResumeFromInclusive is not { } next) return total;
            cursor = next;
        }
    }

    private async Task<long> GetWalRetainedBytesAsync(IWalShardGrain wal, int partition, CancellationToken cancellationToken)
    {
        try
        {
            return await wal.GetRetainedByteSizeAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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
