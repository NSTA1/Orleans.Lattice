using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
    ILogger<LatticeWalUsageGrain> logger,
    TimeProvider? timeProvider = null) : ILatticeWalUsage
{
    private string TreeId => context.GrainId.Key.ToString()!;

    private TimeProvider Time => timeProvider ?? TimeProvider.System;

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

    /// <summary>
    /// Each partition's last measured figures, carried over a refused or failed
    /// scan for up to <see cref="WalPartitionUsageCarry.MaxCarryAge"/> (issue
    /// #6017). Activation state only.
    /// </summary>
    private readonly WalPartitionUsageCarry _carry = new();

    /// <summary>Whether this activation has zero-primed the refused-scan counter.</summary>
    private bool _refusalsPrimed;

    /// <inheritdoc />
    public async Task<TreeWalUsageReport> GetWalUsageAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var ttl = optionsResolver.GetConfiguredOptions(TreeId).StorageUsageCacheTtl;
        var now = Time.GetUtcNow();

        // Cache hit: serve the most recent report and re-publish it to
        // the metrics sink so a sibling silo's poller can keep the gauge
        // alive without doing redundant Azure Table work.
        if (_cached is { } cached
            && ttl > TimeSpan.Zero
            && (now - cached.SampledAt) < ttl)
        {
            var resolved = await optionsResolver.ResolveAsync(TreeId);
            PublishToMetrics(cached, resolved);
            return cached;
        }

        // Single-flight: a concurrent caller arriving while a fan-out is
        // already in progress shares the in-flight task rather than
        // launching a parallel one. The fan-out runs under its own budget,
        // never under a caller's token: a caller that cancels (or whose grain
        // call times out) only stops waiting, and the shared fan-out still
        // answers the others. A completed or faulted fan-out is never reused.
        var pending = _inFlight;
        if (pending is null || pending.IsCompleted)
        {
            pending = BuildReportAsync();
            // Every caller may have detached before the shared fan-out faults;
            // observe the fault so it never surfaces as an UnobservedTaskException.
            // The continuation touches no grain state, so it runs off the activation.
            _ = pending.ContinueWith(
                static t => _ = t.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            _inFlight = pending;
        }

        return await pending.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// The wall-clock budget for one fan-out: the lesser of the cluster-wide
    /// <see cref="LatticeOptions.StorageUsageRollupBudget"/> and two thirds of
    /// the silo's Orleans response timeout, or <see langword="null"/> when
    /// neither bounds it. Sized below the response timeout so the fan-out is
    /// cancelled - and its provider scans with it - before the caller's grain
    /// call gives up on it; an abandoned scan otherwise keeps running and the
    /// next poll stacks a fresh fan-out on top of it (issue #6017).
    /// </summary>
    internal static TimeSpan? ComputeFanOutBudget(TimeSpan rollupBudget, TimeSpan responseTimeout)
    {
        TimeSpan? fromResponse = responseTimeout > TimeSpan.Zero && responseTimeout != Timeout.InfiniteTimeSpan
            ? responseTimeout * 2 / 3
            : null;
        TimeSpan? fromRollup = rollupBudget > TimeSpan.Zero ? rollupBudget : null;
        return (fromResponse, fromRollup) switch
        {
            ({ } a, { } b) => a < b ? a : b,
            ({ } a, null) => a,
            (null, { } b) => b,
            _ => null,
        };
    }

    private async Task<TreeWalUsageReport> BuildReportAsync()
    {
        var budget = ComputeFanOutBudget(
            optionsResolver.GetConfiguredOptions(Options.DefaultName).StorageUsageRollupBudget,
            optionsResolver.ResponseTimeout);
        using var budgetSource = budget is { } limit
            ? new CancellationTokenSource(limit, Time)
            : new CancellationTokenSource();
        var budgetToken = budgetSource.Token;

        // Resolve the physical tree id directly via the registry rather
        // than through ILattice.GetRoutingAsync. The public ILattice
        // grain sits in the producer's hot path (every SetAsync /
        // SetManyAsync routes through it), so polling it on the
        // storage-usage cadence forces a sync point on a non-reentrant
        // activation that is otherwise saturated with foreground
        // mutations. The registry is the source of truth for alias
        // resolution and is not in the per-write path.
        var registry = grainFactory.GetLatticeRegistry();
        // Derive initial options from this fresh topology read, retaining
        // lazy seeding and the partition cache used below.
        var (entry, _) = await RegistryEntryOptions.ReadAsync(registry, optionsResolver, TreeId);
        var physicalTreeId = entry?.PhysicalTreeId ?? TreeId;

        var walPartitions = await optionsResolver.GetWalPartitionsAsync(physicalTreeId);
        var pin = await WalPartitionUsageCarry.ReadPinAsync(optionsResolver, TreeId, physicalTreeId, entry);

        var walTasks = new Task<(long Retained, long Physical, bool BudgetExpired)>[walPartitions];
        for (var partition = 0; partition < walPartitions; partition++)
        {
            var wal = grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");
            walTasks[partition] = GetByteSizesAsync(wal, partition, budgetToken);
        }

        var walSizes = await Task.WhenAll(walTasks);

        var sampledAt = Time.GetUtcNow();
        _carry.Begin(WalPartitionUsageCarry.TreeIdentity(physicalTreeId, entry), walPartitions);

        long walRetainedBytes = 0;
        long walPhysicalBytes = 0;
        var partial = false;
        DateTimeOffset? oldestCarried = null;
        var budgetRefusals = 0;
        for (var partition = 0; partition < walSizes.Length; partition++)
        {
            var (measuredRetained, measuredPhysical, budgetExpired) = walSizes[partition];
            if (budgetExpired)
            {
                budgetRefusals++;
            }

            var (retained, physical, carriedFrom) = _carry.Resolve(
                partition,
                WalPartitionUsageCarry.PartitionIdentity(pin, partition),
                measuredRetained,
                measuredPhysical,
                sampledAt);
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

            if (carriedFrom is { } measuredAt && (oldestCarried is null || measuredAt < oldestCarried))
            {
                oldestCarried = measuredAt;
            }
        }

        // Runtime ceilings may change during the fan-out: read them again.
        var options = await optionsResolver.ResolveAsync(TreeId);
        RecordBudgetRefusals(budgetRefusals);

        var report = new TreeWalUsageReport
        {
            TreeId = TreeId,
            WalRetainedBytes = walRetainedBytes,
            WalPhysicalBytes = walPhysicalBytes,
            Partial = partial,
            SampledAt = sampledAt,
            Stale = oldestCarried is not null,
            OldestMeasuredAtUtc = oldestCarried,
        };

        _cached = report;
        PublishToMetrics(report, options);
        return report;
    }

    /// <summary>
    /// Zero-primes the refused-scan counter for this tree on the first report
    /// and counts the partitions whose scan this report's budget cancelled.
    /// Runs after <see cref="LatticeOptionsResolver.ResolveAsync"/>, so the
    /// metric tree id is the cached logical one.
    /// </summary>
    private void RecordBudgetRefusals(int budgetRefusals)
    {
        var metricTreeId = optionsResolver.GetMetricTreeId(TreeId);
        if (!_refusalsPrimed)
        {
            _refusalsPrimed = true;
            LatticeMetrics.PrimeStorageUsageScansRefused(metricTreeId);
        }

        for (var i = 0; i < budgetRefusals; i++)
        {
            LatticeMetrics.RecordStorageUsageScanRefusedBudget(metricTreeId);
        }
    }
    /// <summary>
    /// Publishes the freshly-served WAL report to the observable-gauge sink.
    /// Only the WAL-bytes series and the over-threshold flag are measured
    /// here; leaf-state, snapshot, and total bytes are owned by the deep
    /// path (<see cref="LatticeStorageUsageGrain"/>), and the sink only
    /// carries forward whatever deep figures it already holds for the tree
    /// (<see cref="LatticeStorageUsageMetrics.PublishWal"/>).
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
    /// into the label and grew label cardinality by one value per resize
    /// generation (issue #4152). Keying both by the logical id puts them on one
    /// <c>tree</c> label whose value is identical from either activation, so
    /// where both publish into one sink the duplicate is an idempotent
    /// overwrite rather than a second series. The two activations are placed
    /// independently, though, so on a multi-silo cluster each can export that
    /// label from its own silo; aggregate across silos with
    /// <c>max by (tree)</c>, not <c>sum by (tree)</c>.
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
    /// is exact for a backend whose trim deletes rows outright. A read the
    /// fan-out budget cancelled is also unavailable, and is flagged so the
    /// caller can count it; the cancellation reaches the partition grain and
    /// its provider scan, so the scan stops rather than running on unobserved.
    /// A provider cancelled part-way through a scan throws rather than
    /// returning the part it summed, so a cancelled scan can never surface as
    /// a smaller figure.
    /// </summary>
    private async Task<(long Retained, long Physical, bool BudgetExpired)> GetByteSizesAsync(
        IWalShardGrain wal, int partition, CancellationToken budgetToken)
    {
        try
        {
            var (retained, physical) = await wal.GetByteSizesAsync(budgetToken);
            return (retained, physical < 0 ? retained : physical, false);
        }
        catch (OperationCanceledException) when (budgetToken.IsCancellationRequested)
        {
            logger.LogDebug("WAL byte-size read for partition {Partition} in tree {TreeId} was cancelled by the usage fan-out budget", partition, TreeId);
            return (-1, -1, true);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "WAL byte-size fan-out failed for partition {Partition} in tree {TreeId}", partition, TreeId);
            return (-1, -1, false);
        }
    }
}
