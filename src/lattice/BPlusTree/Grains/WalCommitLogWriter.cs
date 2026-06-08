using System.Collections.Concurrent;
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
    LatticeOptionsResolver optionsResolver,
    ILatticeMergeModeResolver modeResolver,
    ILatticeOriginClusterIdResolver clusterIdResolver) : ICommitLogWriter
{
    // Per-(tree, partition) pending-append tracker. The append paths
    // create one PendingAppend per dispatch, link it into the partition's
    // chain at Enqueued, mutate Stage at every milestone, and unlink at
    // Acked / Failed. The StallWatchdog walks _trackers from a heap
    // snapshot when the silo wedges so the dominant stuck stage is
    // attributable per partition without a source walk. See the
    // PendingAppend / WalAppendStage docstrings for the full lifecycle.
    //
    // Static so the watchdog has a fixed root to find; the field is
    // never read in production code paths (the per-instance Append paths
    // own the only references). Trackers are stateless wrt drain: drain
    // state lives on the per-instance _drainCts below, not on the
    // shared tracker, so disposing one writer never poisons a tracker
    // that a sibling (or a successor in an in-process test fixture)
    // would resolve from the same map.
    internal static readonly ConcurrentDictionary<(string TreeId, int Partition), PartitionTracker> _trackers
        = new();

    // Per-(tree, shard) cumulative dispatch-timeout trip counter,
    // incremented every time the writer abandons an outbound
    // IWalShardGrain.AppendAsync / AppendBatchAsync await because
    // the per-tree WalAppendDispatchTimeout fired. Read by the silo-
    // scoped saturation sampler that backs IWalSaturationSignal: the
    // sampler subtracts the previous tick's reading to derive the
    // per-window trip delta and uses it to drive the Saturated state
    // when the delta crosses WalSaturationDispatchTimeoutThreshold.
    // Static so the sampler has a fixed root to find (the sampler is
    // a hosted singleton constructed off the same DI container as the
    // writer); zero per-call allocation cost - dispatch-timeout trips
    // are exceptional, not a hot path.
    internal static readonly ConcurrentDictionary<(string TreeId, int Shard), long> _dispatchTimeoutCounts
        = new();

    // Per-instance drain CTS. Each WalCommitLogWriter owns its own
    // token; DrainAsync cancels it; AcquireAsync observes it alongside
    // the caller's CT. Because the token lives on the writer instance
    // (not on the process-wide PartitionTracker), a writer's drain
    // cannot reach traffic dispatched through a peer writer instance
    // - that's the multi-silo correctness property the original
    // tracker-state design quietly broke for in-process test fixtures
    // that share the static _trackers map across successive silo
    // builds.
    //
    // Constructed eagerly so the token is non-default from the first
    // dispatch. Never disposed - the writer is a DI singleton with
    // process lifetime; the CTS is a small heap object the GC reclaims
    // at process exit.
    private readonly CancellationTokenSource _drainCts = new();

    // Writer-level drain flag. Flipped at DrainAsync entry so
    // GetTracker calls during shutdown fast-fail with
    // InvalidOperationException instead of registering fresh dispatches
    // that would race against the drain.
    private volatile bool _isDraining;

    private PartitionTracker GetTracker(string treeId, int partition)
    {
        // Pre-drain gate at the writer level: refuse to route a
        // dispatch through a writer instance that has already begun
        // draining. Reads the writer-local flag, not any tracker state,
        // so successor writers in the same process see a clean gate.
        if (_isDraining)
        {
            throw new InvalidOperationException(
                $"WAL append dispatch to tree '{treeId}' partition {partition} refused: the owning WalCommitLogWriter is shutting down ({nameof(LatticeOptions.WalDrainBudget)}).");
        }
        return _trackers.GetOrAdd((treeId, partition), static key => new PartitionTracker(key.TreeId, key.Partition));
    }

    /// <summary>
    /// Writer-side drain entry: releases every parked admission caller
    /// this writer dispatched through, then returns. Idempotent; the
    /// second call is a no-op.
    /// <para>
    /// Multi-silo correctness: cancels only this writer's per-instance
    /// drain token. Peer silos in the cluster have their own
    /// <see cref="WalCommitLogWriter"/> singleton with its own drain
    /// token, so a drain here does not touch any admission gate on a
    /// peer silo. In-process test fixtures that build successive
    /// silos through the same static <see cref="_trackers"/> map see
    /// the same isolation - drain state lives on the writer instance,
    /// not on the shared tracker, so a drained writer cannot poison a
    /// tracker that a successor writer instance resolves from the
    /// shared map. The downstream
    /// <see cref="IWalShardGrain"/> activations are also untouched -
    /// they continue serving traffic from any peer silo whose writer
    /// has not yet drained.
    /// </para>
    /// <para>
    /// Caller contract: typically invoked by the silo lifecycle's
    /// stop stage via the registered
    /// <c>WalCommitLogWriterDrainer</c> <see cref="Microsoft.Extensions.Hosting.IHostedService"/>;
    /// hosts that want deterministic shutdown can call directly. After
    /// this method returns, every parked
    /// <see cref="PartitionTracker.AcquireAsync"/> caller dispatched
    /// through this writer has surfaced a typed
    /// <see cref="TimeoutException"/> naming
    /// <see cref="LatticeOptions.WalDrainBudget"/> and the writer
    /// refuses new dispatches with <see cref="InvalidOperationException"/>.
    /// In-flight dispatches that successfully acquired a slot before
    /// the drain are not interrupted - they complete or fail through
    /// the existing dispatch-deadline path.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Host-level shutdown token; observed only for the synchronous transition (the drain itself does not block).</param>
    public Task DrainAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _isDraining = true;
        // Cancel asynchronously: CancelAsync (.NET 8+) queues every
        // registered callback's continuation to the threadpool rather
        // than running them synchronously on the canceller's thread.
        // That keeps DrainAsync from observing a synchronous re-entry
        // of AcquireAsync's catch handler, which would tangle the call
        // stack between the drain caller and the parked callers being
        // released. The fire-and-forget shape is safe because every
        // parked caller's continuation is independent (their faulting
        // writes to per-Task state) and DrainAsync has no completion
        // contract beyond "the flag is set and the CTS will cancel".
        try { _ = _drainCts.CancelAsync(); }
        catch (ObjectDisposedException) { /* defensive: nobody disposes _drainCts but cheap to guard */ }
        return Task.CompletedTask;
    }
    /// <inheritdoc />
    public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Grain-context-resuming awaits below: this singleton helper is
        // invoked from a grain turn (BPlusLeafGrain, AtomicWriteGrain, ...).
        // Internal awaits must NOT silently drop the grain context - only
        // the deliberate wedge-attribution outbound shard-RPC awaits do
        // (each annotated inline with why ConfigureAwait(false) is required).
        var (stamped, partition, walPartitions, perTree) = await RouteAsync(entry);
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{stamped.TreeId}/{partition}");

        // Mode-B wedge attribution: record a per-partition pending stamp
        // that the StallWatchdog reads from a heap snapshot when the
        // silo wedges. The stamp's Stage walks Enqueued -> SentToShard
        // -> Acked / Failed, and an out-of-process [wal-append] line
        // names the dominant stuck stage per partition. See
        // WalAppendStage and PendingAppend for the lifecycle details.
        var tracker = GetTracker(stamped.TreeId, partition);
        var treeTagWriter = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, stamped.TreeId);
        var partitionTagWriter = new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition);

        // Writer-side admission: cap PartitionTracker._inFlight at
        // WalMaxPendingBatches so the writer back-pressures honestly
        // when the downstream shard cannot drain. The acquire bound is
        // the same WalAppendDispatchTimeout that bounds the shard RPC
        // below - a single deadline covers admission + dispatch, so a
        // wedged downstream surfaces a typed TimeoutException to the
        // caller in bounded time rather than silently absorbing into
        // an unbounded writer queue.
        double admissionWaitMs;
        try
        {
            admissionWaitMs = await tracker.AcquireAsync(perTree.WalMaxPendingBatches, perTree.WalAppendDispatchTimeout, cancellationToken, _drainCts.Token);
        }
        catch (TimeoutException ex)
        {
            // Distinguish the two TimeoutException sources AcquireAsync
            // can raise: a genuine WalAppendDispatchTimeout deadline
            // trip (the silo is healthy but offered load exceeded the
            // drain rate) versus a drain release (the silo is shutting
            // down and the parked caller was released by the writer-side
            // drain). The drain-release path is its own metric
            // (WalAppendDrainReleases, emitted inside
            // PartitionTracker.AcquireAsync) and must NOT count toward
            // the admission-timeout counter, because operators
            // dashboarding the admission counter want to see
            // "saturated steady-state" not "clean shutdown".
            // The discriminator is the exception message: only the
            // drain-release path names WalDrainBudget.
            var isDrainRelease = ex.Message.Contains(nameof(LatticeOptions.WalDrainBudget), StringComparison.Ordinal);
            if (!isDrainRelease)
            {
                System.Console.WriteLine($"[wal-admission-timeout] tree={stamped.TreeId} partition={partition} entries=1 cap={perTree.WalMaxPendingBatches} timeout={perTree.WalAppendDispatchTimeout}");
                LatticeMetrics.WalAppendAdmissionTimeouts.Add(1, treeTagWriter, partitionTagWriter);
            }
            throw;
        }
        LatticeMetrics.WalAppendAdmissionWait.Record(admissionWaitMs, treeTagWriter, partitionTagWriter);

        var pending = new PendingAppend(stamped.TreeId, partition, entryCount: 1, batchBytes: 0);
        var preDepth = tracker.LinkReturningPreDepth(pending);
        LatticeMetrics.WalAppendDispatched.Add(1, treeTagWriter, partitionTagWriter);
        LatticeMetrics.WalAppendPendingDispatches.Record(preDepth, treeTagWriter, partitionTagWriter);

        // A2 cross-grain dispatch attribution: clock the awaited grain
        // RPC on the caller side so the Orleans turn-queue wait at the
        // target WalShardGrain activation becomes visible. Subtracting
        // WalAppendTurnWait (the WAL grain's own self-clock) from this
        // histogram isolates the scheduling tax on the single WAL
        // activation per partition - the dominant cost under the
        // legacy WalPartitions = 1 shape.
        var dispatchStartTicks = Stopwatch.GetTimestamp();
        try
        {
            // Writer-side dispatch deadline. The outbound
            // IWalShardGrain RPC is the outermost observable seam on the
            // write pipeline; without a writer-side bound a wedged shard
            // activation holds every caller's dispatch parked until the
            // Orleans response timeout (default 3 minutes) expires.
            //
            // The bound is enforced via a deadline-CTS linked to the
            // caller's token, and that linked token is passed INTO the
            // grain RPC (so Orleans' own request-cancellation pipeline
            // observes the deadline) AND observed on the caller's wait
            // (so the wait abandons regardless of whether the callee
            // honours the token). A prior implementation used
            // Task.WaitAsync(TimeSpan) instead, but a 2026-06-03 cohort
            // observed that pattern fail to fire after 116 seconds of
            // parked dispatches against a 30-second deadline (timer
            // thread alive, threadpool idle, vanilla Task<T> source);
            // the linked-CTS shape uses the same threadpool timer queue
            // but exposes cancellation as an OperationCanceledException
            // on a registered callback path that does not depend on
            // WaitAsync(TimeSpan)'s internal timer-task plumbing.
            var dispatchTimeout = perTree.WalAppendDispatchTimeout;
            if (dispatchTimeout == Timeout.InfiniteTimeSpan)
            {
                pending.AdvanceTo(WalAppendStage.SentToShard);
                try
                {
                    // Wedge-attribution exception: route the catch off the
                    // (possibly wedged) caller grain context onto the
                    // threadpool, so the writer-side diagnostic counter and
                    // log line fire even when the grain scheduler is parked.
                    // See WalAppendStage / StallWatchdog for the lifecycle.
                    var offsetInf = await grain.AppendAsync(stamped, cancellationToken).ConfigureAwait(false);
                    pending.AdvanceTo(WalAppendStage.Acked);
                    return offsetInf;
                }
                catch
                {
                    pending.AdvanceTo(WalAppendStage.Failed);
                    throw;
                }
            }
            using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadlineCts.CancelAfter(dispatchTimeout);
            pending.AdvanceTo(WalAppendStage.SentToShard);
            var grainCall = grain.AppendAsync(stamped, deadlineCts.Token);
            try
            {
                // Wedge-attribution exception: same rationale as the
                // infinite-timeout branch above - the catch must land on the
                // threadpool so the dispatch-timeout diagnostic is emitted
                // even when the caller's grain context is wedged.
                var offset = await grainCall.WaitAsync(deadlineCts.Token).ConfigureAwait(false);
                pending.AdvanceTo(WalAppendStage.Acked);
                return offset;
            }
            catch (OperationCanceledException) when (deadlineCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // Empirical diagnostic (paired with the metric below):
                // distinguishes "catch never entered" (line absent from
                // silo log) from "counter silently dropped" (line present,
                // counter still 0). New prefix `-cts` so a single grep
                // separates the Option-B linked-CTS path from the earlier
                // WaitAsync(TimeSpan) path. Loud prefix matches the
                // existing [silo] / [stall-watchdog] log conventions on
                // the azure-throughput silo.
                System.Console.WriteLine($"[wal-dispatch-timeout-cts] tree={stamped.TreeId} shard={partition} entries=1 timeout={dispatchTimeout}");
                LatticeMetrics.WalAppendDispatchTimeouts.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, stamped.TreeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition));
                // Per-(tree, shard) cumulative trip count consumed by
                // the silo-scoped saturation sampler (IWalSaturationSignal).
                // AddOrUpdate runs under the dictionary's internal
                // striped-lock so concurrent dispatches from peer call
                // sites cannot drop an increment.
                _dispatchTimeoutCounts.AddOrUpdate(
                    (stamped.TreeId, partition),
                    static _ => 1L,
                    static (_, prior) => prior + 1L);
                pending.AdvanceTo(WalAppendStage.Failed);
                throw new TimeoutException(
                    $"WAL append dispatch to shard {partition} of tree '{stamped.TreeId}' exceeded the {dispatchTimeout} dispatch deadline ({nameof(LatticeOptions.WalAppendDispatchTimeout)}); the target WalShardGrain activation did not return within the deadline, indicating a wedged shard.");
            }
            catch
            {
                // Any other path that escapes the awaits (cancellation,
                // shard exception) is a Failed terminus from the writer's
                // perspective so the heap-snapshot stamp is correct
                // before the unlink in the finally below.
                pending.AdvanceTo(WalAppendStage.Failed);
                throw;
            }
        }
        finally
        {
            tracker.Unlink(pending);
            tracker.ReleaseAdmission();
            RecordDispatchOutcome(stamped.TreeId, partition, walPartitions, perTree, entryCount: 1, dispatchStartTicks);
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
            var offset = await AppendAsync(entries[0], cancellationToken);
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
        var partitionMeta = new Dictionary<string, (string TreeId, int Partition, int WalPartitions, LatticeOptions PerTree)>(StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var (stamped, partition, walPartitions, perTree) = await RouteAsync(entries[i]);
            var grainKey = $"{stamped.TreeId}/{partition}";
            if (!partitionEntries.TryGetValue(grainKey, out var list))
            {
                list = new List<WalRecord>();
                partitionEntries[grainKey] = list;
                partitionReverse[grainKey] = new List<int>();
                partitionMeta[grainKey] = (stamped.TreeId, partition, walPartitions, perTree);
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
            tasks[t++] = AppendForPartitionAsync(grainKey, grain, list, meta.TreeId, meta.Partition, meta.WalPartitions, meta.PerTree, cancellationToken);
        }
        var partitionResults = await Task.WhenAll(tasks);

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

    private async Task<KeyValuePair<string, IReadOnlyList<long>>> AppendForPartitionAsync(
        string grainKey,
        IWalShardGrain grain,
        IReadOnlyList<WalRecord> entries,
        string treeId,
        int partition,
        int walPartitions,
        LatticeOptions perTree,
        CancellationToken cancellationToken)
    {
        // A2 cross-grain dispatch attribution on the batched path:
        // mirrors the single-entry overload so AppendBatchAsync's
        // per-partition fan-out is attributable too. Each partition
        // gets one observation per AppendManyAsync call.
        var dispatchStartTicks = Stopwatch.GetTimestamp();
        // Mode-B wedge attribution (batched path): mirror the single-
        // entry stamp/unlink so a wedged batched dispatch is visible in
        // the StallWatchdog [wal-append] output too.
        var tracker = GetTracker(treeId, partition);
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var partitionTag = new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition);

        // Writer-side admission (batched path): same shape as the
        // single-entry overload above. The acquire bound is the same
        // WalAppendDispatchTimeout that bounds the shard RPC below.
        double admissionWaitMs;
        try
        {
            admissionWaitMs = await tracker.AcquireAsync(perTree.WalMaxPendingBatches, perTree.WalAppendDispatchTimeout, cancellationToken, _drainCts.Token);
        }
        catch (TimeoutException ex)
        {
            // Same drain-vs-deadline disambiguation as the single-entry
            // overload above: the drain-release path has its own metric
            // (WalAppendDrainReleases) and must not be counted toward
            // WalAppendAdmissionTimeouts.
            var isDrainRelease = ex.Message.Contains(nameof(LatticeOptions.WalDrainBudget), StringComparison.Ordinal);
            if (!isDrainRelease)
            {
                System.Console.WriteLine($"[wal-admission-timeout] tree={treeId} partition={partition} entries={entries.Count} cap={perTree.WalMaxPendingBatches} timeout={perTree.WalAppendDispatchTimeout}");
                LatticeMetrics.WalAppendAdmissionTimeouts.Add(1, treeTag, partitionTag);
            }
            throw;
        }
        LatticeMetrics.WalAppendAdmissionWait.Record(admissionWaitMs, treeTag, partitionTag);

        var pending = new PendingAppend(treeId, partition, entryCount: entries.Count, batchBytes: 0);
        var preDepth = tracker.LinkReturningPreDepth(pending);
        LatticeMetrics.WalAppendDispatched.Add(1, treeTag, partitionTag);
        LatticeMetrics.WalAppendPendingDispatches.Record(preDepth, treeTag, partitionTag);
        try
        {
            // Writer-side dispatch deadline (batched path); see
            // AppendAsync above for the rationale, including why the
            // linked-CTS shape replaces the prior WaitAsync(TimeSpan).
            // Held on the per-tree perTree.WalAppendDispatchTimeout so
            // per-tree overrides apply uniformly to the single-entry
            // and batched dispatches.
            var dispatchTimeout = perTree.WalAppendDispatchTimeout;
            IReadOnlyList<long> offsets;
            if (dispatchTimeout == Timeout.InfiniteTimeSpan)
            {
                pending.AdvanceTo(WalAppendStage.SentToShard);
                try
                {
                    // Wedge-attribution exception (batched path); see
                    // AppendAsync's single-entry branch above for the full
                    // rationale: catch must land off the grain context so the
                    // dispatch-timeout diagnostic still fires under a wedge.
                    offsets = await grain.AppendBatchAsync(entries, cancellationToken).ConfigureAwait(false);
                    pending.AdvanceTo(WalAppendStage.Acked);
                }
                catch
                {
                    pending.AdvanceTo(WalAppendStage.Failed);
                    throw;
                }
            }
            else
            {
                using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                deadlineCts.CancelAfter(dispatchTimeout);
                pending.AdvanceTo(WalAppendStage.SentToShard);
                var grainCall = grain.AppendBatchAsync(entries, deadlineCts.Token);
                try
                {
                    // Wedge-attribution exception (batched path); see the
                    // single-entry WaitAsync site above for the rationale.
                    offsets = await grainCall.WaitAsync(deadlineCts.Token).ConfigureAwait(false);
                    pending.AdvanceTo(WalAppendStage.Acked);
                }
                catch (OperationCanceledException) when (deadlineCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    // Empirical diagnostic: see the single-entry catch above.
                    System.Console.WriteLine($"[wal-dispatch-timeout-cts] tree={treeId} shard={partition} entries={entries.Count} timeout={dispatchTimeout}");
                    LatticeMetrics.WalAppendDispatchTimeouts.Add(
                        1,
                        new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                        new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition));
                    // Per-(tree, shard) cumulative trip count consumed
                    // by the silo-scoped saturation sampler
                    // (IWalSaturationSignal). See the single-entry catch
                    // above for the rationale.
                    _dispatchTimeoutCounts.AddOrUpdate(
                        (treeId, partition),
                        static _ => 1L,
                        static (_, prior) => prior + 1L);
                    pending.AdvanceTo(WalAppendStage.Failed);
                    throw new TimeoutException(
                        $"WAL append-batch dispatch to shard {partition} of tree '{treeId}' ({entries.Count} entries) exceeded the {dispatchTimeout} dispatch deadline ({nameof(LatticeOptions.WalAppendDispatchTimeout)}); the target WalShardGrain activation did not return within the deadline, indicating a wedged shard.");
                }
                catch
                {
                    pending.AdvanceTo(WalAppendStage.Failed);
                    throw;
                }
            }
            return new KeyValuePair<string, IReadOnlyList<long>>(grainKey, offsets);
        }
        finally
        {
            tracker.Unlink(pending);
            tracker.ReleaseAdmission();
            RecordDispatchOutcome(treeId, partition, walPartitions, perTree, entryCount: entries.Count, dispatchStartTicks);
        }
    }

    private static void RecordDispatchOutcome(string treeId, int partition, int walPartitions, LatticeOptions perTree, int entryCount, long startTicks)
    {
        var elapsedMs = Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, partition);
        // walPartitions tag must reflect the tree-registry pinned value
        // (routing-truth) rather than the live IOptionsMonitor value,
        // so the metric attribution matches the writer-side routing
        // shape exactly.
        var walPartitionsTag = new KeyValuePair<string, object?>(LatticeMetrics.TagWalPartitions, walPartitions);
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
    /// <para>
    /// <see cref="LatticeOptions.WalPartitions"/> is sourced from the
    /// tree-registry pin via <see cref="LatticeOptionsResolver"/>, not
    /// from <see cref="IOptionsMonitor{TOptions}"/>, so the writer-side
    /// routing and the activation-time materialiser always agree on
    /// the partition fan-out shape for the lifetime of the tree -
    /// flipping the silo's live <see cref="LatticeOptions.WalPartitions"/>
    /// value after the tree has accepted writes cannot silently
    /// re-route new writes into partitions the materialiser is not
    /// configured to read from.
    /// </para>
    /// </summary>
    private async Task<(WalRecord Entry, int Partition, int WalPartitions, LatticeOptions PerTree)> RouteAsync(WalRecord entry)
    {
        // Resolve WalPartitions through the resolver's per-tree fast-path
        // cache so the foreground commit path does not pay a
        // ILatticeRegistry grain RPC per append. The pin is established
        // at first RegisterAsync and is tree-immutable thereafter, so
        // the cache is correct by construction (see the resolver's
        // GetWalPartitionsAsync docstring); the resolver's
        // GetWalPartitionsAsync returns a synchronously-completed
        // ValueTask on a cache hit and falls back to the registry only
        // on a cold tree's first hit.
        //
        // Other per-tree options not covered by the registry pin
        // (WalMaxPendingBatches and friends used by the dispatch
        // histogram below) are still read from the live IOptionsMonitor
        // here - they are dynamic-tunable by design.
        var partitions = await optionsResolver.GetWalPartitionsAsync(entry.TreeId);
        var perTree = options.Get(entry.TreeId);

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
        return (stamped, partition, partitions, perTree);
    }

    /// <summary>
    /// Lifecycle stage of a single <see cref="PendingAppend"/> stamp.
    /// Mirrors the shape of <c>WalShardGrain.WalFlushStage</c> one layer
    /// up so the same out-of-process <c>StallWatchdog</c> ClrMD walk can
    /// read it as a raw byte and label the stuck stage by name. Stable
    /// ordinal layout - do not renumber.
    /// </summary>
    internal enum WalAppendStage : byte
    {
        /// <summary>Pending stamp linked into the partition tracker; the caller has begun the dispatch but not yet started the shard RPC.</summary>
        Enqueued = 0,

        /// <summary>Reserved for a future batcher loop. The current direct-dispatch implementation skips straight from Enqueued to SentToShard; observing this stage in the watchdog log under the current writer would indicate a future code change introduced a dequeue step.</summary>
        DequeuedForBatch = 1,

        /// <summary>Shard-grain <c>AppendAsync</c> / <c>AppendBatchAsync</c> RPC has been invoked; the await on the grain call is the current parking point if a wedge holds at this stage.</summary>
        SentToShard = 2,

        /// <summary>Shard acked the offsets; the await has returned successfully and the pending stamp is about to be unlinked.</summary>
        Acked = 3,

        /// <summary>Shard threw (including the writer-side dispatch-timeout); the pending stamp is about to be unlinked on the failure path.</summary>
        Failed = 4,
    }

    /// <summary>
    /// Per-dispatch pending-append stamp held in the partition tracker's
    /// chain while the underlying <see cref="IWalShardGrain.AppendAsync"/>
    /// / <see cref="IWalShardGrain.AppendBatchAsync"/> call is in flight.
    /// Stamps are linked at <see cref="WalAppendStage.Enqueued"/>,
    /// mutated at every milestone, and unlinked at
    /// <see cref="WalAppendStage.Acked"/> or
    /// <see cref="WalAppendStage.Failed"/>.
    /// <para>
    /// Field shape is intentionally watchdog-readable: <c>Stage</c> +
    /// <c>StageStartedTicks</c> are plain public fields detected by
    /// field-signature match in <c>StallWatchdog.EmitWalAppendLifecycle</c>
    /// (the 2026-06-03 cohort confirmed literal nested-type-name match
    /// is fragile across ClrMD versions; field signatures are not).
    /// </para>
    /// </summary>
    internal sealed class PendingAppend
    {
        /// <summary>Tree id this dispatch belongs to.</summary>
        public string TreeId;

        /// <summary>Writer partition this dispatch targets.</summary>
        public int Partition;

        /// <summary>Number of WAL entries in this dispatch (1 for the single-entry path).</summary>
        public int EntryCount;

        /// <summary>Approximate byte size of the dispatched batch. The single-entry path leaves this at 0; the batched path may also leave it at 0 since computing entry sizes adds overhead and the watchdog uses EntryCount as the dominant volume signal.</summary>
        public int BatchBytes;

        /// <summary>Current lifecycle stage. Read as a raw byte by <c>StallWatchdog</c>; do not change the field type.</summary>
        public WalAppendStage Stage;

        /// <summary>Stopwatch ticks when <see cref="Stage"/> was last assigned. Read as a raw long by <c>StallWatchdog</c>; do not change the field type.</summary>
        public long StageStartedTicks;

        public PendingAppend(string treeId, int partition, int entryCount, int batchBytes)
        {
            TreeId = treeId;
            Partition = partition;
            EntryCount = entryCount;
            BatchBytes = batchBytes;
            Stage = WalAppendStage.Enqueued;
            StageStartedTicks = Stopwatch.GetTimestamp();
        }

        /// <summary>
        /// Mutates <see cref="Stage"/> and refreshes
        /// <see cref="StageStartedTicks"/>. Field writes are plain (no
        /// volatile / interlocked) because the watchdog walks a heap
        /// snapshot, not the live heap: a torn read picks up either the
        /// old or new stage value but never a frankenstein of both
        /// fields, and either is a valid attribution of the wedge
        /// instant.
        /// </summary>
        public void AdvanceTo(WalAppendStage stage)
        {
            Stage = stage;
            StageStartedTicks = Stopwatch.GetTimestamp();
        }
    }

    /// <summary>
    /// Per-(tree, partition) tracker holding the chain of in-flight
    /// <see cref="PendingAppend"/> stamps for one writer partition. The
    /// chain is a <see cref="LinkedList{T}"/> so the watchdog's
    /// node-by-node walk shape matches the shard-grain tracker exactly
    /// (the same walker helpers apply). The internal lock serialises
    /// link / unlink across concurrent dispatches; metric emission
    /// happens outside the lock to keep the critical section small.
    /// <para>
    /// Also owns the per-partition admission semaphore that caps
    /// <see cref="_inFlight"/> depth at
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/>, mirroring the
    /// shard-side ceiling so the writer back-pressures honestly when
    /// the downstream shard cannot drain. The semaphore is initialised
    /// lazily at first dispatch with the first per-tree options
    /// snapshot the partition observes; per-tree
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> changes after
    /// first activation do not retune the cap (matches the existing
    /// per-tree-immutable convention for the shard-side ceiling and
    /// keeps the cap stable across the lifetime of one
    /// <see cref="PartitionTracker"/> so attribution of admission
    /// timeouts to a single configured cap is unambiguous).
    /// </para>
    /// </summary>
    internal sealed class PartitionTracker
    {
        public readonly string TreeId;
        public readonly int Partition;
        public readonly LinkedList<PendingAppend> _inFlight = new();
        private readonly object _gate = new();
        // Semaphore initialised on first AcquireAsync call; null
        // when the per-tree options resolved at that moment opted
        // out via WalMaxPendingBatches <= 0 (the unbounded shape,
        // for parity with the historical pre-cap writer). Once
        // initialised, the cap is stable for the tracker's lifetime;
        // subsequent option changes do not re-tune the semaphore.
        private SemaphoreSlim? _admission;
        private int _admissionCap;

        public PartitionTracker(string treeId, int partition)
        {
            TreeId = treeId;
            Partition = partition;
        }

        /// <summary>
        /// Acquires a per-partition admission slot, bounding writer-side
        /// pending-dispatch depth at the per-tree
        /// <see cref="LatticeOptions.WalMaxPendingBatches"/> ceiling.
        /// Returns the wall-clock ms spent waiting (zero on the uncontended
        /// fast path; non-zero when the partition was at the cap and the
        /// caller had to wait for a peer dispatch to release its slot).
        /// Throws <see cref="TimeoutException"/> on
        /// <paramref name="timeout"/> expiry; the catch site is responsible
        /// for recording <see cref="LatticeMetrics.WalAppendAdmissionTimeouts"/>
        /// with the appropriate tags before re-throwing.
        /// <para>
        /// Throws <see cref="TimeoutException"/> naming
        /// <see cref="LatticeOptions.WalDrainBudget"/> when the owning
        /// <see cref="WalCommitLogWriter"/>'s drain token cancels while
        /// the caller is parked on the admission semaphore - parked
        /// callers release within bounded time of drain entry with an
        /// attributable exception. The drain token lives on the writer
        /// instance, not on this tracker, so a writer's drain cannot
        /// poison a tracker that a sibling (or successor) writer would
        /// resolve from the same shared map.
        /// </para>
        /// </summary>
        /// <param name="maxPending">Per-partition admission cap; opt-out / unbounded shape when &lt;= 0.</param>
        /// <param name="timeout">Per-call admission deadline; <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> waits indefinitely.</param>
        /// <param name="cancellationToken">Caller-supplied cancellation token (typically <see cref="CancellationToken.None"/> from the foreground commit path).</param>
        /// <param name="drainToken">Writer-supplied drain token; cancelling it surfaces a typed <see cref="TimeoutException"/> naming <see cref="LatticeOptions.WalDrainBudget"/> to every parked caller without mutating any tracker state.</param>
        public async Task<double> AcquireAsync(int maxPending, TimeSpan timeout, CancellationToken cancellationToken, CancellationToken drainToken)
        {
            // Lazy first-use initialisation of the semaphore under the
            // tracker gate so the cap is set at most once even under
            // concurrent first dispatches. WalMaxPendingBatches <= 0 is
            // treated as the opt-out / unbounded shape; the semaphore
            // stays null and every dispatch admits immediately. Drain
            // state is NOT on the tracker - see the writer's _drainCts.
            if (_admission is null && maxPending > 0)
            {
                lock (_gate)
                {
                    if (_admission is null)
                    {
                        _admissionCap = maxPending;
                        _admission = new SemaphoreSlim(initialCount: maxPending, maxCount: maxPending);
                    }
                }
            }
            if (_admission is null)
            {
                return 0d; // opt-out / unbounded path
            }

            // Fast path: if the semaphore is uncontended, WaitAsync
            // completes synchronously and the elapsed measurement is
            // sub-microsecond. Only the contended path pays the await
            // suspension cost.
            //
            // The drain-token must be observed alongside the caller's
            // CT, but only allocate a linked CTS when the caller's token
            // is genuinely cancellable. The dominant hot-path caller is
            // the leaf grain's foreground commit path, which typically
            // passes CancellationToken.None - linking unconditionally
            // would cost one CTS allocation per dispatch on every
            // append. When the caller's token cannot be cancelled, pass
            // drainToken directly; when both tokens are live, build the
            // link and dispose it in the finally.
            var startTicks = Stopwatch.GetTimestamp();
            CancellationTokenSource? linkedCts = null;
            CancellationToken waitToken;
            if (cancellationToken.CanBeCanceled)
            {
                linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, drainToken);
                waitToken = linkedCts.Token;
            }
            else
            {
                waitToken = drainToken;
            }
            try
            {
                bool acquired;
                if (timeout == Timeout.InfiniteTimeSpan)
                {
                    await _admission.WaitAsync(waitToken);
                    acquired = true;
                }
                else
                {
                    acquired = await _admission.WaitAsync(timeout, waitToken);
                }
                if (!acquired)
                {
                    throw new TimeoutException(
                        $"WAL append admission to writer partition {Partition} of tree '{TreeId}' exceeded the {timeout} admission deadline ({nameof(LatticeOptions.WalAppendDispatchTimeout)}); the partition's pending-append tracker was saturated at cap={_admissionCap} ({nameof(LatticeOptions.WalMaxPendingBatches)}) and no slot freed within the deadline, indicating a wedged downstream shard.");
                }
                // Acquired a slot, but a drain that arrived between
                // the WaitAsync returning and us re-checking should
                // still fault the caller rather than letting it dispatch.
                // Release the slot we just took so the writer's drain
                // accounting does not see it as an undrained in-flight.
                if (drainToken.IsCancellationRequested)
                {
                    _admission.Release();
                    LatticeMetrics.WalAppendDrainReleases.Add(
                        1,
                        new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                        new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, Partition));
                    throw new TimeoutException(
                        $"WAL append admission to writer partition {Partition} of tree '{TreeId}' was released by the owning WalCommitLogWriter's drain ({nameof(LatticeOptions.WalDrainBudget)}) before the dispatch could proceed; the silo is shutting down.");
                }
                return Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;
            }
            catch (OperationCanceledException) when (drainToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // The drain signal fired while we were parked on the
                // semaphore. Convert to a typed TimeoutException so the
                // caller's catch site can attribute the trip to the
                // silo-drain path without source-walking, and record a
                // metric sample for dashboard observability.
                LatticeMetrics.WalAppendDrainReleases.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, Partition));
                throw new TimeoutException(
                    $"WAL append admission to writer partition {Partition} of tree '{TreeId}' was released by the owning WalCommitLogWriter's drain ({nameof(LatticeOptions.WalDrainBudget)}) while parked on the admission semaphore; the silo is shutting down.");
            }
            finally
            {
                // Dispose the linked CTS only when we actually allocated
                // one (the cancellable-caller-token branch above). The
                // uncancellable-caller branch passes drainToken directly
                // and has no per-call allocation to release.
                linkedCts?.Dispose();
            }
        }

        /// <summary>
        /// Releases a previously-acquired admission slot. Safe to call
        /// even when the semaphore was opted-out (no-op).
        /// </summary>
        public void ReleaseAdmission()
        {
            _admission?.Release();
        }

        /// <summary>
        /// Returns the pre-link depth (the count callers would observe
        /// in the partition's pending-append histogram for this enqueue)
        /// and links <paramref name="pending"/> at the tail.
        /// </summary>
        public int LinkReturningPreDepth(PendingAppend pending)
        {
            lock (_gate)
            {
                var pre = _inFlight.Count;
                _inFlight.AddLast(pending);
                return pre;
            }
        }

        public void Unlink(PendingAppend pending)
        {
            lock (_gate)
            {
                // LinkedList.Remove(T) is O(n) on the value walker. We
                // ALWAYS hold an exclusive reference to the same instance
                // we linked, so use the reference-based Remove via a
                // cached node would require a refactor; instead, the
                // hot-path cost is bounded by the partition's in-flight
                // depth which the WalMaxPendingBatches ceiling caps at a
                // small value (default 8). For a wedged partition the
                // unlink never runs (the await never returns), so the
                // O(n) cost never compounds.
                _inFlight.Remove(pending);
            }
        }

        /// <summary>
        /// Snapshot read used by the silo-scoped saturation sampler
        /// that backs <see cref="Orleans.Lattice.IWalSaturationSignal"/>.
        /// Returns the partition's current in-flight depth, the
        /// admission cap (<c>0</c> when the semaphore is in the
        /// opt-out / unbounded shape), and a heuristic indicating
        /// whether the admission semaphore has any parked callers
        /// (depth at-or-above cap with cap &gt; 0).
        /// <para>
        /// Reads <see cref="_inFlight"/>.Count under the same lock the
        /// link / unlink paths use so the snapshot cannot tear; the
        /// cap and parked-callers heuristic are derived from the
        /// already-snapshotted depth so the result is internally
        /// consistent. The sampler invokes this every
        /// <see cref="LatticeOptions.WalSaturationSampleInterval"/>
        /// across every live partition tracker, so the snapshot must
        /// be allocation-free; the returned value type satisfies that.
        /// </para>
        /// </summary>
        internal PartitionDepthSnapshot SnapshotDepth()
        {
            int depth;
            lock (_gate)
            {
                depth = _inFlight.Count;
            }
            var cap = _admissionCap;
            // Parked callers heuristic: when the cap is in effect
            // (cap > 0) and the tracker's in-flight depth has reached
            // the cap, AcquireAsync callers parked on the semaphore
            // are blocked waiting for a peer dispatch to release a
            // slot. We do not have an exact wait-queue count from
            // SemaphoreSlim, but the depth-at-cap condition is the
            // condition the public surface AC#3 names for the
            // Saturated state ("semaphore at cap with non-empty wait
            // queue"); the wait-queue side is implied by any caller
            // that arrived after the cap was reached.
            var hasParkedCallers = cap > 0 && depth >= cap;
            return new PartitionDepthSnapshot(TreeId, Partition, depth, cap, hasParkedCallers);
        }
    }

    /// <summary>
    /// Allocation-free snapshot of a single
    /// <see cref="PartitionTracker"/>'s current admission state.
    /// Consumed by the silo-scoped saturation sampler that backs
    /// <see cref="Orleans.Lattice.IWalSaturationSignal"/>.
    /// </summary>
    /// <param name="TreeId">The tree id owning this partition tracker.</param>
    /// <param name="Partition">The partition index within the tree.</param>
    /// <param name="InFlightDepth">Current count of in-flight dispatches linked into the partition's chain.</param>
    /// <param name="AdmissionCap">The per-partition admission cap (the <see cref="LatticeOptions.WalMaxPendingBatches"/> value snapshotted at first use). <c>0</c> when the semaphore is in the opt-out / unbounded shape.</param>
    /// <param name="HasParkedCallers">Heuristic: <c>true</c> when the admission semaphore is at cap and any further <c>AcquireAsync</c> caller is parked.</param>
    internal readonly record struct PartitionDepthSnapshot(
        string TreeId,
        int Partition,
        int InFlightDepth,
        int AdmissionCap,
        bool HasParkedCallers);
}
