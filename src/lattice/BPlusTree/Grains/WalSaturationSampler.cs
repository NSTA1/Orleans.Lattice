using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Silo-scoped hosted service that ticks at
/// <see cref="LatticeOptions.WalSaturationSampleInterval"/>, reads the
/// writer-side admission-gate state and the per-(tree, shard)
/// dispatch-timeout trip deltas from <see cref="WalCommitLogWriter"/>,
/// recomputes the per-tree saturation classification, and writes the
/// result through to <see cref="WalSaturationSignal"/>. Transitions
/// fire the <see cref="LatticeMetrics.WalSaturationTransitions"/>
/// counter and dispatch fan-out through
/// <see cref="WalSaturationObserverDispatcher"/>.
/// <para>
/// Single-threaded by construction: each tick awaits the previous
/// tick's observer fan-out before scheduling the next, so an observer
/// that takes longer than the sample interval back-pressures the
/// sampler rather than overlapping ticks. Cancellation: the loop
/// exits cleanly when <see cref="StopAsync"/> fires.
/// </para>
/// <para>
/// Idle cost: when no partition tracker exists (an idle silo with no
/// tree traffic) the loop's per-tick work is a no-op - the
/// dictionary enumeration is empty and the dispatch-timeout snapshot
/// is empty. When traffic exists, per-tick work is one
/// <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}"/>
/// enumeration plus per-tree state arithmetic, never a grain call.
/// </para>
/// </summary>
internal sealed class WalSaturationSampler : IHostedService, IDisposable
{
    private readonly WalSaturationSignal _signal;
    private readonly WalSaturationObserverDispatcher _dispatcher;
    private readonly IWalCursorRegistry _cursors;
    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly ILogger<WalSaturationSampler> _logger;
    private readonly TimeProvider _time;

    // Per-(tree, shard) prior reading of the cumulative dispatch-
    // timeout trip counter; the per-tick delta is the source signal
    // for the dispatch-timeout half of the Saturated classification.
    // First-tick deltas are skipped (the prior reading initialises to
    // the live value) so a silo that starts after a long-running wedge
    // does not double-count the historical trips it inherited from the
    // static counter.
    private readonly ConcurrentDictionary<(string TreeId, int Shard), long> _priorDispatchTimeoutCounts
        = new();

    // Per-(tree, shard) prior reading of the cumulative provider-
    // failure counter; same per-tick delta-from-prior pattern as the
    // dispatch-timeout counter above. Feeds the third Saturated branch
    // (provider-side commit failure rate, e.g. the Azure-Tables-
    // single-account 409-Conflict burst), which the dispatch-timeout
    // counter cannot reach because terminal provider failures surface
    // well within the dispatch deadline. First-tick deltas are skipped
    // for the same reason (no double-counting of historical failures
    // inherited from the static counter on a late-starting silo).
    private readonly ConcurrentDictionary<(string TreeId, int Shard), long> _priorProviderFailureCounts
        = new();

    // Per-(tree, shard) prior reading of the cumulative flush-latency
    // trip counter; same per-tick delta-from-prior pattern as the
    // dispatch-timeout / provider-failure counters above. Feeds the
    // fourth Saturated branch (sustained slow flushes on small-batch
    // workloads where the indirect HasParkedCallers signal is too
    // thin). First-tick deltas are skipped for the same baseline
    // reason - no double-counting of historical trips inherited from
    // the static counter on a late-starting silo.
    private readonly ConcurrentDictionary<(string TreeId, int Shard), long> _priorFlushLatencyTripCounts
        = new();
    private bool _priorInitialised;

    // Per-tree wall-clock of the most recent tick at which the
    // classifier observed Saturated. The classifier consults this
    // on every tick to upgrade a transient Healthy classification to
    // Throttled while the recovery window is still open, so the
    // advisory regime persists across bursty per-partition WAL drain
    // cycles instead of flapping at the sampler cadence. Single-
    // threaded (sampler thread only); no concurrency primitives
    // needed. Grows monotonically with the set of trees ever observed,
    // matching the bounded cardinality of the underlying
    // WalSaturationSignal._states dictionary.
    private readonly Dictionary<string, DateTimeOffset> _lastSaturatedTickUtc
        = new(StringComparer.Ordinal);

    // Per-tree consecutive-window counter for the flush-latency
    // classifier input. Incremented on every sampler tick
    // whose per-window flush-latency trip delta is non-zero for the
    // tree and reset to zero on every tick whose delta is zero. The
    // classifier escalates the tree to Saturated via the flush-latency
    // branch when the counter reaches
    // LatticeOptions.WalSaturationFlushLatencySampleWindows. Single-
    // threaded (sampler thread only); no concurrency primitives
    // needed. Grows monotonically with the set of trees ever observed
    // with the flush-latency input enabled, matching the bounded
    // cardinality of the underlying WalSaturationSignal._states
    // dictionary.
    private readonly Dictionary<string, int> _consecutiveFlushLatencyWindows
        = new(StringComparer.Ordinal);

    // Per-tree consecutive-window counter for the drain-lag classifier
    // input. Incremented on every sampler tick whose freshly-computed drain-lag
    // is over the threshold and reset to zero on every tick whose fresh lag is
    // at/under it. The classifier holds the tree at Throttled via the drain-lag
    // branch when the counter reaches
    // LatticeOptions.WalSaturationMaterialiserLagSampleWindows. Drain lag is a
    // sustained-pressure signal, so it drives Throttled (a pure back-off) rather
    // than Saturated - it never engages the admission gate's
    // LatticeSaturatedException fast-fail.
    private readonly Dictionary<string, int> _consecutiveMaterialiserDrainLagWindows
        = new(StringComparer.Ordinal);

    // Reusable per-tick accumulator map and a small free-list of accumulator
    // objects. The sampler is single-threaded (each tick awaits the previous
    // tick's fan-out before the next is scheduled), so the map and the pooled
    // accumulators can be reused across ticks instead of allocating a fresh
    // Dictionary plus one TreeAccumulator per active tree on every 200 ms tick.
    // Both are bounded by the per-tick live-tree cardinality. Single-threaded
    // access only - no concurrency primitives needed.
    private readonly Dictionary<string, TreeAccumulator> _perTree
        = new(StringComparer.Ordinal);
    private readonly Stack<TreeAccumulator> _accumulatorPool = new();

    // Rents a zeroed accumulator for the tree from the pool (or allocates one
    // when the pool is empty) and stores it in the reusable per-tick map.
    private TreeAccumulator RentAccumulator(string treeId)
    {
        var acc = _accumulatorPool.Count > 0 ? _accumulatorPool.Pop() : new TreeAccumulator();
        acc.Reset(treeId);
        _perTree[treeId] = acc;
        return acc;
    }

    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;

    public WalSaturationSampler(
        WalSaturationSignal signal,
        WalSaturationObserverDispatcher dispatcher,
        IOptionsMonitor<LatticeOptions> options,
        ILogger<WalSaturationSampler> logger,
        IWalCursorRegistry cursors)
        : this(signal, dispatcher, options, logger, TimeProvider.System, cursors)
    {
    }

    /// <summary>
    /// Test constructor accepting a custom <see cref="TimeProvider"/>
    /// so unit tests can drive the sampler deterministically through a
    /// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>.
    /// </summary>
    internal WalSaturationSampler(
        WalSaturationSignal signal,
        WalSaturationObserverDispatcher dispatcher,
        IOptionsMonitor<LatticeOptions> options,
        ILogger<WalSaturationSampler> logger,
        TimeProvider time,
        IWalCursorRegistry cursors)
    {
        ArgumentNullException.ThrowIfNull(signal);
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(cursors);
        _signal = signal;
        _dispatcher = dispatcher;
        _cursors = cursors;
        _options = options;
        _logger = logger;
        _time = time;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var interval = _options.Get(string.Empty).WalSaturationSampleInterval;
        if (interval == Timeout.InfiniteTimeSpan)
        {
            // Operator explicitly disabled the sampler. Leave the loop
            // dormant - every tree's signal stays Healthy forever, and
            // the polling getters / observable gauge reflect that.
            _logger.LogDebug(
                "WAL saturation sampler disabled via WalSaturationSampleInterval = Infinite; signal pinned to Healthy.");
            return Task.CompletedTask;
        }

        _loopCts = new CancellationTokenSource();
        _loopTask = Task.Run(() => SampleLoopAsync(_loopCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_loopCts is null) return;
        try
        {
            _loopCts.Cancel();
        }
        catch (ObjectDisposedException) { /* defensive */ }
        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { /* host shutdown bound exhausted */ }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "WAL saturation sampler loop faulted during shutdown.");
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _loopCts?.Dispose();
    }

    private async Task SampleLoopAsync(CancellationToken cancellationToken)
    {
        // The sample interval is a silo-global option (read off the empty-key
        // default), so it is hoisted out of the loop and read once: re-reading
        // it on every tick bought nothing but an IOptionsMonitor.Get per tick.
        // A change to the cadence takes effect on the next silo start, matching
        // the Infinite-disables guard already applied once in StartAsync.
        var interval = _options.Get(string.Empty).WalSaturationSampleInterval;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await SampleOnceAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Defensive: the sampler must never crash the silo. Log
                // and continue; the next tick re-reads the signal from
                // scratch.
                _logger.LogWarning(ex, "WAL saturation sampler tick threw; continuing on next tick.");
            }

            try
            {
                if (interval == Timeout.InfiniteTimeSpan)
                {
                    break;
                }
                await Task.Delay(interval, _time, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Performs a single sampler pass. Exposed as <c>internal</c> so
    /// unit tests can drive the sampler one tick at a time without
    /// wiring up the host's <see cref="IHostedService"/> lifecycle.
    /// </summary>
    internal async ValueTask SampleOnceAsync(CancellationToken cancellationToken)
    {
        // Snapshot every live partition tracker's depth + cap once per
        // tick. The tracker map is concurrent and the snapshot is
        // allocation-bounded by the live (tree, partition) cardinality.
        // We aggregate into a small per-tree accumulator to derive each
        // tree's worst-case depth ratio + parked-callers flag.
        // Reclaim the previous tick's accumulators into the free-list and clear
        // the reusable per-tick map. Done at the START of the tick (not in a
        // finally) so every early-return path below leaves the map populated for
        // inspection yet still gets cleaned up before the next tick reuses it.
        // The sampler is single-threaded, so no instance can be in flight here.
        foreach (var pooled in _perTree.Values)
        {
            _accumulatorPool.Push(pooled);
        }
        _perTree.Clear();
        var perTree = _perTree;
        foreach (var kv in WalCommitLogWriter._trackers)
        {
            var snap = kv.Value.SnapshotDepth();
            if (!perTree.TryGetValue(snap.TreeId, out var acc))
            {
                acc = RentAccumulator(snap.TreeId);
            }
            // Ratio is 0 when the semaphore is in the unbounded shape
            // (cap == 0); a tree using opt-out admission cannot be
            // throttled / saturated via the admission path. The
            // dispatch-timeout path (below) still applies.
            double ratio = snap.AdmissionCap > 0
                ? (double)snap.InFlightDepth / snap.AdmissionCap
                : 0.0;
            if (ratio > acc.MaxDepthRatio)
            {
                acc.MaxDepthRatio = ratio;
                acc.AttributedPartition = snap.Partition;
            }
            if (snap.HasParkedCallers)
            {
                acc.HasParkedCallers = true;
                // Record the partition for attribution if we did not
                // already get a partition from the depth-ratio path.
                acc.AttributedPartition ??= snap.Partition;
            }
        }

        // Snapshot the per-(tree, shard) cumulative dispatch-timeout
        // trip counts and compute the per-window delta from the
        // previous reading. First tick initialises the prior baseline
        // without firing any transitions, so a silo that comes up after
        // a long-running wedge does not double-count historical trips.
        foreach (var kv in WalCommitLogWriter._dispatchTimeoutCounts)
        {
            var (treeId, shard) = kv.Key;
            var current = kv.Value;
            var prior = _priorDispatchTimeoutCounts.TryGetValue(kv.Key, out var p) ? p : 0L;
            _priorDispatchTimeoutCounts[kv.Key] = current;
            if (!_priorInitialised) continue;

            var delta = current - prior;
            if (delta <= 0) continue;

            if (!perTree.TryGetValue(treeId, out var acc))
            {
                acc = RentAccumulator(treeId);
            }
            acc.DispatchTimeoutDeltaInWindow += delta;
            acc.AttributedShard ??= shard;
        }

        // Snapshot the per-(tree, shard) cumulative provider-failure
        // counts and compute the per-window delta from the previous
        // reading. Mirrors the dispatch-timeout loop above by
        // construction so a peer call site that increments either
        // counter from a writer broad catch is observed on the next
        // sampler tick with the same first-tick-baseline semantics.
        // Feeds the third Saturated branch in Classify.
        foreach (var kv in WalCommitLogWriter._providerFailureCounts)
        {
            var (treeId, shard) = kv.Key;
            var current = kv.Value;
            var prior = _priorProviderFailureCounts.TryGetValue(kv.Key, out var p) ? p : 0L;
            _priorProviderFailureCounts[kv.Key] = current;
            if (!_priorInitialised) continue;

            var delta = current - prior;
            if (delta <= 0) continue;

            if (!perTree.TryGetValue(treeId, out var acc))
            {
                acc = RentAccumulator(treeId);
            }
            acc.ProviderFailureDeltaInWindow += delta;
            acc.AttributedShard ??= shard;
        }

        // Snapshot the per-(tree, shard) cumulative flush-latency trip
        // counts and compute the per-window delta from the previous
        // reading. Mirrors the dispatch-timeout / provider-failure
        // loops above by construction; the writer-side increment site
        // (WalShardGrain after WalAppendProviderDuration.Record) is
        // gated on LatticeOptions.WalSaturationFlushLatencyThreshold
        // being non-null, so when the input is disabled the map stays
        // empty and this loop is a no-op. Feeds the fourth Saturated
        // branch in Classify via the per-tree consecutive-window
        // counter on _consecutiveFlushLatencyWindows.
        foreach (var kv in WalCommitLogWriter._flushLatencyTripCounts)
        {
            var (treeId, shard) = kv.Key;
            var current = kv.Value;
            var prior = _priorFlushLatencyTripCounts.TryGetValue(kv.Key, out var p) ? p : 0L;
            _priorFlushLatencyTripCounts[kv.Key] = current;
            if (!_priorInitialised) continue;

            var delta = current - prior;
            if (delta <= 0) continue;

            if (!perTree.TryGetValue(treeId, out var acc))
            {
                acc = RentAccumulator(treeId);
            }
            acc.FlushLatencyTripDeltaInWindow += delta;
            acc.AttributedShard ??= shard;
        }

        _priorInitialised = true;

        var opts = _options.Get(string.Empty);
        var dispatchThreshold = opts.WalSaturationDispatchTimeoutThreshold;
        var providerFailureThreshold = opts.WalSaturationProviderFailureRateThreshold;
        var throttledRatio = opts.WalSaturationThrottledRatio;
        var recoveryWindow = opts.WalSaturationRecoveryWindow;
        var flushLatencyEnabled = opts.WalSaturationFlushLatencyThreshold is not null;
        var flushLatencySampleWindows = opts.WalSaturationFlushLatencySampleWindows;
        var drainLagEnabled = opts.WalSaturationMaterialiserLagThreshold is not null;
        var drainLagSampleWindows = opts.WalSaturationMaterialiserLagSampleWindows;
        var drainLagThreshold = opts.WalSaturationMaterialiserLagThreshold;
        var observedAt = _time.GetUtcNow();

        // Drain-lag is computed live every tick from two in-memory sources, so
        // the signal engages immediately on a write spike instead of waiting for
        // a WAL GC pass (which runs only at WalGcInterval - an hour by default -
        // on a non-replicated tree). For each tree that has accepted a write,
        // the lag is the WAL head wall clock (the newest routed entry's HLC,
        // tracked in WalCommitLogWriter._walHeadWallClockTicks) minus the slowest
        // in-memory materialiser cursor (the IWalCursorRegistry min). It is
        // head-relative, so it reads zero once the materialiser catches up -
        // including on a quiescent tree whose head stops advancing while the
        // cursor drains forward - so an idle-but-healthy tree never trips. A
        // null / Zero frontier (no consumer has reported a real checkpoint, or a
        // block pin disabled the cursor branch) is treated as zero lag rather
        // than the absolute head, so a never-checkpointed leaf never pins the
        // regime - exactly as the block-pin contract requires. Every checked
        // tree seeds a perTree accumulator (even at zero lag) so a recovered
        // tree is reclassified back to Healthy even when it has no live tracker.
        //
        // The IWalCursorRegistry is always present: AddLattice registers the
        // in-memory default as a guaranteed fallback (a host that opts into a
        // materialiser/replication stack may replace it via AddWalCursorRegistry,
        // which also wires the leaf cursor reporter). A core-only host with no
        // materialiser reports a null frontier and therefore zero lag - the
        // signal reads correctly idle rather than being silently disabled.
        if (drainLagEnabled && drainLagThreshold is { } lagThreshold)
        {
            foreach (var headEntry in WalCommitLogWriter._walHeadWallClockTicks)
            {
                var treeId = headEntry.Key;
                var headWallTicks = headEntry.Value;

                long lagTicks = 0;
                var frontier = await _cursors
                    .GetMinCursorAsync(treeId, cancellationToken)
                    .ConfigureAwait(false);
                if (frontier is { } cursor && cursor > HybridLogicalClock.Zero
                    && headWallTicks > cursor.WallClockTicks)
                {
                    lagTicks = headWallTicks - cursor.WallClockTicks;
                }

                // Emit the drain-lag gauge for every checked tree, not only the
                // over-threshold ones, so the metric reflects the full lag
                // distribution leading up to a trip.
                LatticeMetrics.MaterialiserDrainLag.Record(
                    TimeSpan.FromTicks(lagTicks).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId));

                if (!perTree.TryGetValue(treeId, out var acc))
                {
                    acc = RentAccumulator(treeId);
                }
                if (lagTicks > lagThreshold.Ticks)
                {
                    acc.MaterialiserDrainLagOverThreshold = true;
                }
            }
        }

        // Reset the per-tree consecutive-window counter for every tree
        // whose flush-latency delta this tick was zero, so the
        // classifier's escalation requires CONSECUTIVE non-zero
        // windows. Iterates a snapshot of the keys so the in-loop
        // mutations are safe; the counter dictionary is bounded by the
        // tree cardinality. Skipped entirely when the flush-latency
        // input is disabled (the dictionary stays empty by
        // construction in that case because the writer-side increment
        // site is also gated).
        if (flushLatencyEnabled && _consecutiveFlushLatencyWindows.Count > 0)
        {
            // Snapshot keys to avoid mutation-during-enumeration.
            var staleKeys = default(List<string>);
            foreach (var treeId in _consecutiveFlushLatencyWindows.Keys)
            {
                if (!perTree.TryGetValue(treeId, out var acc) || acc.FlushLatencyTripDeltaInWindow == 0)
                {
                    staleKeys ??= new List<string>();
                    staleKeys.Add(treeId);
                }
            }
            if (staleKeys is not null)
            {
                foreach (var treeId in staleKeys)
                {
                    _consecutiveFlushLatencyWindows[treeId] = 0;
                }
            }
        }

        // Same stale-reset sweep for the drain-lag consecutive-window counter.
        if (drainLagEnabled && _consecutiveMaterialiserDrainLagWindows.Count > 0)
        {
            var staleKeys = default(List<string>);
            foreach (var treeId in _consecutiveMaterialiserDrainLagWindows.Keys)
            {
                if (!perTree.TryGetValue(treeId, out var acc) || !acc.MaterialiserDrainLagOverThreshold)
                {
                    staleKeys ??= new List<string>();
                    staleKeys.Add(treeId);
                }
            }
            if (staleKeys is not null)
            {
                foreach (var treeId in staleKeys)
                {
                    _consecutiveMaterialiserDrainLagWindows[treeId] = 0;
                }
            }
        }

        if (perTree.Count == 0) return;

        foreach (var acc in perTree.Values)
        {
            // Maintain the per-tree consecutive-window counter for the
            // flush-latency input. Increment on every tick whose per-
            // window flush-latency trip delta is non-zero; the stale-
            // tree reset sweep above already handled trees whose
            // delta was zero this tick (counter reset to 0) or whose
            // delta was missing entirely (no perTree entry). The
            // counter is read by Classify and gates the fourth
            // Saturated branch.
            if (flushLatencyEnabled && acc.FlushLatencyTripDeltaInWindow > 0)
            {
                var prior = _consecutiveFlushLatencyWindows.TryGetValue(acc.TreeId, out var c) ? c : 0;
                _consecutiveFlushLatencyWindows[acc.TreeId] = prior + 1;
            }
            var flushLatencyConsecutiveWindows = flushLatencyEnabled
                && _consecutiveFlushLatencyWindows.TryGetValue(acc.TreeId, out var cw)
                ? cw
                : 0;

            // Maintain the per-tree drain-lag consecutive-window counter. The
            // input is a standing LEVEL: increment on every tick whose fresh
            // observation is over the threshold; the stale-reset sweep above
            // already zeroed trees whose fresh level was at/under the threshold
            // this tick or whose observation went stale.
            if (drainLagEnabled && acc.MaterialiserDrainLagOverThreshold)
            {
                var prior = _consecutiveMaterialiserDrainLagWindows.TryGetValue(acc.TreeId, out var d) ? d : 0;
                _consecutiveMaterialiserDrainLagWindows[acc.TreeId] = prior + 1;
            }
            var drainLagConsecutiveWindows = drainLagEnabled
                && _consecutiveMaterialiserDrainLagWindows.TryGetValue(acc.TreeId, out var dw)
                ? dw
                : 0;

            var newState = Classify(
                acc,
                throttledRatio,
                dispatchThreshold,
                providerFailureThreshold,
                flushLatencyConsecutiveWindows,
                flushLatencyEnabled ? flushLatencySampleWindows : 0,
                drainLagConsecutiveWindows,
                drainLagEnabled ? drainLagSampleWindows : 0);

            // Apply the recovery-window upgrade. When the current-
            // tick classification is Healthy but the tree was observed
            // Saturated within the configured recovery window, hold it
            // at Throttled instead. This defends against the bursty
            // per-partition WAL drain pattern where one partition fills
            // to cap and drains entirely within a sampler period, so
            // the per-tick max(depth_ratio) across partitions
            // oscillates ~1.0 <-> ~0.0 and the classifier would
            // otherwise flap Healthy <-> Saturated at the sampler
            // cadence with Throttled never observed as a stable
            // state.
            //
            // Bookkeeping rules:
            // - Saturated observation: refresh the per-tree timestamp
            //   so the recovery window restarts on every Saturated
            //   tick. This handles the regime where Saturated fires
            //   repeatedly across a long episode.
            // - Throttled observation: do NOT refresh the timestamp.
            //   Throttled is the pure depth-ratio signal; using it to
            //   extend the recovery window would conflate "we've been
            //   above the throttled threshold" with "we've recently
            //   been at cap", losing the distinction the regime
            //   classifies.
            // - Healthy classification + window not yet elapsed:
            //   upgrade to Throttled. The upgrade is silent (no extra
            //   bookkeeping) since it happens deterministically from
            //   the timestamp on every tick.
            // - InfiniteTimeSpan recovery window: once Saturated has
            //   been observed, every subsequent Healthy tick upgrades
            //   to Throttled forever (useful for tests, defensive).
            // - Zero recovery window: the upgrade never fires; the
            //   classifier behaves as the pre-recovery-window shape
            //   did (per-tick classification drives the regime
            //   directly).
            if (newState == WalSaturationState.Saturated)
            {
                _lastSaturatedTickUtc[acc.TreeId] = observedAt;
            }
            else if (newState == WalSaturationState.Healthy
                && recoveryWindow != TimeSpan.Zero
                && _lastSaturatedTickUtc.TryGetValue(acc.TreeId, out var lastSat)
                && (recoveryWindow == Timeout.InfiniteTimeSpan
                    || (observedAt - lastSat) < recoveryWindow))
            {
                newState = WalSaturationState.Throttled;
            }

            var previousState = _signal.UpdateState(acc.TreeId, newState);
            if (previousState == newState) continue;

            // Record the transition counter with the appropriate
            // attribution tags. The state tag value is lowercased to
            // match the WalSaturationSignal gauge's spelling.
            var newStateTag = WalSaturationSignal.StateTagValue(newState);
            var previousStateTag = WalSaturationSignal.StateTagValue(previousState);
            var tags = new List<KeyValuePair<string, object?>>(capacity: 5)
            {
                new(LatticeMetrics.TagTree, acc.TreeId),
                new(LatticeMetrics.TagWalSaturationState, newStateTag),
                new(LatticeMetrics.TagWalSaturationPreviousState, previousStateTag),
            };
            if (acc.AttributedPartition is int p)
            {
                tags.Add(new(LatticeMetrics.TagPartition, p));
            }
            if (acc.AttributedShard is int s)
            {
                tags.Add(new(LatticeMetrics.TagShard, s));
            }
            LatticeMetrics.WalSaturationTransitions.Add(1, tags.ToArray());

            if (_dispatcher.HasObservers)
            {
                var change = new WalSaturationStateChange
                {
                    TreeId = acc.TreeId,
                    PreviousState = previousState,
                    NewState = newState,
                    AttributedPartition = acc.AttributedPartition,
                    AttributedShard = acc.AttributedShard,
                    ObservedAt = observedAt,
                };
                await _dispatcher.PublishAsync(change, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Classifies a tree's per-tick accumulator into the three-state
    /// <see cref="WalSaturationState"/> regime. Saturated wins over
    /// Throttled which wins over Healthy. Saturated is triggered by
    /// any of four independent acute signals (in the order they are
    /// evaluated, but the choice is symmetric):
    /// dispatch-timeout-rate, provider-failure-rate, sustained
    /// flush-latency, or admission semaphore at-cap with parked
    /// callers. The provider-failure-rate trigger is disabled when
    /// <paramref name="providerFailureThreshold"/> is zero (the
    /// documented sentinel); the flush-latency trigger is disabled
    /// when <paramref name="flushLatencySampleWindows"/> is zero (the
    /// internal sentinel used when
    /// <see cref="LatticeOptions.WalSaturationFlushLatencyThreshold"/>
    /// is <c>null</c>).
    /// <para>
    /// The drain-lag input is deliberately NOT a Saturated trigger. A
    /// materialiser falling behind the WAL head is a sustained-pressure
    /// condition the system should ride out by slowing producers, not an
    /// acute storage fault the admission gate should fast-fail. It therefore
    /// drives <see cref="WalSaturationState.Throttled"/> (a pure back-off),
    /// so it never engages the writer admission gate's
    /// <c>LatticeSaturatedException</c> path. The drain-lag trigger is
    /// disabled when <paramref name="drainLagSampleWindows"/> is zero (the
    /// internal sentinel used when
    /// <see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> is
    /// <c>null</c>).
    /// </para>
    /// </summary>
    private static WalSaturationState Classify(
        TreeAccumulator acc,
        double throttledRatio,
        int dispatchThreshold,
        int providerFailureThreshold,
        int flushLatencyConsecutiveWindows,
        int flushLatencySampleWindows,
        int drainLagConsecutiveWindows,
        int drainLagSampleWindows)
    {
        // Saturated wins: dispatch-timeout threshold crossed OR
        // provider-failure threshold crossed (when enabled) OR
        // flush-latency consecutive-window threshold crossed (when
        // enabled) OR semaphore at cap with parked callers.
        if (acc.DispatchTimeoutDeltaInWindow >= dispatchThreshold
            || (providerFailureThreshold > 0 && acc.ProviderFailureDeltaInWindow >= providerFailureThreshold)
            || (flushLatencySampleWindows > 0 && flushLatencyConsecutiveWindows >= flushLatencySampleWindows)
            || acc.HasParkedCallers)
        {
            return WalSaturationState.Saturated;
        }
        // Throttled: depth ratio at-or-above the throttled threshold OR a
        // sustained drain-lag consecutive-window run (when enabled). Drain lag
        // is a back-off, not a fault, so it stops here rather than escalating.
        if (acc.MaxDepthRatio >= throttledRatio
            || (drainLagSampleWindows > 0 && drainLagConsecutiveWindows >= drainLagSampleWindows))
        {
            return WalSaturationState.Throttled;
        }
        return WalSaturationState.Healthy;
    }

    private sealed class TreeAccumulator
    {
        public string TreeId = string.Empty;
        public double MaxDepthRatio;
        public bool HasParkedCallers;
        public long DispatchTimeoutDeltaInWindow;
        public long ProviderFailureDeltaInWindow;
        public long FlushLatencyTripDeltaInWindow;
        public bool MaterialiserDrainLagOverThreshold;
        public int? AttributedPartition;
        public int? AttributedShard;

        // Zeroes every field so a pooled instance carries no state from the
        // tick it was last used on. Must clear ALL fields - a missed field
        // would leak a prior tree's reading into the next tick's classification.
        public void Reset(string treeId)
        {
            TreeId = treeId;
            MaxDepthRatio = 0.0;
            HasParkedCallers = false;
            DispatchTimeoutDeltaInWindow = 0;
            ProviderFailureDeltaInWindow = 0;
            FlushLatencyTripDeltaInWindow = 0;
            MaterialiserDrainLagOverThreshold = false;
            AttributedPartition = null;
            AttributedShard = null;
        }
    }
}
