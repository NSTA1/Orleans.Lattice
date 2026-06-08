using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
    private bool _priorInitialised;

    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;

    public WalSaturationSampler(
        WalSaturationSignal signal,
        WalSaturationObserverDispatcher dispatcher,
        IOptionsMonitor<LatticeOptions> options,
        ILogger<WalSaturationSampler> logger)
        : this(signal, dispatcher, options, logger, TimeProvider.System)
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
        TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(signal);
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(time);
        _signal = signal;
        _dispatcher = dispatcher;
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
                // Re-read the interval every tick so a per-tree override
                // change takes effect on the next sample boundary.
                var interval = _options.Get(string.Empty).WalSaturationSampleInterval;
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
        var perTree = new Dictionary<string, TreeAccumulator>(StringComparer.Ordinal);
        foreach (var kv in WalCommitLogWriter._trackers)
        {
            var snap = kv.Value.SnapshotDepth();
            if (!perTree.TryGetValue(snap.TreeId, out var acc))
            {
                acc = new TreeAccumulator { TreeId = snap.TreeId };
                perTree[snap.TreeId] = acc;
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
                acc = new TreeAccumulator { TreeId = treeId };
                perTree[treeId] = acc;
            }
            acc.DispatchTimeoutDeltaInWindow += delta;
            acc.AttributedShard ??= shard;
        }
        _priorInitialised = true;

        if (perTree.Count == 0) return;

        var opts = _options.Get(string.Empty);
        var dispatchThreshold = opts.WalSaturationDispatchTimeoutThreshold;
        var throttledRatio = opts.WalSaturationThrottledRatio;
        var observedAt = _time.GetUtcNow();

        foreach (var acc in perTree.Values)
        {
            var newState = Classify(acc, throttledRatio, dispatchThreshold);
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
    /// Throttled which wins over Healthy.
    /// </summary>
    private static WalSaturationState Classify(TreeAccumulator acc, double throttledRatio, int dispatchThreshold)
    {
        // Saturated wins: dispatch-timeout threshold crossed OR
        // semaphore at cap with parked callers.
        if (acc.DispatchTimeoutDeltaInWindow >= dispatchThreshold || acc.HasParkedCallers)
        {
            return WalSaturationState.Saturated;
        }
        // Throttled: depth ratio at-or-above the throttled threshold.
        if (acc.MaxDepthRatio >= throttledRatio)
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
        public int? AttributedPartition;
        public int? AttributedShard;
    }
}
