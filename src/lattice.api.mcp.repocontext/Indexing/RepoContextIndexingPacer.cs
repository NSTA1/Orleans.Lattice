using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The silo's shared, resource-adaptive pacer for repository-context indexing
/// (issue #3447). Every embedding batch any repository's pass sends goes through
/// it, so two repositories reconciling at once share one budget instead of each
/// assuming the host is idle.
/// <para>
/// <b>Why it exists.</b> The drain loop used to send fixed-size batches back to
/// back with nothing between them, and its only brake was a three-strike abort.
/// Load therefore swung between idle and flat-out: a pass held the embedder and
/// the vector trees at full load for as long as it lasted, then stopped. The
/// pacer replaces that binary with a controller that reads the signals the host
/// already publishes and adjusts the gap between batches.
/// </para>
/// <para>
/// <b>What it does before each batch</b> (<see cref="PaceAsync"/>), in order:
/// </para>
/// <list type="number">
/// <item>Rests for <see cref="RepoContextIndexingOptions.PacingSliceRest"/> once a
/// work slice of <see cref="RepoContextIndexingOptions.PacingSliceDuration"/> has
/// elapsed - the duty cycle that turns a long back-fill into bounded bursts even
/// on an idle host.</item>
/// <item>Waits, bounded by <see cref="MaxSaturationWait"/>, while a vector tree
/// reports <see cref="WalSaturationState.Saturated"/>.</item>
/// <item>Yields, bounded by <see cref="MaxForegroundYield"/>, while a foreground
/// search or context request is in flight (<see cref="EnterForeground"/>).</item>
/// <item>Sleeps the current inter-batch delay.</item>
/// </list>
/// <para>
/// <b>How the delay moves</b> (<see cref="RecordBatch"/>). A congested batch - one
/// that failed, one slower than <see cref="CongestionRatio"/> times the learned
/// baseline latency, one that finished with the GC at its high-memory-load
/// threshold, or one that finished with a vector tree throttled - doubles the
/// delay, capped at <see cref="RepoContextIndexingOptions.PacingMaxBatchDelay"/>.
/// A clean batch shortens it by a step, so the rate returns to full speed once
/// the pressure clears. The baseline is learned rather than configured because
/// batch latency is a property of the hardware, and the reference container pins
/// <c>DOTNET_PROCESSOR_COUNT</c> above its real CPU count, so no processor-count
/// arithmetic can be trusted to stand in for it.
/// </para>
/// <para>
/// <b>What it never does</b> is change what a pass reports. It changes when a
/// batch runs, never which batches run or how their outcome is classified, so
/// the coverage verdict (issue #3340), the partial-embed-on-fault contract, and
/// the memory arm's orphan sweep are exactly as they were.
/// </para>
/// </summary>
internal sealed class RepoContextIndexingPacer
{
    /// <summary>The delay a first congestion signal raises a zero delay to; later signals double it.</summary>
    internal static readonly TimeSpan InitialBackoffDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>The least a clean batch shortens the delay by. Larger delays shrink by a quarter per clean batch.</summary>
    internal static readonly TimeSpan RecoveryStep = TimeSpan.FromMilliseconds(50);

    /// <summary>How many times the learned baseline a batch must take before its latency counts as congestion.</summary>
    internal const double CongestionRatio = 2.5;

    /// <summary>Latencies at or under this never count as congestion, so a very fast baseline cannot make jitter look like pressure.</summary>
    internal static readonly TimeSpan CongestionFloor = TimeSpan.FromMilliseconds(250);

    /// <summary>How far each clean batch above the baseline pulls it upward, so the baseline follows hardware that genuinely got slower.</summary>
    internal const double BaselineDrift = 0.02;

    /// <summary>The longest the pacer waits on a saturated vector tree before it lets the batch run anyway.</summary>
    internal static readonly TimeSpan MaxSaturationWait = TimeSpan.FromSeconds(30);

    /// <summary>How often a saturation wait re-reads the signal.</summary>
    internal static readonly TimeSpan SaturationPollInterval = TimeSpan.FromMilliseconds(200);

    /// <summary>The longest a single batch yields to foreground requests, so a steady query stream cannot starve indexing.</summary>
    internal static readonly TimeSpan MaxForegroundYield = TimeSpan.FromSeconds(2);

    /// <summary>How often a foreground yield re-checks whether the foreground work has drained.</summary>
    internal static readonly TimeSpan ForegroundPollInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// How long without batch activity before the pacer counts itself idle. Idle
    /// resets the delay and starts a fresh work slice, because the pressure a
    /// previous pass observed was largely that pass's own load.
    /// </summary>
    internal static readonly TimeSpan IdleAfter = TimeSpan.FromMinutes(2);

    private static readonly string[] VectorTrees =
    [
        RepoContextTrees.VectorMembership,
        RepoContextTrees.VectorMetadata,
        RepoContextTrees.VectorPayload,
    ];

    private readonly object _gate = new();
    private readonly RepoContextIndexingOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RepoContextIndexingPacer> _logger;
    private readonly IWalSaturationSignal? _saturation;
    private readonly Func<double> _memoryLoad;

    private TimeSpan _delay;
    private TimeSpan _baseline;
    private long? _lastActivityAt;
    private long? _sliceStartedAt;
    private int _foreground;
    private bool _underPressure;
    private string _congestionReason = string.Empty;
    private RepoIndexPaceState _state = RepoIndexPaceState.Idle;
    private string _reason = "no embedding batch has run on this silo yet";
    private DateTimeOffset? _since;

    /// <summary>Creates the pacer.</summary>
    /// <param name="options">The indexing options carrying the pacing switch and duty cycle. Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock every delay and latency is measured on. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger that records pressure transitions. Must not be <see langword="null"/>.</param>
    /// <param name="saturation">The silo's WAL saturation signal, or <see langword="null"/> in a host that registers none.</param>
    /// <param name="memoryLoad">
    /// Reads the GC memory load as a fraction of the GC's own high-memory-load
    /// threshold (1.0 means at the threshold). <see langword="null"/> reads
    /// <see cref="GC.GetGCMemoryInfo()"/>; tests substitute a fixed value.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/>, <paramref name="timeProvider"/>, or <paramref name="logger"/> is null.</exception>
    public RepoContextIndexingPacer(
        RepoContextIndexingOptions options,
        TimeProvider timeProvider,
        ILogger<RepoContextIndexingPacer> logger,
        IWalSaturationSignal? saturation = null,
        Func<double>? memoryLoad = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
        _saturation = saturation;
        _memoryLoad = memoryLoad ?? ReadGcMemoryLoad;
    }

    /// <summary>Whether pacing is switched on for this host.</summary>
    public bool Enabled => _options.Pacing;

    /// <summary>
    /// Waits until the next embedding batch may run, and returns the timestamp to
    /// hand back to <see cref="RecordBatch"/> once it has. Returns at once when
    /// pacing is switched off.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The batch's start timestamp on the pacer's clock.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled during the wait.</exception>
    public async ValueTask<long> PaceAsync(CancellationToken cancellationToken)
    {
        if (!_options.Pacing)
        {
            return _timeProvider.GetTimestamp();
        }

        try
        {
            // The slice check runs first because it is also where an idle pacer
            // resets its delay; run after the saturation wait, that reset would
            // discard the backoff the wait had just raised.
            await RestIfSliceSpentAsync(cancellationToken).ConfigureAwait(false);
            await WaitOutSaturationAsync(cancellationToken).ConfigureAwait(false);
            await YieldToForegroundAsync(cancellationToken).ConfigureAwait(false);

            TimeSpan delay;
            lock (_gate)
            {
                delay = _delay;
            }

            if (delay > TimeSpan.Zero)
            {
                Transition(RepoIndexPaceState.Backoff, _congestionReason);
                await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                Transition(RepoIndexPaceState.Pacing, "running at the full rate");
            }
        }
        catch (OperationCanceledException)
        {
            // A pass cancelled mid-wait must not leave a waiting, resting, or
            // yielding state behind: nothing would ever transition it out, and
            // index_status would report a wait nobody is in.
            Transition(RepoIndexPaceState.Idle, "the last paced pass was cancelled");
            throw;
        }

        var startedAt = _timeProvider.GetTimestamp();
        lock (_gate)
        {
            _lastActivityAt = startedAt;
        }

        return startedAt;
    }

    /// <summary>
    /// Feeds one batch's outcome into the controller: a congested batch doubles the
    /// inter-batch delay, a clean one shortens it.
    /// </summary>
    /// <param name="startedAt">The timestamp <see cref="PaceAsync"/> returned for this batch.</param>
    /// <param name="succeeded">Whether the batch embedded, stored, and recorded its sources.</param>
    public void RecordBatch(long startedAt, bool succeeded)
    {
        if (!_options.Pacing)
        {
            return;
        }

        var latency = _timeProvider.GetElapsedTime(startedAt);
        var memoryLoad = _memoryLoad();
        var throttledTree = FindTree(WalSaturationState.Throttled);

        RepoIndexPaceState next;
        string reason;
        lock (_gate)
        {
            _lastActivityAt = _timeProvider.GetTimestamp();

            string? cause = null;
            if (!succeeded)
            {
                cause = "an embedding batch failed";
            }
            else if (_baseline > TimeSpan.Zero
                && latency > CongestionFloor
                && latency.Ticks > _baseline.Ticks * CongestionRatio)
            {
                cause = $"batch latency {(long)latency.TotalMilliseconds} ms is over {CongestionRatio}x the "
                    + $"{(long)_baseline.TotalMilliseconds} ms baseline";
            }
            else if (memoryLoad >= 1.0)
            {
                cause = "GC memory load reached its high-load threshold";
            }
            else if (throttledTree is not null)
            {
                cause = $"vector tree '{throttledTree}' is throttled";
            }

            // Measured against the baseline BEFORE the baseline moves, or a slow
            // batch would raise the bar it is being judged against.
            if (succeeded)
            {
                _baseline = _baseline == TimeSpan.Zero || latency < _baseline
                    ? latency
                    : _baseline + TimeSpan.FromTicks((long)((latency - _baseline).Ticks * BaselineDrift));
            }

            if (cause is not null)
            {
                var doubled = _delay + _delay;
                var raised = doubled > InitialBackoffDelay ? doubled : InitialBackoffDelay;
                _delay = raised < _options.PacingMaxBatchDelay ? raised : _options.PacingMaxBatchDelay;
                _congestionReason = cause;
            }
            else
            {
                var quarter = TimeSpan.FromTicks(_delay.Ticks / 4);
                var step = quarter > RecoveryStep ? quarter : RecoveryStep;
                _delay = _delay > step ? _delay - step : TimeSpan.Zero;
            }

            next = _delay > TimeSpan.Zero ? RepoIndexPaceState.Backoff : RepoIndexPaceState.Pacing;
            reason = _delay > TimeSpan.Zero ? _congestionReason : "running at the full rate";
        }

        Transition(next, reason);
    }

    /// <summary>
    /// Marks a foreground request (a search or a context bundle) as in flight until
    /// the returned lease is disposed. While any lease is open the drain loop
    /// yields, bounded, before each batch, and background maintenance defers.
    /// </summary>
    /// <returns>A lease that ends the foreground request when disposed. Disposing it twice is harmless.</returns>
    public IDisposable EnterForeground()
    {
        Interlocked.Increment(ref _foreground);
        return new ForegroundLease(this);
    }

    /// <summary>
    /// Whether background maintenance (an approximate-index build tick, the
    /// coverage-digest audit) should stand aside this time: a foreground request is
    /// in flight, or the drain loop is backing off a congested vector plane. Always
    /// false when pacing is switched off. The caller is responsible for bounding how
    /// many consecutive times it defers, so maintenance is slowed, never starved.
    /// </summary>
    /// <param name="reason">Why maintenance should defer; empty when it should not.</param>
    /// <returns><see langword="true"/> when maintenance should skip this turn.</returns>
    public bool ShouldDeferBackground(out string reason)
    {
        reason = string.Empty;
        if (!_options.Pacing)
        {
            return false;
        }

        var foreground = Volatile.Read(ref _foreground);
        if (foreground > 0)
        {
            reason = $"{foreground} foreground search or context request(s) in flight";
            return true;
        }

        lock (_gate)
        {
            // A batch parked on a saturated tree is live state, so it defers
            // maintenance however long ago the last batch ran; a backed-off delay
            // only counts while a pass is actually running under it.
            var recentlyActive = _lastActivityAt is { } at && _timeProvider.GetElapsedTime(at) < IdleAfter;
            if (_state == RepoIndexPaceState.Waiting || (recentlyActive && _delay > TimeSpan.Zero))
            {
                reason = $"indexing is backing off a congested vector plane ({_congestionReason})";
                return true;
            }
        }

        return false;
    }

    /// <summary>A point-in-time reading of the pacer for <c>index_status</c>.</summary>
    /// <returns>The current reading.</returns>
    public RepoIndexPacing Snapshot()
    {
        if (!_options.Pacing)
        {
            return new RepoIndexPacing
            {
                State = RepoIndexPaceState.Disabled,
                Reason = $"pacing is switched off ({RepoContextIndexingOptions.PacingKey})",
                ForegroundRequests = Volatile.Read(ref _foreground),
            };
        }

        lock (_gate)
        {
            var waiting = _state is RepoIndexPaceState.Waiting
                or RepoIndexPaceState.Yielding
                or RepoIndexPaceState.Resting;
            var idle = !waiting
                && (_lastActivityAt is not { } at || _timeProvider.GetElapsedTime(at) >= IdleAfter);
            return new RepoIndexPacing
            {
                State = idle ? RepoIndexPaceState.Idle : _state,
                Reason = idle ? "no embedding batch has run on this silo recently" : _reason,
                BatchDelayMilliseconds = (long)_delay.TotalMilliseconds,
                Since = idle ? null : _since,
                ForegroundRequests = Volatile.Read(ref _foreground),
            };
        }
    }

    private async ValueTask WaitOutSaturationAsync(CancellationToken cancellationToken)
    {
        var tree = FindTree(WalSaturationState.Saturated);
        if (tree is null)
        {
            return;
        }

        lock (_gate)
        {
            _congestionReason = $"vector tree '{tree}' is saturated";
            if (_delay < InitialBackoffDelay)
            {
                _delay = InitialBackoffDelay;
            }
        }

        Transition(
            RepoIndexPaceState.Waiting,
            $"vector tree '{tree}' is saturated; waiting up to {(long)MaxSaturationWait.TotalSeconds} s for it to recover");

        var startedAt = _timeProvider.GetTimestamp();
        while (tree is not null && _timeProvider.GetElapsedTime(startedAt) < MaxSaturationWait)
        {
            await Task.Delay(SaturationPollInterval, _timeProvider, cancellationToken).ConfigureAwait(false);
            tree = FindTree(WalSaturationState.Saturated);
        }

        if (tree is not null)
        {
            _logger.LogWarning(
                "Repository-context indexing waited {Seconds} s on saturated vector tree '{Tree}' without it "
                + "recovering; letting the next batch run at the backed-off rate rather than wedging the pass.",
                (long)MaxSaturationWait.TotalSeconds,
                tree);
        }
    }

    private async ValueTask YieldToForegroundAsync(CancellationToken cancellationToken)
    {
        var foreground = Volatile.Read(ref _foreground);
        if (foreground <= 0)
        {
            return;
        }

        Transition(
            RepoIndexPaceState.Yielding,
            $"yielding to {foreground} in-flight foreground search or context request(s)");
        var startedAt = _timeProvider.GetTimestamp();
        while (Volatile.Read(ref _foreground) > 0 && _timeProvider.GetElapsedTime(startedAt) < MaxForegroundYield)
        {
            await Task.Delay(ForegroundPollInterval, _timeProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask RestIfSliceSpentAsync(CancellationToken cancellationToken)
    {
        var slice = _options.PacingSliceDuration;
        var rest = _options.PacingSliceRest;
        bool mustRest;
        lock (_gate)
        {
            var now = _timeProvider.GetTimestamp();
            var idle = _lastActivityAt is not { } at || _timeProvider.GetElapsedTime(at) >= IdleAfter;
            if (idle || _sliceStartedAt is null)
            {
                // A fresh burst of work after a quiet spell starts a new slice at the
                // full rate: the pressure the last pass saw was mostly its own load.
                _sliceStartedAt = now;
                if (idle)
                {
                    _delay = TimeSpan.Zero;
                }

                return;
            }

            mustRest = slice > TimeSpan.Zero
                && rest > TimeSpan.Zero
                && _timeProvider.GetElapsedTime(_sliceStartedAt.Value) >= slice;
        }

        if (!mustRest)
        {
            return;
        }

        Transition(
            RepoIndexPaceState.Resting,
            $"resting {rest.TotalSeconds:0.#} s after a {slice.TotalSeconds:0.#} s work slice");
        await Task.Delay(rest, _timeProvider, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            _sliceStartedAt = _timeProvider.GetTimestamp();
        }
    }

    private void Transition(RepoIndexPaceState state, string reason)
    {
        bool changed;
        bool pressureChanged;
        bool underPressure;
        lock (_gate)
        {
            changed = _state != state;
            _state = state;
            _reason = reason;
            if (changed)
            {
                _since = _timeProvider.GetUtcNow();
            }

            underPressure = _delay > TimeSpan.Zero || state == RepoIndexPaceState.Waiting;
            pressureChanged = underPressure != _underPressure;
            _underPressure = underPressure;
        }

        // Pressure flips are the operator-facing transitions and are logged at
        // Information; the routine rest/yield/pace cycling that happens inside a
        // healthy pass is Debug, or a long back-fill would log a line per slice.
        if (pressureChanged)
        {
            if (underPressure)
            {
                _logger.LogInformation(
                    "Repository-context indexing is backing off: {Reason}. The inter-batch delay rises until "
                    + "batches run clean again; index_status reports the pacer as {State}.",
                    reason,
                    state);
            }
            else
            {
                _logger.LogInformation(
                    "Repository-context indexing recovered: the inter-batch delay is back to zero and batches "
                    + "run at the full rate.");
            }
        }
        else if (changed)
        {
            _logger.LogDebug("Repository-context indexing pacer is {State}: {Reason}.", state, reason);
        }
    }

    private string? FindTree(WalSaturationState state) => FindVectorTree(_saturation, state);

    /// <summary>
    /// Reads the platform's own verdict on the vector plane: the first vector tree
    /// (membership, metadata, or payload) whose <see cref="IWalSaturationSignal"/>
    /// state is at or above <paramref name="atLeast"/>. This is the one place the
    /// indexing path names the trees it writes, so the pacer and the ingestor
    /// consult the same set rather than each keeping a list that can drift
    /// (issue #2683).
    /// </summary>
    /// <param name="saturation">The silo's WAL saturation signal, or <see langword="null"/> in a host that registers none.</param>
    /// <param name="atLeast">The least severe state that counts as a match.</param>
    /// <returns>The first matching vector tree id, or <see langword="null"/> when none matches or no signal is registered.</returns>
    internal static string? FindVectorTree(IWalSaturationSignal? saturation, WalSaturationState atLeast)
    {
        if (saturation is null)
        {
            return null;
        }

        foreach (var tree in VectorTrees)
        {
            if (saturation.GetCurrentState(tree) >= atLeast)
            {
                return tree;
            }
        }

        return null;
    }

    private static double ReadGcMemoryLoad()
    {
        var info = GC.GetGCMemoryInfo();
        return info.HighMemoryLoadThresholdBytes > 0
            ? (double)info.MemoryLoadBytes / info.HighMemoryLoadThresholdBytes
            : 0d;
    }

    private void ExitForeground() => Interlocked.Decrement(ref _foreground);

    /// <summary>Ends one foreground request exactly once, however many times it is disposed.</summary>
    private sealed class ForegroundLease(RepoContextIndexingPacer pacer) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                pacer.ExitForeground();
            }
        }
    }
}
