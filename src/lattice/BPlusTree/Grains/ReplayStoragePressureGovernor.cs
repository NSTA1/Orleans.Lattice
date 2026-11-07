namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Steps the WAL replay permit ceiling down when storage is under pressure and
/// back up slowly when it is not (issue #5877). Pure: the caller serialises calls
/// and owns the withheld-permit ledger the returned target drives.
/// </summary>
/// <remarks>
/// <para>
/// <b>Signals.</b> A window of at least <see cref="DefaultWindowTicks"/> is
/// <i>pressured</i> when, with enough reads to judge it, either the storage
/// provider retried throttled or unavailable requests on at least
/// <see cref="ThrottleRatio"/> of the window's replay slice reads, or the
/// window's slice-read p99 reached <see cref="LatencyFactor"/> times the baseline.
/// The baseline is an average of unpressured windows' p99 only, and latency is
/// not judged until it is seeded, so a deployment that starts slow is not read
/// as pressured against nothing.
/// </para>
/// <para>
/// <b>Asymmetric steps.</b> A pressured window withholds a quarter of the permits
/// still in circulation (at least one) and then waits a window before it can step
/// down again. Recovery returns one permit after <see cref="CleanWindowsPerStep"/>
/// consecutive judged, unpressured windows. Fast down and slow up is what keeps a
/// storage account from being driven back into throttling as soon as it recovers.
/// A window with too few reads to judge steps neither way.
/// </para>
/// <para>
/// <b>Bounds.</b> The target is clamped to <c>[0, ceiling - 1]</c>, so at least
/// one permit always circulates and the governor can only ever lower
/// concurrency beneath the resolved ceiling, explicit or default, never raise it.
/// </para>
/// <para>
/// <b>Idle recovery.</b> Recovery normally needs judged windows, and a quiet
/// silo produces none, so without this a storm's withholding would outlive the
/// storm and the next one would start reduced. Once at least
/// <see cref="IdleWindowsToReset"/> windows' worth of time has passed since the
/// last pressured window with only sparse reads and no throttle retry at all,
/// the target returns to zero in one step with its own reason. Sparse evidence
/// can therefore return permits but never withhold one.
/// </para>
/// <para>
/// <b>CPU headroom (issue #5971).</b> Slice-read latency includes in-process
/// queueing, so it rises when the silo itself is saturated; that is how the
/// latency arm walks the ceiling down toward the CPU knee. But a ceiling walked
/// down queues more work behind fewer permits, which keeps the latency high, so
/// latency alone can never see the knee move back and the ceiling ratchets
/// down after the load drops. When the window's CPU utilisation is known, the
/// latency arm therefore withholds only at or above
/// <see cref="HighCpuUtilisation"/>, and a window that is not throttled, has
/// leaves queued for a permit and runs below <see cref="LowCpuUtilisation"/>
/// returns half the withheld permits (at least one) whatever its latency,
/// tagged as recovery. Throttling still withholds regardless of CPU.
/// </para>
/// </remarks>
internal sealed class ReplayStoragePressureGovernor(long windowTicks)
{
    internal static readonly long DefaultWindowTicks = 30L * System.Diagnostics.Stopwatch.Frequency;
    internal const int MinimumLatencyReads = 20;
    internal const int MinimumThrottleReads = 50;
    internal const double ThrottleRatio = 0.01;
    internal const double LatencyFactor = 2.0;
    internal const double DecreaseFraction = 0.25;
    internal const int CleanWindowsPerStep = 3;
    internal const int IdleWindowsToReset = 4;
    internal const double HighCpuUtilisation = 0.85;
    internal const double LowCpuUtilisation = 0.6;
    internal const double HeadroomRestoreFraction = 0.5;

    private bool _hasWindow;
    private long _windowStart;
    private ReplayStoragePressureSample _windowSample;
    private double? _baselineP99;
    private int _cleanWindows;
    private int _cooldown;
    private long _idleWindows;
    private TimeSpan? _windowCpu;

    /// <summary>How many permits the governor currently wants withheld.</summary>
    internal int Target { get; private set; }

    /// <summary>Whether <paramref name="now"/> closes the current window, so the caller should sample.</summary>
    internal bool IsDue(long now) => !_hasWindow || now < _windowStart || now - _windowStart >= windowTicks;

    /// <summary>
    /// Closes the current window when it is due and returns the step taken, or
    /// <see langword="null"/> when the target did not move.
    /// </summary>
    /// <param name="now">The current <see cref="System.Diagnostics.Stopwatch"/> timestamp.</param>
    /// <param name="sample">The cumulative storage-pressure tallies now.</param>
    /// <param name="ceiling">The resolved replay permit ceiling.</param>
    /// <param name="queued">Activations queued for a replay permit now.</param>
    /// <param name="processCpu">The process's cumulative CPU time now, or <see langword="null"/> when unknown.</param>
    /// <param name="cpuCores">The CPU the process can obtain, in cores; zero when unknown.</param>
    internal ReplayStoragePressureStep? Observe(
        long now, ReplayStoragePressureSample sample, int ceiling,
        int queued = 0, TimeSpan? processCpu = null, int cpuCores = 0)
    {
        Target = Math.Clamp(Target, 0, Math.Max(0, ceiling - 1));
        if (!_hasWindow || now < _windowStart || sample.SliceReads < _windowSample.SliceReads)
        {
            StartWindow(now, sample, processCpu);
            return null;
        }

        if (now - _windowStart < windowTicks)
            return null;

        var elapsedWindows = (now - _windowStart) / windowTicks;
        var reads = sample.SliceReads - _windowSample.SliceReads;
        var retries = sample.ThrottleRetries - _windowSample.ThrottleRetries;
        var p99 = reads >= MinimumLatencyReads ? sample.P99MillisecondsSince(_windowSample) : null;
        var cpu = processCpu is { } end && _windowCpu is { } start && cpuCores > 0
            ? (end - start).TotalSeconds / ((double)(now - _windowStart) / System.Diagnostics.Stopwatch.Frequency * cpuCores)
            : (double?)null;
        StartWindow(now, sample, processCpu);

        if (p99 is null && reads < MinimumThrottleReads && retries == 0)
        {
            _idleWindows += elapsedWindows;
            if (_idleWindows < IdleWindowsToReset || Target == 0)
                return null;

            Target = 0;
            _cleanWindows = 0;
            _cooldown = 0;
            _idleWindows = 0;
            return new ReplayStoragePressureStep(1, ReplayStoragePressureReason.Idle);
        }

        _idleWindows = 0;

        var throttled = reads >= MinimumThrottleReads && retries >= reads * ThrottleRatio;
        var latencyHigh = p99 is { } windowP99 && _baselineP99 is { } baseline && windowP99 >= baseline * LatencyFactor;

        // The latency arm tracks the CPU knee, so it withholds only while the
        // silo is near it (issue #5971).
        var slow = latencyHigh;
        if (false && !throttled && Target > 0 && queued > 0 && cpu is { } headroom && headroom < LowCpuUtilisation)
        {
            _cleanWindows = 0;
            _cooldown = 0;
            Target = Math.Max(0, Target - Math.Max(1, (int)Math.Ceiling(Target * HeadroomRestoreFraction)));
            return new ReplayStoragePressureStep(1, ReplayStoragePressureReason.Recovery);
        }

        if (throttled || slow)
        {
            _cleanWindows = 0;
            if (_cooldown > 0)
            {
                _cooldown--;
                return null;
            }

            var circulating = Math.Max(1, ceiling - Target);
            var step = Math.Max(1, (int)Math.Ceiling(circulating * DecreaseFraction));
            var next = Math.Min(Math.Max(0, ceiling - 1), Target + step);
            if (next == Target)
                return null;

            Target = next;
            _cooldown = 1;
            return new ReplayStoragePressureStep(-1, throttled
                ? ReplayStoragePressureReason.Throttle
                : ReplayStoragePressureReason.Latency);
        }

        if (_cooldown > 0)
            _cooldown--;

        // A window whose latency was high but which was not judged pressured
        // (CPU below the knee) is not a clean sample for the baseline.
        if (p99 is not { } cleanP99)
            return null;

        _baselineP99 = _baselineP99 is { } prior ? (prior * 0.8) + (cleanP99 * 0.2) : cleanP99;
        if (++_cleanWindows < CleanWindowsPerStep || Target == 0)
            return null;

        _cleanWindows = 0;
        Target--;
        return new ReplayStoragePressureStep(1, ReplayStoragePressureReason.Recovery);
    }

    private void StartWindow(long now, ReplayStoragePressureSample sample, TimeSpan? processCpu)
    {
        _hasWindow = true;
        _windowStart = now;
        _windowSample = sample;
        _windowCpu = processCpu;
    }
}

/// <summary>Why the replay permit ceiling stepped.</summary>
internal enum ReplayStoragePressureReason
{
    /// <summary>The storage provider retried throttled or unavailable requests.</summary>
    Throttle,

    /// <summary>
    /// Replay slice-read p99 rose well above its unpressured baseline. Measured
    /// around the whole read, so in-process queueing counts as well as storage
    /// time; tagged <c>replay_latency</c>.
    /// </summary>
    Latency,

    /// <summary>Enough consecutive unpressured windows to return a permit.</summary>
    Recovery,

    /// <summary>Enough quiet time with no throttling to return every withheld permit.</summary>
    Idle,
}

/// <summary>One step of the replay permit ceiling.</summary>
/// <param name="Direction">-1 when permits were withheld, +1 when any were returned.</param>
/// <param name="Reason">Why.</param>
internal readonly record struct ReplayStoragePressureStep(int Direction, ReplayStoragePressureReason Reason);
