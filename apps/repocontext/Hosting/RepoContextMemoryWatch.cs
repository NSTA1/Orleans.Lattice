using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// One reading of what this run has actually needed so far: the ceiling it is held
/// to, the highest commitment it has reached against that ceiling, and how many
/// times it has run out.
/// </summary>
/// <param name="LimitBytes">The managed-heap ceiling most recently observed.</param>
/// <param name="PeakCommittedBytes">The highest commitment observed so far this run.</param>
/// <param name="ExhaustionEvents">Managed out-of-memory exceptions observed so far this run.</param>
public readonly record struct RepoContextMemorySample(
    long LimitBytes,
    long PeakCommittedBytes,
    long ExhaustionEvents)
{
    /// <summary>
    /// The share of the ceiling consumed at the peak, or <see langword="null"/> when
    /// no usable ceiling has been observed.
    /// </summary>
    public double? PeakOccupancyRatio => LimitBytes > 0
        ? (double)PeakCommittedBytes / LimitBytes
        : null;
}

/// <summary>
/// Measures what this corpus actually requires on this host - the peak commitment
/// reached and the margin consumed against the granted ceiling - publishes it while
/// the process lives, and records it where the next process will find it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Issue #3255, item 3: measure, do not predict.</b> The container's memory grant
/// is currently a deploy-time absolute produced by fitting a corpus model on one
/// host, and nothing afterwards ever checks it against what the corpus turned out to
/// need. This type supplies the missing half. It adds no model and no constant: it
/// watches two numbers the runtime already reports and remembers the worst pair it
/// saw.
/// </para>
/// <para>
/// <b>Why a first-chance handler, and why the obvious alternative does not work.</b>
/// The exhaustion this must detect is <i>caught</i>. The documented failure is a wave
/// of <see cref="OutOfMemoryException"/> inside a grain-state read, which Orleans
/// catches and reports as a STORAGE fault - so the process never terminates on it and
/// <see cref="AppDomain.UnhandledException"/> never fires. The 129 and 304 exception
/// waves recorded on the 12 GiB run were all handled. A first-chance handler is the
/// only seam that observes an exception the runtime is about to let somebody catch.
/// </para>
/// <para>
/// <b>The handler is deliberately three instructions and cannot allocate.</b> It runs
/// while the process is out of memory, so allocating in it risks a second
/// <see cref="OutOfMemoryException"/> inside the handler for the first one. It does a
/// type test and an <see cref="Interlocked.Increment(ref long)"/>; everything else -
/// deciding, formatting, writing - happens later on the sampling timer, where
/// allocation is safe. The cost when nothing is wrong is one type test per thrown
/// exception process-wide, which is the price of seeing a caught failure at all.
/// </para>
/// <para>
/// <b>The peak is sampled, so it is a floor on the true peak.</b> A spike falling
/// entirely between two samples is not seen. It therefore errs low, which is the safe
/// direction for every consumer here: the admission check only escalates on it, so
/// under-reporting costs a missed warning rather than a wrong refusal.
/// </para>
/// <para>
/// <b>Recording is best-effort and says so.</b> The exhaustion record is written by
/// the sampling timer rather than by the handler, so a process that dies within one
/// sample period of its first out-of-memory exception records nothing. In the
/// observed failure - waves of 129 and 304 exceptions over minutes - the timer fires
/// many times over, but the gap is real and is not papered over. A write that fails
/// is logged and never thrown, because failing to record a diagnostic must not become
/// a failure to run.
/// </para>
/// </remarks>
public sealed class RepoContextMemoryWatch : IHostedService, IDisposable
{
    /// <summary>
    /// How often the ceiling and commitment are sampled.
    /// </summary>
    /// <remarks>
    /// Fifteen seconds is short enough that a crash-loop measured in minutes records
    /// its exhaustion many times over, and long enough that the cost is irrelevant:
    /// one <see cref="GC.GetGCMemoryInfo(GCKind)"/> call, which reads figures the
    /// collector already maintains and does not itself collect.
    /// </remarks>
    public static readonly TimeSpan SamplePeriod = TimeSpan.FromSeconds(15);

    private readonly ILogger<RepoContextMemoryWatch> _logger;
    private readonly Func<RepoContextHeapCeiling> _read;
    private readonly Func<DateTimeOffset> _clock;
    private readonly Func<RepoContextMemoryObservation, bool> _write;
    private readonly RepoContextMemoryObservation? _previous;
    private readonly RepoContextMemoryAdmissionDecision _decision;
    private readonly TimeSpan _samplePeriod;
    private readonly Lock _writeGate = new();

    private long _limitBytes;
    private long _peakCommittedBytes;
    private long _exhaustionEvents;
    private long _lastRecordedPeak = -1;
    private bool _exhaustionRecorded;
    private bool _subscribed;
    private Timer? _timer;

    /// <summary>Initializes the watch.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="decision">
    /// The admission decision taken before the host was built, reported here because
    /// this is the first point at which a real logger exists.
    /// </param>
    /// <param name="previous">The last recorded run, or <see langword="null"/>.</param>
    /// <param name="write">Records an observation; returns whether it landed.</param>
    /// <param name="read">
    /// Reads the current ceiling figures. Defaults to the runtime; a test substitutes it.
    /// </param>
    /// <param name="clock">The clock. Defaults to <see cref="DateTimeOffset.UtcNow"/>.</param>
    /// <param name="samplePeriod">The sampling period. Defaults to <see cref="SamplePeriod"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> or <paramref name="write"/> is null.</exception>
    public RepoContextMemoryWatch(
        ILogger<RepoContextMemoryWatch> logger,
        RepoContextMemoryAdmissionDecision decision,
        RepoContextMemoryObservation? previous,
        Func<RepoContextMemoryObservation, bool> write,
        Func<RepoContextHeapCeiling>? read = null,
        Func<DateTimeOffset>? clock = null,
        TimeSpan? samplePeriod = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _write = write ?? throw new ArgumentNullException(nameof(write));
        _decision = decision;
        _previous = previous;
        _read = read ?? ReadFromRuntime;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        _samplePeriod = samplePeriod ?? SamplePeriod;
    }

    /// <summary>The current reading, for the instruments that publish it.</summary>
    public RepoContextMemorySample Current => new(
        Interlocked.Read(ref _limitBytes),
        Interlocked.Read(ref _peakCommittedBytes),
        Interlocked.Read(ref _exhaustionEvents));

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Reported here rather than where it was decided: admission runs while the
        // host is still being built, before any logger exists. A refusal throws and
        // needs no logger; everything else has to wait for one, and an operator
        // reading the startup log is the only consumer either way.
        LogDecision();

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChanceException;
        _subscribed = true;

        // Guarded, and this is load-bearing rather than defensive habit. This is a
        // hosted service, so an exception escaping here fails host startup - which
        // would make the diagnostic that exists to stop an unexplained failure to
        // start into a cause of one. It measures the process; it does not serve it.
        try
        {
            // Sample once immediately so the instruments carry a real reading from
            // the first scrape, and record the admitted marker before any work is
            // accepted.
            Sample();
            Record(RepoContextMemoryOutcome.Admitted, force: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "RepoContext could not take its first heap reading. The measurement will be incomplete "
                + "and the next start may have nothing to admit against, which admits by default.");
        }

        _timer = new Timer(_ => SafeSample(), null, _samplePeriod, _samplePeriod);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_subscribed)
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChanceException;
            _subscribed = false;
        }

        _timer?.Change(Timeout.Infinite, Timeout.Infinite);

        // Guarded for the same reason as the start path, and one more: this runs
        // inside the stop grace period that the drain also has to fit into, so a
        // throw here would cost the remainder of a shutdown sequence to save a record
        // of it.
        try
        {
            Sample();

            // Completed claims a clean stop and nothing more - see
            // RepoContextMemoryOutcome. An exhausted run that then stops cleanly
            // keeps its exhausted outcome, because how it stopped does not unmake
            // what it measured.
            var outcome = Interlocked.Read(ref _exhaustionEvents) > 0
                ? RepoContextMemoryOutcome.Exhausted
                : RepoContextMemoryOutcome.Completed;
            Record(outcome, force: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RepoContext could not record its final heap measurement.");
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Takes one reading, advancing the high-water mark. Exposed so a test can drive
    /// the watch deterministically instead of waiting on a timer.
    /// </summary>
    public void Sample()
    {
        var ceiling = _read();

        if (ceiling.LimitBytes > 0)
        {
            Interlocked.Exchange(ref _limitBytes, ceiling.LimitBytes);
        }

        if (ceiling.CommittedBytes > Interlocked.Read(ref _peakCommittedBytes))
        {
            Interlocked.Exchange(ref _peakCommittedBytes, ceiling.CommittedBytes);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_subscribed)
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChanceException;
            _subscribed = false;
        }

        _timer?.Dispose();
        _timer = null;
    }

    private static RepoContextHeapCeiling ReadFromRuntime()
    {
        var info = GC.GetGCMemoryInfo();
        return new RepoContextHeapCeiling(
            info.TotalAvailableMemoryBytes,
            info.TotalCommittedBytes,
            info.HighMemoryLoadThresholdBytes);
    }

    // Three instructions and no allocation: this runs while the process is out of
    // memory, so allocating here risks a second OutOfMemoryException inside the
    // handler for the first one. Everything else happens on the sampling timer.
    private void OnFirstChanceException(object? sender, FirstChanceExceptionEventArgs e)
    {
        if (e.Exception is OutOfMemoryException)
        {
            Interlocked.Increment(ref _exhaustionEvents);
        }
    }

    private void SafeSample()
    {
        try
        {
            Sample();

            var exhausted = Interlocked.Read(ref _exhaustionEvents) > 0;
            Record(
                exhausted ? RepoContextMemoryOutcome.Exhausted : RepoContextMemoryOutcome.Admitted,
                force: exhausted && !_exhaustionRecorded);
        }
        catch (Exception ex)
        {
            // Catching everything, on purpose: this is a timer callback, and an
            // exception escaping one is unhandled on a thread-pool thread, which
            // terminates the process. A sampling failure taking the container down
            // would be this diagnostic causing exactly the class of unexplained exit
            // it was added to explain.
            _logger.LogDebug(ex, "RepoContext heap sampling failed; the measurement is incomplete.");
        }
    }

    /// <summary>
    /// Records the current reading, carrying the ever-worst exhaustion ceiling
    /// forward so evidence recorded under one grant survives a run under another.
    /// </summary>
    /// <param name="outcome">The outcome to record.</param>
    /// <param name="force">
    /// Whether to write regardless of how little has changed. Set at the transitions
    /// that matter - admission, first exhaustion, and stop - so the routine path can
    /// stay quiet without those ever being skipped.
    /// </param>
    private void Record(RepoContextMemoryOutcome outcome, bool force)
    {
        lock (_writeGate)
        {
            var limit = Interlocked.Read(ref _limitBytes);
            var peak = Interlocked.Read(ref _peakCommittedBytes);
            var events = Interlocked.Read(ref _exhaustionEvents);

            // Written only when something an operator would act on has changed: a
            // forced transition, or a peak that has grown by at least a hundredth of
            // the ceiling. The data mount is shared with the WAL, so a write every
            // fifteen seconds for a byte of drift would be noise competing with the
            // durability path it sits beside.
            if (!force && (_lastRecordedPeak < 0 || limit <= 0 || peak - _lastRecordedPeak < limit / 100))
            {
                return;
            }

            var thisRun = new RepoContextMemoryObservation(
                _clock(),
                outcome,
                limit,
                peak,
                events,
                outcome == RepoContextMemoryOutcome.Exhausted && limit > 0 ? limit : null,
                _decision.HonouredOverrideBytes);

            var record = thisRun with { ExhaustedAtLimitBytes = thisRun.MergeExhaustionHighWater(_previous) };

            if (_write(record))
            {
                _lastRecordedPeak = peak;
                if (outcome == RepoContextMemoryOutcome.Exhausted && !_exhaustionRecorded)
                {
                    _exhaustionRecorded = true;
                    _logger.LogError(
                        "RepoContext ran out of managed heap: {Events} OutOfMemoryException observed at a "
                        + "ceiling of {LimitBytes} bytes, peaking at {PeakBytes} bytes. This grant is too "
                        + "small for this corpus and the next start will refuse it unless the grant is "
                        + "raised. It does not present as an out-of-memory kill - expect STORAGE errors "
                        + "reading grain state and a crash-loop that reads as flakiness.",
                        events,
                        limit,
                        peak);
                }
            }
            else if (force)
            {
                _logger.LogWarning(
                    "RepoContext could not record its measured heap requirement ({Outcome}, peak "
                    + "{PeakBytes} bytes of {LimitBytes} bytes). The next start will have no measurement "
                    + "to admit against and will admit by default.",
                    outcome,
                    peak,
                    limit);
            }
        }
    }

    private void LogDecision()
    {
        switch (_decision.Verdict)
        {
            case RepoContextMemoryVerdict.Warn:
                // Warning on every start it applies to, never once: an override or a
                // shortfall that announces itself a single time is indistinguishable
                // from one nobody ever set.
                _logger.LogWarning("{Message}", _decision.Message);
                break;
            case RepoContextMemoryVerdict.Refuse:
                // Unreachable in practice - a refusal throws before the host is built,
                // so this service is never constructed. Present so that a future
                // caller which downgrades a refusal still reports it.
                _logger.LogError("{Message}", _decision.Message);
                break;
            default:
                _logger.LogInformation("{Message}", _decision.Message);
                break;
        }
    }
}
