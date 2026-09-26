using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Reports the relationship between the shutdown budget this process derived and the
/// drain that budget has to cover: once at startup against the last recorded drain,
/// and then continuously against the resident activation set as it grows.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this fixes, and what it deliberately does not.</b> Issue #2598 records a
/// 90s budget meeting a 102.1s drain, and observes that declaring the grace period
/// the derivation already assumes changes nothing. It does not, and no arrangement of
/// this budget can: it is bounded from outside by the container's real
/// <c>stop_grace_period</c>, which the process cannot see and cannot exceed, and a
/// budget raised past that grant silences the drain-abandoned alarm rather than
/// buying drain time (see <see cref="RepoContextShutdownBudget"/>). So this service
/// does not adapt the budget. It removes the property that made the overrun a
/// surprise: that the mismatch was <b>only ever observable at the moment it was too
/// late to act on</b>.
/// </para>
/// <para>
/// <b>Both halves are measured.</b> The startup report compares this process's budget
/// with the previous process's measured drain, carried across the restart by
/// <see cref="RepoContextDrainHistory"/>. The running report multiplies the live
/// resident activation count by the per-activation cost that same drain measured. No
/// constant is invented at either end: with no recorded drain the service reports
/// that it cannot project, which is the honest answer and is distinguishable in the
/// exposition from a projection of zero.
/// </para>
/// <para>
/// <b>The running report is what answers the standing remark.</b>
/// <see cref="RepoContextDrainSignal"/> has shipped the sentence "drain duration
/// tracks the resident activation set, which nothing here bounds" as a remark in an
/// abandonment message, which is to say it is emitted to explain a failure that has
/// already happened. Nothing here bounds that set either. What changes is that the
/// set's consequence is now compared with the budget on every poll, so the container
/// says it will not be able to stop cleanly <i>while it is still running</i>.
/// </para>
/// <para>
/// <b>The stop gate is the windowed peak, not the instant (issue #3628).</b> The
/// resident set swings by an order of magnitude within minutes, so an instant reading
/// that fits says almost nothing about the reading fifteen seconds later, when the
/// drain takes its own sample. A deploy stopped on a "fits" reading of 786 activations
/// began its drain with 1,791 resident and was abandoned at the budget. The service
/// therefore samples every <see cref="DefaultPollInterval"/>, retains the samples of
/// the trailing <see cref="DefaultPeakWindow"/>, and reports the projection at the
/// <b>peak</b> of that window next to the instant one. The logged verdict follows the
/// peak, which is the reading a stop should be gated on.
/// </para>
/// </remarks>
public sealed class RepoContextDrainForecastService : IHostedService, IDisposable
{
    /// <summary>
    /// The meter these gauges are published on. Named under the collector's
    /// <see cref="RepoContextMetricsCollector.MeterNamePrefix"/> so the container's
    /// existing scrape endpoint picks them up without further wiring. Retained as an
    /// alias of <see cref="RepoContextHostMeter.Name"/>, which is the owner.
    /// </summary>
    public const string MeterName = RepoContextHostMeter.Name;

    /// <summary>The gauge reporting the budget the host will actually enforce.</summary>
    public const string BudgetGaugeName = "lattice_repocontext_shutdown_budget_seconds";

    /// <summary>
    /// The gauge reporting whether the container grant was declared (1) or assumed (0).
    /// </summary>
    /// <remarks>
    /// This is the observable difference between declaring
    /// <see cref="RepoContextShutdownBudget.StopGracePeriodKey"/> at the conventional
    /// 120s and leaving it unset, and it is deliberately not a difference in the
    /// budget: at that value the declared and assumed grants are the same number, so
    /// a budget that moved would mean the derivation was ignoring one of them. What
    /// changes is that an assumed grant is <b>known to be unverified</b>, which an
    /// alert can be written against and a log line buried in a cold start cannot.
    /// </remarks>
    public const string GrantDeclaredGaugeName = "lattice_repocontext_stop_grace_period_declared";

    /// <summary>The gauge reporting the duration of the last recorded drain.</summary>
    public const string LastDrainGaugeName = "lattice_repocontext_last_drain_seconds";

    /// <summary>
    /// The gauge reporting the startup forecast as a
    /// <see cref="RepoContextDrainForecastVerdict"/> ordinal.
    /// </summary>
    public const string ForecastGaugeName = "lattice_repocontext_drain_forecast";

    /// <summary>The gauge reporting the resident activation count.</summary>
    public const string ResidentGaugeName = "lattice_repocontext_resident_activations";

    /// <summary>The gauge reporting the drain the resident activation set projects to.</summary>
    public const string ProjectedDrainGaugeName = "lattice_repocontext_projected_drain_seconds";

    /// <summary>
    /// The gauge reporting the smallest container grant that would cover the
    /// projected drain.
    /// </summary>
    public const string RequiredGrantGaugeName = "lattice_repocontext_required_stop_grace_period_seconds";

    /// <summary>
    /// The gauge reporting the highest resident activation count sampled over the
    /// trailing peak window.
    /// </summary>
    public const string PeakResidentGaugeName = "lattice_repocontext_resident_activations_peak";

    /// <summary>
    /// The gauge reporting the drain the peak resident activation count over the
    /// trailing window projects to. This is the reading a stop should be gated on.
    /// </summary>
    public const string PeakProjectedDrainGaugeName = "lattice_repocontext_projected_drain_peak_seconds";

    /// <summary>
    /// The gauge reporting whether the projections are only floors (1) or scaled from
    /// a measured cost (0), because the last drain was abandoned (issue #3628).
    /// </summary>
    public const string ProjectionLowerBoundGaugeName = "lattice_repocontext_projected_drain_lower_bound";

    /// <summary>The default interval between residency polls.</summary>
    /// <remarks>
    /// Ten seconds, down from a minute (issue #3628). A sample costs one observable
    /// callback on an instrument Orleans already publishes, so polling faster costs
    /// nothing measurable, and a minute was long enough for the resident set to more
    /// than double between the reading an operator acted on and the drain that
    /// followed it.
    /// </remarks>
    public static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(10);

    /// <summary>The default trailing window the peak residency is taken over.</summary>
    /// <remarks>
    /// Ten minutes spans the residency waves the hourly compaction and reclaim walks
    /// drive (issue #3607) without holding a peak from the previous hour, so a peak
    /// that fits is evidence that the current wave has passed rather than that the
    /// last sample happened to land between two.
    /// </remarks>
    public static readonly TimeSpan DefaultPeakWindow = TimeSpan.FromMinutes(10);

    private readonly ILogger<RepoContextDrainForecastService> _logger;
    private readonly RepoContextShutdownBudgetResolution _resolution;
    private readonly RepoContextDrainForecast _forecast;
    private readonly Func<int?> _residentActivations;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<long> _timestamp;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _peakWindow;
    private readonly Meter _meter;
    private readonly Lock _gate = new();
    private readonly Queue<(long At, int Count)> _window = new();
    private RepoContextDrainProjection? _projection;
    private RepoContextDrainProjection? _peakProjection;
    private int? _resident;
    private int? _peakResident;
    private bool? _lastReportedExceeds;
    private CancellationTokenSource? _pollCancellation;
    private Task? _pollLoop;

    /// <summary>Initializes the forecast service.</summary>
    /// <param name="logger">The logger the forecast lines are written to.</param>
    /// <param name="resolution">The resolved grant and derived budget.</param>
    /// <param name="last">The last recorded drain, or <see langword="null"/>.</param>
    /// <param name="residentActivations">
    /// The resident activation probe. Returns <see langword="null"/> when no reading
    /// is available, which is reported as unavailable rather than as zero.
    /// </param>
    /// <param name="pollInterval">The residency poll interval, defaulting to <see cref="DefaultPollInterval"/>.</param>
    /// <param name="delay">The delay used between polls, injectable so a test need not wait one out.</param>
    /// <param name="peakWindow">The trailing window the peak residency is taken over, defaulting to <see cref="DefaultPeakWindow"/>.</param>
    /// <param name="timestamp">
    /// The monotonic timestamp source the peak window is measured on, defaulting to
    /// <see cref="Stopwatch.GetTimestamp"/>. Injectable so a test can age a sample out
    /// of the window without waiting.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> or <paramref name="residentActivations"/> is null.</exception>
    public RepoContextDrainForecastService(
        ILogger<RepoContextDrainForecastService> logger,
        RepoContextShutdownBudgetResolution resolution,
        RepoContextDrainObservation? last,
        Func<int?> residentActivations,
        TimeSpan? pollInterval = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        TimeSpan? peakWindow = null,
        Func<long>? timestamp = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _residentActivations = residentActivations ?? throw new ArgumentNullException(nameof(residentActivations));
        _resolution = resolution;
        _forecast = RepoContextDrainForecast.Evaluate(last, resolution.ShutdownBudget);
        _pollInterval = pollInterval ?? DefaultPollInterval;
        _peakWindow = peakWindow is { } window && window > TimeSpan.Zero ? window : DefaultPeakWindow;
        _delay = delay ?? Task.Delay;
        _timestamp = timestamp ?? Stopwatch.GetTimestamp;

        _meter = new Meter(MeterName);
        _meter.CreateObservableGauge(BudgetGaugeName, () => _resolution.ShutdownBudget.TotalSeconds);
        _meter.CreateObservableGauge(GrantDeclaredGaugeName, () => _resolution.GrantWasDeclared ? 1d : 0d);
        _meter.CreateObservableGauge(ForecastGaugeName, () => (double)(int)_forecast.Verdict);
        _meter.CreateObservableGauge(LastDrainGaugeName, ObserveLastDrain);
        _meter.CreateObservableGauge(ResidentGaugeName, ObserveResident);
        _meter.CreateObservableGauge(ProjectedDrainGaugeName, ObserveProjectedDrain);
        _meter.CreateObservableGauge(RequiredGrantGaugeName, ObserveRequiredGrant);
        _meter.CreateObservableGauge(PeakResidentGaugeName, ObservePeakResident);
        _meter.CreateObservableGauge(PeakProjectedDrainGaugeName, ObservePeakProjectedDrain);
        _meter.CreateObservableGauge(ProjectionLowerBoundGaugeName, ObserveProjectionLowerBound);
    }

    /// <summary>The forecast this service reported at startup.</summary>
    public RepoContextDrainForecast Forecast => _forecast;

    /// <summary>
    /// The most recent projection, or <see langword="null"/> when none could be made.
    /// </summary>
    public RepoContextDrainProjection? Projection
    {
        get { lock (_gate) { return _projection; } }
    }

    /// <summary>
    /// The most recent resident activation reading, or <see langword="null"/> when
    /// none was available.
    /// </summary>
    /// <remarks>
    /// Tracked separately from <see cref="Projection"/> because the two can differ:
    /// residency is readable on a first-ever start, where no recorded drain exists to
    /// project it against. Folding the two together would leave the container with no
    /// residency metric at all until its first clean stop, which is exactly the
    /// deployment least able to spare one.
    /// </remarks>
    public int? ResidentActivations
    {
        get { lock (_gate) { return _resident; } }
    }

    /// <summary>
    /// The highest resident activation count sampled over the trailing peak window,
    /// or <see langword="null"/> when no sample in the window was readable.
    /// </summary>
    public int? PeakResidentActivations
    {
        get { lock (_gate) { return _peakResident; } }
    }

    /// <summary>
    /// The projection made from <see cref="PeakResidentActivations"/>, or
    /// <see langword="null"/> when none could be made. This is the reading a stop
    /// should be gated on (issue #3628), and the one the logged verdict follows.
    /// </summary>
    public RepoContextDrainProjection? PeakProjection
    {
        get { lock (_gate) { return _peakProjection; } }
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ReportForecast();

        _pollCancellation = new CancellationTokenSource();
        _pollLoop = PollAsync(_pollCancellation.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Stopping promptly is not a nicety here: this service is one of the hosted
        // services the drain waits on, so a slow stop would be charged to the very
        // budget it exists to report on.
        _pollCancellation?.Cancel();

        if (_pollLoop is { } loop)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected: the loop was cancelled.
            }
        }
    }

    /// <summary>
    /// Samples residency once, updates the instant and windowed-peak projections, and
    /// reports a change in whether the peak projection fits the budget.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Reporting on transition rather than on every poll is what keeps this usable: a
    /// line per poll would be noise an operator filters out, and the filtered line
    /// would be the one that mattered. Exposed so a test can drive one poll instead of
    /// waiting one out.
    /// </para>
    /// <para>
    /// The transition follows the <b>peak</b> over the trailing window rather than the
    /// instant reading (issue #3628). The instant flips with every residency wave, so
    /// a verdict that followed it alternated by construction and cleared a stop on
    /// whichever sample happened to land between two waves.
    /// </para>
    /// </remarks>
    /// <returns>The instant projection made, or <see langword="null"/> when none could be made.</returns>
    public RepoContextDrainProjection? PollOnce()
    {
        var resident = SampleResident();
        var now = _timestamp();

        RepoContextDrainProjection? instant = null;
        RepoContextDrainProjection? peak = null;
        bool report;

        lock (_gate)
        {
            if (resident is { } sampled)
            {
                _window.Enqueue((now, sampled));
            }

            while (_window.Count > 0 && Stopwatch.GetElapsedTime(_window.Peek().At, now) > _peakWindow)
            {
                _window.Dequeue();
            }

            int? peakResident = null;
            foreach (var (_, count) in _window)
            {
                peakResident = peakResident is { } highest ? Math.Max(highest, count) : count;
            }

            if (resident is { } current && _forecast.TryProject(current, out var instantProjection))
            {
                instant = instantProjection;
            }

            if (peakResident is { } highestInWindow && _forecast.TryProject(highestInWindow, out var peakProjection))
            {
                peak = peakProjection;
            }

            _resident = resident;
            _peakResident = peakResident;
            _projection = instant;
            _peakProjection = peak;

            report = peak is { } gate && _lastReportedExceeds != gate.ExceedsBudget;
            if (peak is { } latched)
            {
                _lastReportedExceeds = latched.ExceedsBudget;
            }
        }

        if (report && peak is { } reported)
        {
            ReportProjection(reported, instant);
        }

        return instant;
    }

    private void ReportProjection(RepoContextDrainProjection peak, RepoContextDrainProjection? instant)
    {
        var costMilliseconds = (_forecast.PerActivationCost ?? TimeSpan.Zero).TotalMilliseconds;
        var instantClause = instant is { } now
            ? string.Create(
                System.Globalization.CultureInfo.InvariantCulture,
                $"{now.ResidentActivations} resident now, projecting {now.ProjectedDrain.TotalSeconds:F1}s")
            : "the resident count could not be read just now";
        var windowMinutes = _peakWindow.TotalMinutes;

        if (peak.ExceedsBudget)
        {
            _logger.LogError(
                "RepoContext projects a drain of {Qualifier}{ProjectedSeconds:F1}s against a {BudgetSeconds:F0}s "
                + "shutdown budget: the peak of {Resident} resident activations over the last {WindowMinutes:F0} "
                + "minutes ({Instant}), at the {CostMilliseconds:F1}ms each the last drain measured{CostQualifier}, "
                + "will NOT deactivate in time. The next stop is expected to be abandoned and to exit {ExitCode}, "
                + "tearing down leaf activations without banking their projection checkpoints. This is reported "
                + "now, while the container is running, so it can be acted on before that stop: gate the stop on "
                + "the windowed peak rather than on an instant reading, raise the service's stop_grace_period and "
                + "the {Key} that declares it together to at least {RequiredSeconds:F0}s, or reduce the resident set.",
                peak.IsLowerBound ? "AT LEAST " : string.Empty,
                peak.ProjectedDrain.TotalSeconds,
                peak.Budget.TotalSeconds,
                peak.ResidentActivations,
                windowMinutes,
                instantClause,
                costMilliseconds,
                peak.IsLowerBound
                    ? " (a FLOOR: that drain was abandoned before it finished, so the real cost is higher)"
                    : string.Empty,
                RepoContextExitCode.DrainAbandoned,
                RepoContextShutdownBudget.StopGracePeriodKey,
                peak.RequiredStopGracePeriod.TotalSeconds);
            return;
        }

        if (peak.IsLowerBound)
        {
            _logger.LogWarning(
                "RepoContext projects a drain of AT LEAST {ProjectedSeconds:F1}s against a {BudgetSeconds:F0}s "
                + "shutdown budget from the peak of {Resident} resident activations over the last "
                + "{WindowMinutes:F0} minutes ({Instant}). That floor fits, but it proves nothing: the "
                + "{CostMilliseconds:F1}ms per activation it scales was measured on a drain that was abandoned "
                + "before it finished, so the real cost is higher by an unknown margin. Treat a stop as unproven "
                + "until a drain completes and records a real measurement.",
                peak.ProjectedDrain.TotalSeconds,
                peak.Budget.TotalSeconds,
                peak.ResidentActivations,
                windowMinutes,
                instantClause,
                costMilliseconds);
            return;
        }

        _logger.LogInformation(
            "RepoContext projects a drain of {ProjectedSeconds:F1}s against a {BudgetSeconds:F0}s shutdown budget "
            + "from the peak of {Resident} resident activations over the last {WindowMinutes:F0} minutes "
            + "({Instant}), which fits.",
            peak.ProjectedDrain.TotalSeconds,
            peak.Budget.TotalSeconds,
            peak.ResidentActivations,
            windowMinutes,
            instantClause);
    }

    /// <summary>Disposes the meter these gauges are published on.</summary>
    public void Dispose()
    {
        _pollCancellation?.Cancel();
        _pollCancellation?.Dispose();
        _pollCancellation = null;
        _meter.Dispose();
    }

    /// <summary>
    /// Emits the startup forecast: what the last drain cost, measured against the
    /// budget this process derived.
    /// </summary>
    /// <remarks>
    /// Exposed so a test can assert the line and its severity without starting the
    /// service, and called once from <see cref="StartAsync"/>.
    /// </remarks>
    public void ReportForecast()
    {
        switch (_forecast.Verdict)
        {
            case RepoContextDrainForecastVerdict.NoHistory:
                _logger.LogInformation(
                    "RepoContext has no record of a previous drain, so the {BudgetSeconds:F0}s shutdown budget "
                    + "cannot yet be compared with what a drain actually costs here. The next clean stop records "
                    + "one, and from then on this line reports whether the budget covers it.",
                    _resolution.ShutdownBudget.TotalSeconds);
                return;

            case RepoContextDrainForecastVerdict.KilledMidDrain:
                _logger.LogError(
                    "RepoContext was KILLED mid-drain on the previous stop: a drain began and no outcome was "
                    + "ever recorded, so the process did not survive to report one. The container's real "
                    + "stop_grace_period is therefore smaller than that drain required, whatever this process "
                    + "was told - the {GrantSeconds:F0}s grant it derived its {BudgetSeconds:F0}s budget from is "
                    + "{Provenance}. Raise the service's stop_grace_period, and declare the same value in {Key} "
                    + "so the budget follows it.",
                    _resolution.StopGracePeriod.TotalSeconds,
                    _resolution.ShutdownBudget.TotalSeconds,
                    _resolution.GrantWasDeclared ? "DECLARED and contradicted by this evidence" : "ASSUMED and unverified",
                    RepoContextShutdownBudget.StopGracePeriodKey);
                return;

            case RepoContextDrainForecastVerdict.Exceeded:
                _logger.LogError(
                    "RepoContext's last drain took {DrainSeconds:F1}s and does not fit this process's "
                    + "{BudgetSeconds:F0}s shutdown budget ({ConsumedPercent:F0}% of it), so the next stop is "
                    + "expected to be abandoned and to exit {ExitCode} unless something changes first. The grant "
                    + "the budget was derived from is {Provenance}. Raise the service's stop_grace_period and "
                    + "the {Key} that declares it together to at least {RequiredSeconds:F0}s - that figure is "
                    + "derived from the measured drain and grants no headroom, so allow for growth.{Stranded}",
                    (_forecast.Last?.Duration ?? TimeSpan.Zero).TotalSeconds,
                    _resolution.ShutdownBudget.TotalSeconds,
                    (_forecast.ConsumedFraction ?? 0d) * 100d,
                    RepoContextExitCode.DrainAbandoned,
                    _resolution.GrantWasDeclared ? "DECLARED" : "ASSUMED and unverified",
                    RepoContextShutdownBudget.StopGracePeriodKey,
                    (_forecast.RequiredStopGracePeriod ?? TimeSpan.Zero).TotalSeconds,
                    DescribeStranded(_forecast.Last));
                return;

            case RepoContextDrainForecastVerdict.Unproven:
                _logger.LogWarning(
                    "RepoContext's last drain was ABANDONED after {DrainSeconds:F1}s, under a "
                    + "{PreviousBudgetSeconds:F0}s budget that was smaller than this process's "
                    + "{BudgetSeconds:F0}s one. That duration is therefore a FLOOR on what a complete drain "
                    + "costs here and not a measurement of one, because the drain was cut short before it "
                    + "finished. The floor is {ConsumedPercent:F0}% of this budget, so the next stop is NOT "
                    + "predicted to be abandoned - but this budget stays unproven until a drain completes "
                    + "inside it. The grant the budget was derived from is {Provenance}. The floor alone needs "
                    + "a grant of {RequiredSeconds:F0}s, which the {GrantSeconds:F0}s declared here already "
                    + "covers, so nothing needs raising on this evidence. The next clean stop measures the real "
                    + "requirement and replaces this line with it.{Stranded}",
                    (_forecast.Last?.Duration ?? TimeSpan.Zero).TotalSeconds,
                    (_forecast.Last?.Budget ?? TimeSpan.Zero).TotalSeconds,
                    _resolution.ShutdownBudget.TotalSeconds,
                    (_forecast.ConsumedFraction ?? 0d) * 100d,
                    _resolution.GrantWasDeclared ? "DECLARED" : "ASSUMED and unverified",
                    (_forecast.RequiredStopGracePeriod ?? TimeSpan.Zero).TotalSeconds,
                    _resolution.StopGracePeriod.TotalSeconds,
                    DescribeStranded(_forecast.Last));
                return;

            case RepoContextDrainForecastVerdict.Thin:
                _logger.LogWarning(
                    "RepoContext's last drain took {DrainSeconds:F1}s, consuming {ConsumedPercent:F0}% of this "
                    + "process's {BudgetSeconds:F0}s shutdown budget. It fits, but drain time grows with the "
                    + "resident activation set, so the next growth may carry it past the budget.",
                    (_forecast.Last?.Duration ?? TimeSpan.Zero).TotalSeconds,
                    (_forecast.ConsumedFraction ?? 0d) * 100d,
                    _resolution.ShutdownBudget.TotalSeconds);
                return;

            default:
                _logger.LogInformation(
                    "RepoContext's last drain took {DrainSeconds:F1}s, consuming {ConsumedPercent:F0}% of this "
                    + "process's {BudgetSeconds:F0}s shutdown budget.",
                    (_forecast.Last?.Duration ?? TimeSpan.Zero).TotalSeconds,
                    (_forecast.ConsumedFraction ?? 0d) * 100d,
                    _resolution.ShutdownBudget.TotalSeconds);
                return;
        }
    }

    /// <summary>
    /// Renders what an abandoned drain left behind, from the stranded count the
    /// drain record carries (issue #3628), as a clause to append to the startup line.
    /// Empty when the record carries none, so an old record reads as it always did.
    /// </summary>
    private static string DescribeStranded(RepoContextDrainObservation? last)
    {
        if (last is not { Outcome: RepoContextDrainOutcome.Abandoned, StrandedActivations: { } stranded })
        {
            return string.Empty;
        }

        var culture = System.Globalization.CultureInfo.InvariantCulture;
        return last.Value.ResidentActivations is { } resident
            ? string.Create(
                culture,
                $" When the host stopped waiting, {stranded} of the {resident} activations resident at the start were still resident and were torn down without banking their projection checkpoints.")
            : string.Create(
                culture,
                $" When the host stopped waiting, {stranded} activations were still resident and were torn down without banking their projection checkpoints.");
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _delay(_pollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                PollOnce();
            }
            catch (Exception ex)
            {
                // Diagnostics must never take the process down, and a projection is
                // worth strictly less than the container it is reporting on.
                _logger.LogDebug(ex, "The RepoContext drain forecast poll failed.");
            }
        }
    }

    private int? SampleResident()
    {
        try
        {
            return _residentActivations();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Sampling the RepoContext resident activation count failed.");
            return null;
        }
    }

    private IEnumerable<Measurement<double>> ObserveLastDrain()
        => _forecast.Last?.Duration is { } duration
            ? [new Measurement<double>(duration.TotalSeconds)]
            : [];

    private IEnumerable<Measurement<double>> ObserveResident()
    {
        var resident = ResidentActivations;
        return resident is { } value ? [new Measurement<double>(value)] : [];
    }

    private IEnumerable<Measurement<double>> ObserveProjectedDrain()
    {
        var projection = Projection;
        return projection is { } value ? [new Measurement<double>(value.ProjectedDrain.TotalSeconds)] : [];
    }

    private IEnumerable<Measurement<double>> ObserveRequiredGrant()
    {
        var projection = Projection;
        return projection is { } value
            ? [new Measurement<double>(value.RequiredStopGracePeriod.TotalSeconds)]
            : [];
    }

    private IEnumerable<Measurement<double>> ObservePeakResident()
    {
        var peak = PeakResidentActivations;
        return peak is { } value ? [new Measurement<double>(value)] : [];
    }

    private IEnumerable<Measurement<double>> ObservePeakProjectedDrain()
    {
        var projection = PeakProjection;
        return projection is { } value ? [new Measurement<double>(value.ProjectedDrain.TotalSeconds)] : [];
    }

    private IEnumerable<Measurement<double>> ObserveProjectionLowerBound()
        => _forecast.PerActivationCost is null
            ? []
            : [new Measurement<double>(_forecast.PerActivationCostIsLowerBound ? 1d : 0d)];
}
