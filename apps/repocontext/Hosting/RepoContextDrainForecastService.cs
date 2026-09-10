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
/// </remarks>
public sealed class RepoContextDrainForecastService : IHostedService, IDisposable
{
    /// <summary>
    /// The meter these gauges are published on. Named under the collector's
    /// <see cref="RepoContextMetricsCollector.MeterNamePrefix"/> so the container's
    /// existing scrape endpoint picks them up without further wiring.
    /// </summary>
    public const string MeterName = "orleans.lattice.repocontext.host";

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

    /// <summary>The default interval between residency polls.</summary>
    /// <remarks>
    /// A minute is slow enough to cost nothing and fast enough that a container which
    /// grows into an unstoppable state is reported long before the operator next
    /// stops it, which is the only deadline this poll has.
    /// </remarks>
    public static readonly TimeSpan DefaultPollInterval = TimeSpan.FromMinutes(1);

    private readonly ILogger<RepoContextDrainForecastService> _logger;
    private readonly RepoContextShutdownBudgetResolution _resolution;
    private readonly RepoContextDrainForecast _forecast;
    private readonly Func<int?> _residentActivations;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly TimeSpan _pollInterval;
    private readonly Meter _meter;
    private readonly Lock _gate = new();
    private RepoContextDrainProjection? _projection;
    private int? _resident;
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
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> or <paramref name="residentActivations"/> is null.</exception>
    public RepoContextDrainForecastService(
        ILogger<RepoContextDrainForecastService> logger,
        RepoContextShutdownBudgetResolution resolution,
        RepoContextDrainObservation? last,
        Func<int?> residentActivations,
        TimeSpan? pollInterval = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _residentActivations = residentActivations ?? throw new ArgumentNullException(nameof(residentActivations));
        _resolution = resolution;
        _forecast = RepoContextDrainForecast.Evaluate(last, resolution.ShutdownBudget);
        _pollInterval = pollInterval ?? DefaultPollInterval;
        _delay = delay ?? Task.Delay;

        _meter = new Meter(MeterName);
        _meter.CreateObservableGauge(BudgetGaugeName, () => _resolution.ShutdownBudget.TotalSeconds);
        _meter.CreateObservableGauge(GrantDeclaredGaugeName, () => _resolution.GrantWasDeclared ? 1d : 0d);
        _meter.CreateObservableGauge(ForecastGaugeName, () => (double)(int)_forecast.Verdict);
        _meter.CreateObservableGauge(LastDrainGaugeName, ObserveLastDrain);
        _meter.CreateObservableGauge(ResidentGaugeName, ObserveResident);
        _meter.CreateObservableGauge(ProjectedDrainGaugeName, ObserveProjectedDrain);
        _meter.CreateObservableGauge(RequiredGrantGaugeName, ObserveRequiredGrant);
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
    /// Samples residency once, updates the projection, and reports a change in
    /// whether the projected drain fits the budget.
    /// </summary>
    /// <remarks>
    /// Reporting on transition rather than on every poll is what keeps this usable: a
    /// line per minute would be noise an operator filters out, and the filtered line
    /// would be the one that mattered. Exposed so a test can drive one poll instead of
    /// waiting one out.
    /// </remarks>
    /// <returns>The projection made, or <see langword="null"/> when none could be made.</returns>
    public RepoContextDrainProjection? PollOnce()
    {
        var resident = SampleResident();
        if (resident is not { } count || !_forecast.TryProject(count, out var projection))
        {
            lock (_gate)
            {
                _resident = resident;
                _projection = null;
            }

            return null;
        }

        bool report;
        lock (_gate)
        {
            _resident = count;
            _projection = projection;
            report = _lastReportedExceeds != projection.ExceedsBudget;
            _lastReportedExceeds = projection.ExceedsBudget;
        }

        if (!report)
        {
            return projection;
        }

        if (projection.ExceedsBudget)
        {
            _logger.LogError(
                "RepoContext projects a drain of {ProjectedSeconds:F1}s against a {BudgetSeconds:F0}s shutdown "
                + "budget: the {Resident} resident activations, at the {CostMilliseconds:F1}ms each the last "
                + "drain measured, will NOT deactivate in time. The next stop is expected to be abandoned and to "
                + "exit {ExitCode}, tearing down leaf activations without banking their projection checkpoints. "
                + "This is reported now, while the container is running, so it can be acted on before that stop: "
                + "raise the service's stop_grace_period and the {Key} that declares it together to at least "
                + "{RequiredSeconds:F0}s, or reduce the resident set.",
                projection.ProjectedDrain.TotalSeconds,
                projection.Budget.TotalSeconds,
                projection.ResidentActivations,
                (_forecast.PerActivationCost ?? TimeSpan.Zero).TotalMilliseconds,
                RepoContextExitCode.DrainAbandoned,
                RepoContextShutdownBudget.StopGracePeriodKey,
                projection.RequiredStopGracePeriod.TotalSeconds);
            return projection;
        }

        _logger.LogInformation(
            "RepoContext projects a drain of {ProjectedSeconds:F1}s against a {BudgetSeconds:F0}s shutdown budget "
            + "from {Resident} resident activations, which fits.",
            projection.ProjectedDrain.TotalSeconds,
            projection.Budget.TotalSeconds,
            projection.ResidentActivations);
        return projection;
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
                    + "derived from the measured drain and grants no headroom, so allow for growth.",
                    (_forecast.Last?.Duration ?? TimeSpan.Zero).TotalSeconds,
                    _resolution.ShutdownBudget.TotalSeconds,
                    (_forecast.ConsumedFraction ?? 0d) * 100d,
                    RepoContextExitCode.DrainAbandoned,
                    _resolution.GrantWasDeclared ? "DECLARED" : "ASSUMED and unverified",
                    RepoContextShutdownBudget.StopGracePeriodKey,
                    (_forecast.RequiredStopGracePeriod ?? TimeSpan.Zero).TotalSeconds);
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
}
