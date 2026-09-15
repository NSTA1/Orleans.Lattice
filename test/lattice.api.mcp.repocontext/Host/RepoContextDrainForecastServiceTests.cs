using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextDrainForecastService"/>, the service that reports the
/// budget-versus-drain mismatch while the container is running rather than at the
/// stop that fails because of it.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2598 asks either that the drain budget become a function of the work, or
/// that the container refuse or loudly warn when the resident set implies a drain
/// that cannot fit. The first is not available: the budget is bounded from outside by
/// a grant the process cannot observe, and raising it past that grant silences the
/// drain-abandoned alarm instead of buying drain time. So this is the second, and
/// these tests pin the two properties that make it worth anything - that the warning
/// fires on evidence rather than on a constant, and that the absence of evidence is
/// reported as absence rather than as a pass.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextDrainForecastServiceTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(90);
    private static readonly DateTimeOffset Observed = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private static RepoContextShutdownBudgetResolution Resolution(bool declared = false)
        => new(RepoContextShutdownBudget.DefaultStopGracePeriod, Budget, declared);

    private static RepoContextDrainObservation Drain(
        RepoContextDrainOutcome outcome,
        double? seconds,
        int? resident = 10_000)
        => new(
            Observed,
            outcome,
            Budget,
            seconds is { } value ? TimeSpan.FromSeconds(value) : null,
            resident);

    private static RepoContextDrainForecastService Service(
        LevelLogger logger,
        RepoContextDrainObservation? last,
        Func<int?>? resident = null,
        bool declared = false)
        => new(logger, Resolution(declared), last, resident ?? (() => null));

    [Test]
    public void With_no_recorded_drain_the_startup_report_says_so_rather_than_reporting_a_pass()
    {
        // A first start has no evidence either way. Reporting the absence of evidence
        // as "the budget is fine" is how a budget survives years of never covering
        // the work, which is precisely the history issue #2598 records.
        var logger = new LevelLogger();
        using var service = Service(logger, last: null);

        service.ReportForecast();

        Assert.That(logger.Lines, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(logger.Lines[0].Level, Is.EqualTo(LogLevel.Information));
            Assert.That(logger.Lines[0].Message, Does.Contain("no record of a previous drain"));
            Assert.That(logger.Lines[0].Message, Does.Contain("90s"));
        });
    }

    [Test]
    public void A_previous_drain_that_did_not_fit_is_reported_at_error_with_the_grant_that_would_have_covered_it()
    {
        // The gate run 2 measurement. The load-bearing part is the number: "raise the
        // grace period" without one is how a value gets doubled and found wrong in
        // the same direction later.
        var logger = new LevelLogger();
        using var service = Service(logger, Drain(RepoContextDrainOutcome.Abandoned, 102.1));

        service.ReportForecast();

        Assert.That(logger.Lines, Has.Count.EqualTo(1));
        var (level, message) = logger.Lines[0];
        Assert.Multiple(() =>
        {
            Assert.That(level, Is.EqualTo(LogLevel.Error));
            Assert.That(message, Does.Contain("102.1s"));
            Assert.That(message, Does.Contain("137s"), "the required grant, derived from the measured drain");
            Assert.That(message, Does.Contain("70"), "the exit code the next stop is expected to produce");
            Assert.That(message, Does.Contain(RepoContextShutdownBudget.StopGracePeriodKey));
        });
    }

    [Test]
    public void A_previous_process_killed_mid_drain_is_reported_at_error_as_a_grant_smaller_than_the_declaration()
    {
        // The only evidence available from inside a container that its real grace
        // period is smaller than it was told, and it is only available across a
        // restart, which is why the record is written before the drain rather than
        // after it.
        var logger = new LevelLogger();
        using var service = Service(logger, Drain(RepoContextDrainOutcome.Started, seconds: null));

        service.ReportForecast();

        Assert.That(logger.Lines, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(logger.Lines[0].Level, Is.EqualTo(LogLevel.Error));
            Assert.That(logger.Lines[0].Message, Does.Contain("KILLED mid-drain"));
            Assert.That(logger.Lines[0].Message, Does.Contain("real"));
        });
    }

    [Test]
    public void A_declared_grant_and_an_assumed_one_are_distinguishable_in_the_report()
    {
        // Issue #2598's second trap, from the other side. At the conventional 120s the
        // declared and assumed grants derive the SAME budget - a budget that moved
        // would mean the derivation was ignoring one of them - so the difference
        // declaring it buys cannot be a different budget. It is that an assumed grant
        // is known to be unverified, which is what this states.
        var assumed = new LevelLogger();
        var declared = new LevelLogger();

        using var assumedService = Service(assumed, Drain(RepoContextDrainOutcome.Started, null), declared: false);
        using var declaredService = Service(declared, Drain(RepoContextDrainOutcome.Started, null), declared: true);

        assumedService.ReportForecast();
        declaredService.ReportForecast();

        Assert.Multiple(() =>
        {
            Assert.That(assumed.Lines[0].Message, Does.Contain("ASSUMED and unverified"));
            Assert.That(declared.Lines[0].Message, Does.Contain("DECLARED and contradicted by this evidence"));
            Assert.That(
                assumedService.Forecast.Budget,
                Is.EqualTo(declaredService.Forecast.Budget),
                "the budget must NOT differ, or the derivation would be ignoring one of the two grants");
        });
    }

    [Test]
    public void A_previous_drain_that_fitted_thinly_is_reported_at_warning_rather_than_at_error_or_information()
    {
        var logger = new LevelLogger();
        using var service = Service(logger, Drain(RepoContextDrainOutcome.Completed, 67.2));

        service.ReportForecast();

        Assert.Multiple(() =>
        {
            Assert.That(logger.Lines[0].Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(logger.Lines[0].Message, Does.Contain("67.2s"));
            Assert.That(logger.Lines[0].Message, Does.Contain("It fits"));
        });
    }

    [Test]
    public void A_previous_drain_with_headroom_is_reported_at_information()
    {
        // The positive control. Without it, every severity assertion above would pass
        // just as happily against a service that logged everything at Error.
        var logger = new LevelLogger();
        using var service = Service(logger, Drain(RepoContextDrainOutcome.Completed, 20));

        service.ReportForecast();

        Assert.That(logger.Lines[0].Level, Is.EqualTo(LogLevel.Information));
    }

    [Test]
    public void A_poll_whose_projection_passes_the_budget_reports_it_at_error_while_the_container_is_running()
    {
        // The warning issue #2598 asks for, fired from the resident set rather than
        // from a stop. The per-activation cost is measured, not chosen: 30s over
        // 10,000 activations, applied to 40,000 resident now.
        var logger = new LevelLogger();
        using var service = Service(
            logger,
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            resident: () => 40_000);

        var projection = service.PollOnce();

        Assert.That(projection, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(projection!.Value.ProjectedDrain.TotalSeconds, Is.EqualTo(120).Within(1e-6));
            Assert.That(projection.Value.ExceedsBudget, Is.True);
            Assert.That(logger.Lines, Has.Count.EqualTo(1));
            Assert.That(logger.Lines[0].Level, Is.EqualTo(LogLevel.Error));
            Assert.That(logger.Lines[0].Message, Does.Contain("will NOT deactivate in time"));
            Assert.That(logger.Lines[0].Message, Does.Contain("160s"), "the grant the projection requires");
        });
    }

    [Test]
    public void A_projection_that_has_not_changed_verdict_is_not_reported_again()
    {
        // A line a minute is noise an operator filters, and the filtered line is the
        // one that mattered.
        var logger = new LevelLogger();
        using var service = Service(
            logger,
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            resident: () => 40_000);

        service.PollOnce();
        service.PollOnce();
        service.PollOnce();

        Assert.That(logger.Lines, Has.Count.EqualTo(1));
    }

    [Test]
    public void A_projection_that_returns_within_the_budget_is_reported_once_as_recovered()
    {
        var logger = new LevelLogger();
        var resident = 40_000;
        using var service = Service(
            logger,
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            resident: () => resident);

        service.PollOnce();
        resident = 5_000;
        service.PollOnce();

        Assert.That(logger.Lines, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(logger.Lines[1].Level, Is.EqualTo(LogLevel.Information));
            Assert.That(logger.Lines[1].Message, Does.Contain("which fits"));
        });
    }

    [Test]
    public void An_unreadable_residency_yields_no_projection_rather_than_a_projection_of_zero()
    {
        var logger = new LevelLogger();
        using var service = Service(
            logger,
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            resident: () => null);

        Assert.Multiple(() =>
        {
            Assert.That(service.PollOnce(), Is.Null);
            Assert.That(service.Projection, Is.Null);
            Assert.That(service.ResidentActivations, Is.Null);
            Assert.That(logger.Lines, Is.Empty, "an unavailable reading is not a finding to alarm on");
        });
    }

    [Test]
    public void A_throwing_residency_probe_is_swallowed_because_a_diagnostic_must_not_stop_the_container()
    {
        var logger = new LevelLogger();
        using var service = Service(
            logger,
            Drain(RepoContextDrainOutcome.Completed, 30),
            resident: () => throw new InvalidOperationException("probe fault"));

        Assert.That(() => service.PollOnce(), Throws.Nothing);
        Assert.That(service.Projection, Is.Null);
    }

    [Test]
    public void Residency_is_tracked_even_with_no_recorded_drain_to_project_it_against()
    {
        // A first-ever start can read the resident set but has nothing to project it
        // against. Reporting no residency at all there would leave the deployment
        // least able to spare a signal with none.
        var logger = new LevelLogger();
        using var service = Service(logger, last: null, resident: () => 1234);

        Assert.Multiple(() =>
        {
            Assert.That(service.PollOnce(), Is.Null, "no measured cost, so no projection");
            Assert.That(service.ResidentActivations, Is.EqualTo(1234), "but the count itself is known");
        });
    }

    [Test]
    public void The_gauges_are_published_on_the_meter_the_container_scrape_endpoint_already_collects()
    {
        // RepoContextMetricsCollector subscribes to every meter under this prefix, so
        // naming the meter correctly is the whole of the wiring. A meter named outside
        // it would publish perfectly and be scraped by nobody.
        Assert.That(
            RepoContextDrainForecastService.MeterName,
            Does.StartWith(RepoContextMetricsCollector.MeterNamePrefix));
    }

    [Test]
    public void The_budget_and_grant_provenance_gauges_report_without_needing_a_poll()
    {
        var logger = new LevelLogger();
        using var service = Service(logger, Drain(RepoContextDrainOutcome.Abandoned, 102.1), declared: true);

        var readings = Collect(service);

        Assert.Multiple(() =>
        {
            Assert.That(
                readings[RepoContextDrainForecastService.BudgetGaugeName],
                Is.EqualTo(90).Within(1e-6));
            Assert.That(
                readings[RepoContextDrainForecastService.GrantDeclaredGaugeName],
                Is.EqualTo(1).Within(1e-6));
            Assert.That(
                readings[RepoContextDrainForecastService.LastDrainGaugeName],
                Is.EqualTo(102.1).Within(1e-6));
            Assert.That(
                readings[RepoContextDrainForecastService.ForecastGaugeName],
                Is.EqualTo((double)(int)RepoContextDrainForecastVerdict.Exceeded).Within(1e-6));
        });
    }

    [Test]
    public void The_projection_gauges_report_nothing_until_a_projection_exists()
    {
        // Absent, not zero. A projected drain of zero reads as a container in perfect
        // health, which is the opposite of "we cannot tell".
        var logger = new LevelLogger();
        using var service = Service(logger, last: null, resident: () => null);

        var readings = Collect(service);

        Assert.Multiple(() =>
        {
            Assert.That(readings.ContainsKey(RepoContextDrainForecastService.ProjectedDrainGaugeName), Is.False);
            Assert.That(readings.ContainsKey(RepoContextDrainForecastService.RequiredGrantGaugeName), Is.False);
            Assert.That(readings.ContainsKey(RepoContextDrainForecastService.ResidentGaugeName), Is.False);
            Assert.That(readings.ContainsKey(RepoContextDrainForecastService.LastDrainGaugeName), Is.False);
        });
    }

    [Test]
    public void The_projection_gauges_report_the_live_residency_and_the_grant_it_requires()
    {
        var logger = new LevelLogger();
        using var service = Service(
            logger,
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            resident: () => 40_000);

        service.PollOnce();
        var readings = Collect(service);

        Assert.Multiple(() =>
        {
            Assert.That(readings[RepoContextDrainForecastService.ResidentGaugeName], Is.EqualTo(40_000).Within(1e-6));
            Assert.That(
                readings[RepoContextDrainForecastService.ProjectedDrainGaugeName],
                Is.EqualTo(120).Within(1e-6));
            Assert.That(
                readings[RepoContextDrainForecastService.RequiredGrantGaugeName],
                Is.EqualTo(160).Within(1e-6));
        });
    }

    [Test]
    public async Task Starting_the_service_emits_the_startup_forecast_and_stopping_it_returns_promptly()
    {
        // This service is one of the hosted services the drain waits on, so a slow
        // stop would be charged to the very budget it exists to report on.
        var logger = new LevelLogger();
        using var service = Service(logger, Drain(RepoContextDrainOutcome.Abandoned, 102.1));

        await service.StartAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(1));

        var stop = service.StopAsync(CancellationToken.None);
        var finished = await Task.WhenAny(stop, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.That(ReferenceEquals(finished, stop), Is.True, "the poll loop must not outlive the stop");
        await stop;
    }

    [Test]
    public void Disposing_twice_is_safe_because_the_host_disposes_it_on_a_shutdown_path()
    {
        var service = Service(new LevelLogger(), last: null);
        service.Dispose();

        Assert.That(service.Dispose, Throws.Nothing);
    }

    private static Dictionary<string, double> Collect(RepoContextDrainForecastService service)
    {
        _ = service;
        var readings = new Dictionary<string, double>(StringComparer.Ordinal);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextDrainForecastService.MeterName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<double>((instrument, measurement, _, _) => readings[instrument.Name] = measurement);
        listener.Start();
        listener.RecordObservableInstruments();
        return readings;
    }

    /// <summary>
    /// Records severity alongside the message, because the point of the forecast is
    /// that a projected overrun must not be reported at the same level as a fit.
    /// </summary>
    private sealed class LevelLogger : ILogger<RepoContextDrainForecastService>
    {
        private readonly List<(LogLevel Level, string Message)> _lines = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Lines
        {
            get { lock (_lines) { return _lines.ToArray(); } }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel > LogLevel.Debug;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel <= LogLevel.Debug)
            {
                return;
            }

            lock (_lines)
            {
                _lines.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
