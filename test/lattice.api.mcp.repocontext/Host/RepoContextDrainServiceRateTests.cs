using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>Exercises calibrated drain forecasts through the production poll and durable history seams.</summary>
[TestFixture]
public sealed class RepoContextDrainServiceRateTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(180);

    [Test]
    public void PollOnce_single_clean_sample_does_not_claim_a_calibrated_service_rate()
    {
        var logger = new RecordingLogger();
        using var service = new RepoContextDrainForecastService(
            logger, new(TimeSpan.FromSeconds(240), Budget, true),
            new(DateTimeOffset.UtcNow, RepoContextDrainOutcome.Completed, Budget,
                TimeSpan.FromSeconds(94), 1000), () => 3750);

        Assert.That(service.PollOnce(), Is.Null,
            "one wall-time observation cannot distinguish fixed shutdown overhead from aggregate service rate");
        Assert.That(logger.Lines.Single().Message, Does.Contain("uncalibrated"));
    }

    [Test]
    public void PollOnce_identical_warning_is_refreshed_only_after_fifteen_minutes()
    {
        var logger = new RecordingLogger();
        long now = 0;
        using var service = new RepoContextDrainForecastService(
            logger, new(TimeSpan.FromSeconds(240), Budget, true),
            new(DateTimeOffset.UtcNow, RepoContextDrainOutcome.Abandoned, Budget,
                TimeSpan.FromSeconds(180), 1000), () => 1000,
            timestamp: () => now);

        service.PollOnce();
        now = 14 * 60 * Stopwatch.Frequency;
        service.PollOnce();
        Assert.That(logger.Lines, Has.Count.EqualTo(1));
        now = 15 * 60 * Stopwatch.Frequency;
        service.PollOnce();
        Assert.That(logger.Lines, Has.Count.EqualTo(2),
            "a persistent risk must remain visible without logging the same warning every ten seconds");
    }

    [Test]
    public void PollOnce_fixed_overhead_is_not_multiplied_by_the_resident_set()
    {
        var logger = new RecordingLogger();
        var last = Clean(1000, 94) with { CompletedDrains = [Clean(3750, 110)] };
        using var service = new RepoContextDrainForecastService(
            logger, new(TimeSpan.FromSeconds(240), Budget, true), last, () => 3750);

        var projection = service.PollOnce();

        Assert.That(projection, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(projection!.Value.ProjectedDrain.TotalSeconds, Is.EqualTo(132).Within(0.001));
            Assert.That(projection.Value.ExceedsBudget, Is.False);
            Assert.That(logger.Lines.Single().Level, Is.EqualTo(LogLevel.Warning), "132s leaves thin headroom");
            Assert.That(logger.Lines.Single().Message, Does.Contain("60s unwind reserve"));
            Assert.That(logger.Lines.Single().Message, Does.Not.Contain("expected to be abandoned"));
        });
    }

    [Test]
    public void PollOnce_material_growth_and_thin_threshold_are_reported_before_refresh()
    {
        var logger = new RecordingLogger();
        var resident = 1000;
        var last = Clean(4000, 120) with { CompletedDrains = [Clean(1000, 30)] };
        using var service = new RepoContextDrainForecastService(
            logger, new(TimeSpan.FromSeconds(240), Budget, true), last, () => resident);

        service.PollOnce();
        resident = 1300;
        service.PollOnce();
        resident = 1310;
        service.PollOnce();
        resident = 3500;
        service.PollOnce();
        resident = 4000;
        service.PollOnce();

        Assert.That(logger.Lines, Has.Count.EqualTo(3));
        Assert.That(logger.Lines.Select(line => line.Level),
            Is.EqualTo(new[] { LogLevel.Information, LogLevel.Information, LogLevel.Warning }));
    }

    [Test]
    public void TryProject_upper_percentile_residual_and_margin_cover_noisy_samples()
    {
        var last = Clean(4000, 100) with
        {
            CompletedDrains = [Clean(1000, 90), Clean(2000, 110), Clean(3000, 95)],
        };
        var forecast = RepoContextDrainForecast.Evaluate(last, Budget);

        Assert.That(forecast.TryProject(2000, out var projection), Is.True);
        Assert.That(projection.ProjectedDrain.TotalSeconds, Is.GreaterThanOrEqualTo(132),
            "the upper residual must retain the slow 110s sample plus its 20% safety margin");
    }

    [Test]
    public void TryProject_same_residency_only_and_extrapolation_are_uncalibrated()
    {
        var same = Clean(1000, 94) with { CompletedDrains = [Clean(1000, 90)] };
        Assert.That(RepoContextDrainForecast.Evaluate(same, Budget).TryProject(1000, out _), Is.False);
        var last = Clean(3750, 110) with { CompletedDrains = [Clean(1000, 94)] };
        Assert.That(RepoContextDrainForecast.Evaluate(last, Budget).TryProject(3751, out _), Is.False);
    }

    [TestCase(RepoContextDrainOutcome.Started)]
    [TestCase(RepoContextDrainOutcome.Abandoned)]
    public void TryProject_failed_last_drain_is_never_a_clean_calibration_sample(RepoContextDrainOutcome outcome)
    {
        var last = new RepoContextDrainObservation(DateTimeOffset.UtcNow, outcome, Budget,
            outcome == RepoContextDrainOutcome.Started ? null : TimeSpan.FromSeconds(180),
            3750, 1000, [Clean(1000, 94), Clean(3750, 110)]);
        var forecast = RepoContextDrainForecast.Evaluate(last, Budget);

        var available = forecast.TryProject(3750, out var projection);
        Assert.That(available, Is.EqualTo(outcome == RepoContextDrainOutcome.Abandoned));
        if (available)
        {
            Assert.That(projection.IsLowerBound, Is.True);
            Assert.That(projection.ProjectedDrain, Is.EqualTo(Budget));
        }
    }

    [Test]
    public void TryWrite_clean_samples_survive_start_abandonment_and_restart_without_promoting_failures()
    {
        var directory = Path.Combine(Path.GetTempPath(), "drain-service-rate-" + Guid.NewGuid().ToString("N"));
        var path = RepoContextDrainHistory.PathIn(directory);
        try
        {
            Assert.That(RepoContextDrainHistory.TryWrite(path, Clean(1000, 94)), Is.True);
            var start = new RepoContextDrainObservation(DateTimeOffset.UtcNow,
                RepoContextDrainOutcome.Started, Budget, null, 3750);
            Assert.That(RepoContextDrainHistory.TryWrite(path, start), Is.True);
            var killed = RepoContextDrainHistory.Read(path)!.Value;
            Assert.That(killed.Outcome, Is.EqualTo(RepoContextDrainOutcome.Started));
            Assert.That(killed.CompletedDrains, Has.Count.EqualTo(1));
            Assert.That(RepoContextDrainHistory.TryWrite(path,
                start with { Outcome = RepoContextDrainOutcome.Abandoned, Duration = Budget, StrandedActivations = 1000 }), Is.True);
            Assert.That(RepoContextDrainHistory.TryWrite(path, Clean(3750, 110)), Is.True);
            var read = RepoContextDrainHistory.Read(path)!.Value;
            Assert.That(read.CompletedDrains, Has.Count.EqualTo(1));
            Assert.That(read.CompletedDrains!.All(sample => sample.Outcome == RepoContextDrainOutcome.Completed), Is.True);

            using var service = new RepoContextDrainForecastService(new RecordingLogger(),
                new(TimeSpan.FromSeconds(240), Budget, true), read, () => 3750);
            Assert.That(service.PollOnce()!.Value.ProjectedDrain.TotalSeconds, Is.EqualTo(132).Within(0.001),
                "the same persisted history read by the next host must calibrate its production poll");
            Assert.That(File.Exists(path + ".tmp"), Is.False);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Test]
    public void TryWrite_history_is_bounded_and_repeated_terminal_records_do_not_duplicate_samples()
    {
        var directory = Path.Combine(Path.GetTempPath(), "drain-service-rate-" + Guid.NewGuid().ToString("N"));
        var path = RepoContextDrainHistory.PathIn(directory);
        try
        {
            for (var i = 0; i < 25; i++)
            {
                var observation = Clean(1000 + i, 90 + i) with { ObservedAtUtc = DateTimeOffset.UnixEpoch.AddDays(i) };
                Assert.That(RepoContextDrainHistory.TryWrite(path, observation), Is.True);
                Assert.That(RepoContextDrainHistory.TryWrite(path, observation), Is.True);
            }

            var read = RepoContextDrainHistory.Read(path)!.Value;
            Assert.That(read.CompletedDrains, Has.Count.EqualTo(16));
            Assert.That(read.CompletedDrains!.Select(sample => sample.ObservedAtUtc).Distinct().Count(), Is.EqualTo(16));
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Test]
    public void Parse_render_preserves_samples_but_legacy_missing_or_incomplete_evidence_stays_unknown()
    {
        var last = Clean(3750, 110) with { CompletedDrains = [Clean(1000, 94)] };
        var parsed = RepoContextDrainHistory.Parse(RepoContextDrainHistory.Render(last).Split('\n'))!.Value;
        Assert.That(parsed.CompletedDrains!.Single().ResidentActivations, Is.EqualTo(1000));
        var legacy = last with { CompletedDrains = null };
        Assert.That(RepoContextDrainHistory.Parse(RepoContextDrainHistory.Render(legacy).Split('\n'))!.Value.CompletedDrains, Is.Null);
        foreach (var missing in new[] { last with { Duration = null }, last with { ResidentActivations = null } })
        {
            Assert.That(RepoContextDrainForecast.Evaluate(missing, Budget).TryProject(1000, out _), Is.False);
        }
    }

    private static RepoContextDrainObservation Clean(int resident, double seconds)
        => new(DateTimeOffset.UnixEpoch.AddSeconds(resident), RepoContextDrainOutcome.Completed,
            Budget, TimeSpan.FromSeconds(seconds), resident);

    [Test]
    public void TryProject_calibrated_poll_has_no_managed_allocation()
    {
        var forecast = RepoContextDrainForecast.Evaluate(
            Clean(1000, 94) with { CompletedDrains = [Clean(3750, 110)] }, Budget);
        for (var i = 0; i < 100; i++)
        {
            forecast.TryProject(3750, out _);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        var available = true;
        for (var i = 0; i < 1000; i++)
        {
            available &= forecast.TryProject(3750, out _);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.That(available, Is.True);
        Assert.That(allocated, Is.Zero, "bounded stack spans must not introduce per-poll heap work");
    }

    private sealed class RecordingLogger : ILogger<RepoContextDrainForecastService>
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add((logLevel, formatter(state, exception)));
    }
}
