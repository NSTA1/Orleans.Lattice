using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// The saturation-instant announcement (issue #2519). A ceiling refusal is permanent
/// for the life of the process, and the drop counters report it as a level, which
/// cannot say when it began. These tests pin the transition record: exactly one
/// warning per ceiling (per family for the per-family ceiling), carrying the ceiling,
/// its configured value, the family, the series counts and the instant it happened.
/// </summary>
public sealed partial class RepoContextMetricsCollectorTests
{
    /// <summary>
    /// The regression test. Two series are admitted at one instant, the clock moves,
    /// and the third first-seen series is refused: the record must carry THAT
    /// instant, not the admission instant nor any later one, and a flood of further
    /// refusals of the same ceiling must not add a second record.
    /// </summary>
    [Test]
    public void The_first_per_family_refusal_logs_one_warning_stamped_with_the_instant_of_saturation()
    {
        var clock = new AdvanceableTimeProvider();
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 2, timeProvider: clock);
        var logger = new RecordingLogger();
        collector.AttachLogger(logger);
        using var meter = new Meter("orleans.lattice.probe.announce." + Guid.NewGuid().ToString("N"));
        var counter = meter.CreateCounter<long>("orleans.lattice.probe.announce.saturating");
        const string family = "orleans_lattice_probe_announce_saturating_total";

        counter.Add(1, new KeyValuePair<string, object?>("id", 0));
        counter.Add(1, new KeyValuePair<string, object?>("id", 1));
        var beforeSaturation = logger.For(family).Count;

        clock.Advance(TimeSpan.FromMinutes(7));
        var saturatedAt = clock.GetUtcNow();
        counter.Add(1, new KeyValuePair<string, object?>("id", 2));

        clock.Advance(TimeSpan.FromHours(2));
        for (var i = 3; i < 200; i++)
        {
            counter.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        var records = logger.For(family);
        var payload = collector.Render();

        Assert.Multiple(() =>
        {
            Assert.That(beforeSaturation, Is.Zero,
                "admitting series up to the ceiling is not a refusal, so nothing may be announced yet");
            Assert.That(SampleLines(payload, family), Has.Count.EqualTo(2),
                "precondition: the family really was held at its ceiling");
            Assert.That(records, Has.Count.EqualTo(1),
                "197 refusals of one ceiling must produce exactly one record, not one per refusal");
        });

        var record = records.Single();
        Assert.Multiple(() =>
        {
            Assert.That(record.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(record.EventId, Is.EqualTo(RepoContextMetricsCollector.CeilingReachedEvent));
            Assert.That(record.Field("Ceiling"), Is.EqualTo(RepoContextMetricsCollector.FamilyCeilingLabel));
            Assert.That(record.Field("Limit"), Is.EqualTo(2));
            Assert.That(record.Field("FamilySeries"), Is.EqualTo(2L));
            Assert.That((long)record.Field("TotalSeries")!, Is.GreaterThanOrEqualTo(2L),
                "the collector-wide count includes this family's two series");
            Assert.That(record.Field("SaturatedAtUtc"), Is.EqualTo(saturatedAt),
                "the record must carry the instant the first refusal happened");
            Assert.That(record.Message, Does.Contain("IN THIS FAMILY"));
            Assert.That(record.Message, Does.Contain(RepoContextMetricsCollector.DroppedByFamilyCounterName));
        });
    }

    /// <summary>
    /// The per-family ceiling is announced per family: a second family crossing the
    /// same ceiling gets its own record, and neither family gets a second one.
    /// </summary>
    [Test]
    public void Each_family_announces_its_own_per_family_ceiling_exactly_once()
    {
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 1);
        var logger = new RecordingLogger();
        collector.AttachLogger(logger);
        using var meter = new Meter("orleans.lattice.probe.announce." + Guid.NewGuid().ToString("N"));
        var first = meter.CreateCounter<long>("orleans.lattice.probe.announce.first");
        var second = meter.CreateCounter<long>("orleans.lattice.probe.announce.second");

        for (var i = 0; i < 10; i++)
        {
            first.Add(1, new KeyValuePair<string, object?>("id", i));
            second.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        Assert.Multiple(() =>
        {
            Assert.That(logger.For("orleans_lattice_probe_announce_first_total"), Has.Count.EqualTo(1));
            Assert.That(logger.For("orleans_lattice_probe_announce_second_total"), Has.Count.EqualTo(1));
        });
    }

    /// <summary>
    /// The global backstop is announced once for the whole process, not once per
    /// family it refuses.
    /// </summary>
    [Test]
    public void The_global_backstop_announces_once_however_many_families_it_refuses()
    {
        var clock = new AdvanceableTimeProvider();
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 1000, maxSeries: 1, timeProvider: clock);
        var logger = new RecordingLogger();
        collector.AttachLogger(logger);
        using var meter = new Meter("orleans.lattice.probe.announce." + Guid.NewGuid().ToString("N"));
        var admitted = meter.CreateCounter<long>("orleans.lattice.probe.announce.admitted");
        var refused = meter.CreateCounter<long>("orleans.lattice.probe.announce.refused");

        admitted.Add(1);
        clock.Advance(TimeSpan.FromSeconds(30));
        var saturatedAt = clock.GetUtcNow();
        refused.Add(1, new KeyValuePair<string, object?>("id", 0));
        clock.Advance(TimeSpan.FromSeconds(30));
        for (var i = 1; i < 20; i++)
        {
            admitted.Add(1, new KeyValuePair<string, object?>("id", i));
            refused.Add(1, new KeyValuePair<string, object?>("id", i));
        }

        var global = logger.Records
            .Where(r => Equals(r.Field("Ceiling"), RepoContextMetricsCollector.GlobalCeilingLabel))
            .ToList();

        Assert.That(global, Has.Count.EqualTo(1),
            "38 backstop refusals across two families must produce exactly one global record");
        var record = global[0];
        Assert.Multiple(() =>
        {
            Assert.That(record.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(record.EventId, Is.EqualTo(RepoContextMetricsCollector.CeilingReachedEvent));
            Assert.That(record.Field("Limit"), Is.EqualTo(1));
            Assert.That(record.Field("TotalSeries"), Is.EqualTo(1L));
            Assert.That(record.Field("Family"), Is.EqualTo("orleans_lattice_probe_announce_refused_total"),
                "the record names the family whose admission first hit the backstop");
            Assert.That(record.Field("FamilySeries"), Is.EqualTo(0L));
            Assert.That(record.Field("SaturatedAtUtc"), Is.EqualTo(saturatedAt));
            Assert.That(record.Message, Does.Contain("IN EVERY FAMILY"));
            Assert.That(logger.Records.Where(r => Equals(r.Field("Ceiling"), RepoContextMetricsCollector.FamilyCeilingLabel)),
                Is.Empty, "the per-family ceiling of 1000 was never reached, so it must not be announced");
        });
    }

    /// <summary>
    /// The host builds the collector before any logger exists. A crossing in that
    /// window must still be announced when the logger arrives, stamped with the
    /// instant it happened rather than the instant logging became possible, and
    /// attaching a second logger must not announce it again.
    /// </summary>
    [Test]
    public void A_crossing_before_a_logger_is_attached_is_announced_on_attach_with_its_original_instant()
    {
        var clock = new AdvanceableTimeProvider();
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 1, timeProvider: clock);
        using var meter = new Meter("orleans.lattice.probe.announce." + Guid.NewGuid().ToString("N"));
        var counter = meter.CreateCounter<long>("orleans.lattice.probe.announce.early");
        const string family = "orleans_lattice_probe_announce_early_total";

        counter.Add(1, new KeyValuePair<string, object?>("id", 0));
        clock.Advance(TimeSpan.FromMinutes(1));
        var saturatedAt = clock.GetUtcNow();
        counter.Add(1, new KeyValuePair<string, object?>("id", 1));
        counter.Add(1, new KeyValuePair<string, object?>("id", 2));

        clock.Advance(TimeSpan.FromMinutes(55));
        var logger = new RecordingLogger();
        collector.AttachLogger(logger);

        var replacement = new RecordingLogger();
        collector.AttachLogger(replacement);
        counter.Add(1, new KeyValuePair<string, object?>("id", 3));

        Assert.Multiple(() =>
        {
            Assert.That(logger.For(family), Has.Count.EqualTo(1),
                "the crossing captured before attachment must be written once the logger exists");
            Assert.That(logger.For(family).Single().Field("SaturatedAtUtc"), Is.EqualTo(saturatedAt),
                "a deferred record must state when the ceiling was reached, not when it was written");
            Assert.That(replacement.For(family), Is.Empty,
                "a crossing already written is never written again, to a new logger or on a later refusal");
        });
    }

    /// <summary>The negative control: a family that stays under its ceiling announces nothing.</summary>
    [Test]
    public void A_family_below_its_ceiling_announces_nothing()
    {
        using var collector = new RepoContextMetricsCollector(maxSeriesPerFamily: 5);
        var logger = new RecordingLogger();
        collector.AttachLogger(logger);
        using var meter = new Meter("orleans.lattice.probe.announce." + Guid.NewGuid().ToString("N"));
        var counter = meter.CreateCounter<long>("orleans.lattice.probe.announce.bounded");

        for (var i = 0; i < 50; i++)
        {
            counter.Add(1, new KeyValuePair<string, object?>("id", i % 5));
        }

        Assert.Multiple(() =>
        {
            Assert.That(SampleLines(collector.Render(), "orleans_lattice_probe_announce_bounded_total"),
                Has.Count.EqualTo(5), "precondition: the family filled its ceiling exactly, without exceeding it");
            Assert.That(logger.For("orleans_lattice_probe_announce_bounded_total"), Is.Empty);
        });
    }

    [Test]
    public void AttachLogger_rejects_a_null_logger()
    {
        using var collector = new RepoContextMetricsCollector();

        Assert.Throws<ArgumentNullException>(() => collector.AttachLogger(null!));
    }

    [Test]
    public void CeilingReachedEvent_is_a_stable_named_event()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextMetricsCollector.CeilingReachedEvent.Id, Is.EqualTo(1));
            Assert.That(RepoContextMetricsCollector.CeilingReachedEvent.Name, Is.EqualTo("MetricsCeilingReached"));
        });

    /// <summary>A logger that keeps each record's structured fields, not only its text.</summary>
    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<Record> _records = new();

        public IReadOnlyList<Record> Records => _records.ToArray();

        public IReadOnlyList<Record> For(string family)
            => Records.Where(r => Equals(r.Field("Family"), family)).ToList();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var fields = state as IReadOnlyList<KeyValuePair<string, object?>> ?? [];
            _records.Enqueue(new Record(logLevel, eventId, formatter(state, exception), fields.ToArray()));
        }
    }

    /// <summary>One recorded log entry and its structured fields.</summary>
    private sealed record Record(
        LogLevel Level,
        EventId EventId,
        string Message,
        IReadOnlyList<KeyValuePair<string, object?>> Fields)
    {
        public object? Field(string name)
            => Fields.FirstOrDefault(f => string.Equals(f.Key, name, StringComparison.Ordinal)).Value;
    }
}
