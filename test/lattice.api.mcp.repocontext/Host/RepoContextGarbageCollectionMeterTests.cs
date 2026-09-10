using System.Diagnostics.Metrics;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextGarbageCollectionMeter"/>, which promotes the
/// accumulated garbage-collector pause total from a figure reported once at startup
/// into a queryable series.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2605. The startup report names the measurand but cannot carry a useful
/// number, because at that moment the figure is necessarily near zero. What was
/// missing is the ability to ask how much wall-clock the process spent suspended
/// over a window, to alert on it, and to correlate it with request latency. This
/// host runs a multi-GiB heap by design, so a collector pause is a first-class
/// explanation for a request timeout.
/// </para>
/// <para>
/// The property these tests exist to defend is not that the instrument is present.
/// A measurand that is present, populated, and structurally incapable of
/// discriminating what it was added for is the exact failure this reliability
/// bucket kept hitting. What they defend is that a <b>zero pause reading is
/// readable</b>: the collection count is published beside it as a denominator, so a
/// zero on pause seconds beside a rising collection count is a measured absence of
/// pause, while both at zero means no collection has happened yet. Without the
/// denominator those two are the same observation.
/// </para>
/// </remarks>
// NonParallelizable: a MeterListener is process-wide, so it observes every instrument
// published by any concurrently running fixture, not only the one under test. A sibling
// fixture constructing the same reporter publishes the same instrument names on the same
// meter, which both inflates a capture count and lets a foreign measurement land on an arm
// this fixture asserts is still zero. Isolating the fixture is what makes the readings here
// observations of the code under test rather than of whatever else happened to be running.
[TestFixture]
[NonParallelizable]
public sealed class RepoContextGarbageCollectionMeterTests
{
    [Test]
    public void Both_instruments_are_published_on_the_host_meter()
    {
        using var observed = new HostMeterObserver();
        using var meter = new RepoContextGarbageCollectionMeter(
            () => TimeSpan.FromSeconds(3), () => 11);

        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Names,
                Does.Contain(RepoContextGarbageCollectionMeter.PauseSecondsCounterName),
                "The pause total has to reach the exposition, or it stays a figure in a log line that "
                + "nobody can query over a window.");
            Assert.That(
                observed.Names,
                Does.Contain(RepoContextGarbageCollectionMeter.CollectionsCounterName),
                "The collection count is not optional colour. It is the denominator that makes a zero on "
                + "the pause total mean something, and shipping the pause total without it would ship a "
                + "series whose most likely reading cannot be interpreted.");
            Assert.That(
                observed.Names,
                Has.Count.EqualTo(2),
                "Control: exactly the two instruments under test were observed. A listener that saw "
                + "nothing would satisfy neither assertion above, but one that saw everything on every "
                + "meter in the process would satisfy both for the wrong reason.");
        });
    }

    [Test]
    public void The_pause_total_is_reported_in_seconds_from_the_runtime_figure()
    {
        using var observed = new HostMeterObserver();
        using var meter = new RepoContextGarbageCollectionMeter(
            () => TimeSpan.FromMilliseconds(2_500), () => 4);

        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Value(RepoContextGarbageCollectionMeter.PauseSecondsCounterName),
                Is.EqualTo(2.5).Within(1e-9),
                "The unit is seconds, declared as seconds, and read from a TimeSpan. A millisecond figure "
                + "published under a seconds unit would be wrong by three orders of magnitude in the "
                + "direction that makes a pause problem look like no problem.");
            Assert.That(
                observed.Value(RepoContextGarbageCollectionMeter.CollectionsCounterName),
                Is.EqualTo(4d));
        });
    }

    [Test]
    public void A_zero_pause_total_is_distinguishable_from_a_collector_that_has_not_run()
    {
        using var measuredAbsence = new HostMeterObserver();
        using (new RepoContextGarbageCollectionMeter(() => TimeSpan.Zero, () => 42))
        {
            measuredAbsence.Sample();
        }

        using var notYetRun = new HostMeterObserver();
        using (new RepoContextGarbageCollectionMeter(() => TimeSpan.Zero, () => 0))
        {
            notYetRun.Sample();
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                measuredAbsence.Value(RepoContextGarbageCollectionMeter.PauseSecondsCounterName),
                Is.Zero);
            Assert.That(
                measuredAbsence.Value(RepoContextGarbageCollectionMeter.CollectionsCounterName),
                Is.EqualTo(42d),
                "Forty-two collections and no measurable pause is a measured absence of pause: the "
                + "collector demonstrably ran and demonstrably did not suspend this process for long "
                + "enough to register.");
            Assert.That(
                notYetRun.Value(RepoContextGarbageCollectionMeter.CollectionsCounterName),
                Is.Zero,
                "No collections and no pause is a different fact entirely, and the only thing that "
                + "separates the two readings is this denominator. Both series read zero on the pause "
                + "total, so the pause total alone cannot tell them apart.");
        });
    }

    [Test]
    public void Both_series_are_sampled_on_every_scrape_rather_than_appearing_on_a_first_occurrence()
    {
        var pause = TimeSpan.Zero;
        long collections = 0;

        using var observed = new HostMeterObserver();
        using var meter = new RepoContextGarbageCollectionMeter(() => pause, () => collections);

        observed.Sample();
        var firstPause = observed.Value(RepoContextGarbageCollectionMeter.PauseSecondsCounterName);
        var firstNames = observed.Names.Count;

        pause = TimeSpan.FromSeconds(9);
        collections = 3;
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                firstNames,
                Is.EqualTo(2),
                "Both series exist on the very first scrape, while the readings are still zero. That is "
                + "what makes an absent series mean 'the host did not construct this meter, or the "
                + "collector refused it at a ceiling' rather than 'the process has not paused' - the same "
                + "property the repository-context counters get from pre-minting (issue #2515).");
            Assert.That(firstPause, Is.Zero);
            Assert.That(
                observed.Value(RepoContextGarbageCollectionMeter.PauseSecondsCounterName),
                Is.EqualTo(9d).Within(1e-9),
                "Control: the observer really does re-read the source on each scrape, so the zero above "
                + "was an observation rather than a listener that had stopped reporting.");
            Assert.That(
                observed.Value(RepoContextGarbageCollectionMeter.CollectionsCounterName),
                Is.EqualTo(3d));
        });
    }

    [Test]
    public void The_host_constructs_the_meter_eagerly_rather_than_registering_a_lazy_factory()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var builder = Path.Combine(
            repoRoot, "apps", "repocontext", "Hosting", "RepoContextHostBuilder.cs");

        Assert.That(
            File.Exists(builder),
            Is.True,
            "The host builder was not found, so this guard inspected nothing.");

        var text = File.ReadAllText(builder);

        Assert.Multiple(() =>
        {
            Assert.That(
                text,
                Does.Contain("new RepoContextGarbageCollectionMeter()"),
                "The meter has to be constructed during registration, next to the metrics collector and "
                + "for the same reason. An observable instrument that nobody resolves is never published, "
                + "so a singleton registered behind a factory would leave the source containing a "
                + "measurand that the exposition never carries - present in the code, absent from the "
                + "scrape, and silently readable as zero pause.");
            Assert.That(
                text,
                Does.Contain("new RepoContextMetricsCollector()"),
                "Control: this guard is matching on the real registration block. If the collector's own "
                + "eager construction is no longer here, the file has been restructured and the assertion "
                + "above is pinned to a shape that no longer exists.");
        });
    }

    /// <summary>
    /// Observes the two garbage-collection instruments on the host meter, recording
    /// the most recent sampled value of each.
    /// </summary>
    /// <remarks>
    /// Presence is tracked separately from value, because "this series reads zero"
    /// and "this series was never published" are the two readings the instrument
    /// under test exists to keep apart, and folding an absent key into a default of
    /// zero would erase exactly that distinction inside the harness.
    /// </remarks>
    private sealed class HostMeterObserver : IDisposable
    {
        private static readonly string[] Wanted =
        [
            RepoContextGarbageCollectionMeter.PauseSecondsCounterName,
            RepoContextGarbageCollectionMeter.CollectionsCounterName,
        ];

        private readonly Dictionary<string, double> _values = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public HostMeterObserver()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextDrainForecastService.MeterName
                    && Wanted.Contains(instrument.Name, StringComparer.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>((instrument, measurement, _, _) =>
            {
                lock (_values)
                {
                    _values[instrument.Name] = measurement;
                }
            });
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
            {
                lock (_values)
                {
                    _values[instrument.Name] = measurement;
                }
            });
            _listener.Start();
        }

        /// <summary>The instrument names that have reported at all.</summary>
        public IReadOnlyCollection<string> Names
        {
            get
            {
                lock (_values)
                {
                    return _values.Keys.ToArray();
                }
            }
        }

        /// <summary>Forces one observation of every observable instrument.</summary>
        public void Sample() => _listener.RecordObservableInstruments();

        /// <summary>The most recent value reported by one instrument.</summary>
        public double Value(string instrumentName)
        {
            lock (_values)
            {
                return _values.GetValueOrDefault(instrumentName);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
