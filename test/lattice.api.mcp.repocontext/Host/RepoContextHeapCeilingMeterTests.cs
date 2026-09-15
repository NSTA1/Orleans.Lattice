using System.Diagnostics.Metrics;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextHeapCeilingMeter"/>, which exports the memory
/// ceiling this process is held to and the commitment measured against it.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2543. Subscribing the BCL runtime meter supplies what the process is
/// <i>using</i>; nothing supplies what it may use. Two fixes merged on this epic
/// (#2765, #2767) claim heap-ceiling adherence as their primary effect, and with no
/// ceiling series that claim was not checkable from inside the container at all -
/// it had to be inferred from <c>docker stats</c>, which carries one aggregate
/// number, no attribution and no history.
/// </para>
/// <para>
/// The property these tests defend is not that three gauges exist. It is that the
/// ceiling is <b>read from the runtime at scrape time</b> rather than captured, and
/// that the three readings are wired to the fields they claim, because a limit
/// crossed with a commitment silently inverts the adherence ratio and every value on
/// the endpoint still looks plausible.
/// </para>
/// </remarks>
// NonParallelizable for the same reason the garbage-collection fixture is: a
// MeterListener is process-wide, so a sibling fixture publishing the same instrument
// names on the same host meter would land foreign measurements on the arms asserted
// here.
[TestFixture]
[NonParallelizable]
public sealed class RepoContextHeapCeilingMeterTests
{
    [Test]
    public void All_three_instruments_are_published_on_the_host_meter()
    {
        using var observed = new HostMeterObserver();
        using var meter = new RepoContextHeapCeilingMeter(
            () => new RepoContextHeapCeiling(12_884_901_888, 7_516_192_768, 11_596_411_699));

        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Names,
                Does.Contain(RepoContextHeapCeilingMeter.LimitBytesGaugeName),
                "the ceiling is the series the epic is missing: without it, 'adherence to the heap "
                + "limit' has no denominator and stays an inference drawn from outside the container.");
            Assert.That(
                observed.Names,
                Does.Contain(RepoContextHeapCeilingMeter.CommittedBytesGaugeName),
                "a hard limit is enforced against committed memory, so this is the numerator. A "
                + "ceiling with no commitment beside it is as unreadable as a pause total with no "
                + "collection count.");
            Assert.That(
                observed.Names,
                Does.Contain(RepoContextHeapCeilingMeter.HighLoadThresholdBytesGaugeName));
            Assert.That(
                observed.Names,
                Has.Count.EqualTo(3),
                "control: exactly the three instruments under test were observed. A listener that saw "
                + "nothing would satisfy none of the above, but one that saw every instrument in the "
                + "process would satisfy all three for the wrong reason.");
        });
    }

    /// <summary>
    /// Each gauge must carry the field it claims. Three distinct values, so a
    /// crossed pair fails rather than passing on a coincidence.
    /// </summary>
    [Test]
    public void Each_gauge_reports_the_runtime_field_it_names()
    {
        using var observed = new HostMeterObserver();
        using var meter = new RepoContextHeapCeilingMeter(
            () => new RepoContextHeapCeiling(
                LimitBytes: 12_884_901_888,
                CommittedBytes: 7_516_192_768,
                HighLoadThresholdBytes: 11_596_411_699));

        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName),
                Is.EqualTo(12_884_901_888d),
                "reported in bytes, unscaled. A limit published in mebibytes under a byte unit would "
                + "read as a container a thousand times smaller than it is, in the direction that makes "
                + "a healthy process look like it is over its ceiling.");
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.CommittedBytesGaugeName),
                Is.EqualTo(7_516_192_768d));
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.HighLoadThresholdBytesGaugeName),
                Is.EqualTo(11_596_411_699d));
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.CommittedBytesGaugeName)
                    / observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName),
                Is.EqualTo(0.583d).Within(0.001d),
                "the reading the epic actually wants: ceiling adherence as a number. Crossing the two "
                + "fields would invert it to 1.71 while every individual value still looked plausible.");
        });
    }

    /// <summary>
    /// The ceiling is whatever the runtime reports at the moment of the scrape, so
    /// a deployment that changes its memory grant is followed without a restart and
    /// without a configuration change.
    /// </summary>
    [Test]
    public void The_ceiling_is_read_at_scrape_time_rather_than_captured_at_construction()
    {
        var reading = new RepoContextHeapCeiling(12_884_901_888, 1_000, 11_596_411_699);

        using var observed = new HostMeterObserver();
        using var meter = new RepoContextHeapCeilingMeter(() => reading);

        observed.Sample();
        var first = observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName);

        reading = new RepoContextHeapCeiling(25_769_803_776, 2_000, 23_192_823_398);
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(12_884_901_888d));
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName),
                Is.EqualTo(25_769_803_776d),
                "a ceiling captured once at construction would still read the first value here, and "
                + "would keep reading it for the life of the process after a memory grant changed under "
                + "it - a stale number that looks exactly like a measured one.");
        });
    }

    /// <summary>
    /// All three series exist on the first scrape while the readings are still zero,
    /// so an absent series means the host did not construct this meter rather than
    /// that memory is unbounded.
    /// </summary>
    [Test]
    public void All_three_series_exist_on_the_first_scrape_while_the_readings_are_zero()
    {
        var reading = new RepoContextHeapCeiling(0, 0, 0);

        using var observed = new HostMeterObserver();
        using var meter = new RepoContextHeapCeilingMeter(() => reading);

        observed.Sample();
        var namesWhileZero = observed.Names.Count;

        reading = new RepoContextHeapCeiling(8, 4, 6);
        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                namesWhileZero,
                Is.EqualTo(3),
                "an absent series here has to mean 'the host did not construct this meter, or the "
                + "collector refused it at a ceiling'. If a zero reading suppressed the series, absence "
                + "would also mean 'no memory limit', and the two would be indistinguishable.");
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName),
                Is.EqualTo(8d),
                "control: the observer really does re-read the source on each scrape, so the zeros "
                + "above were observations rather than a listener that had stopped reporting.");
        });
    }

    /// <summary>
    /// The default reader is the runtime's own figures, and the limit is populated
    /// before any collection has run.
    /// </summary>
    /// <remarks>
    /// This is the premise the whole type rests on: if the limit only became
    /// readable after a collection, a freshly started container would export a zero
    /// ceiling and the adherence ratio would divide by it. Asserted against the real
    /// runtime rather than a substitute, because a substitute cannot establish it.
    /// </remarks>
    [Test]
    public void The_default_reader_takes_a_populated_ceiling_from_the_runtime()
    {
        using var observed = new HostMeterObserver();
        using var meter = new RepoContextHeapCeilingMeter();

        observed.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName),
                Is.GreaterThan(0d),
                "the runtime reports the memory it may use without waiting for a collection, which is "
                + "what lets a container that has just started export a usable ceiling.");
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.HighLoadThresholdBytesGaugeName),
                Is.GreaterThan(0d));
            Assert.That(
                observed.Value(RepoContextHeapCeilingMeter.HighLoadThresholdBytesGaugeName),
                Is.LessThanOrEqualTo(observed.Value(RepoContextHeapCeilingMeter.LimitBytesGaugeName)),
                "the pressure threshold sits at or below the ceiling by construction. A threshold above "
                + "the limit would mean the two were read from unrelated quantities.");
        });
    }

    /// <summary>
    /// Nothing in this type may know a byte count: the ceiling has to come from the
    /// runtime, or the container inherits one developer machine's memory (issue
    /// #2779).
    /// </summary>
    [Test]
    public void The_ceiling_is_derived_from_the_runtime_and_never_from_a_literal()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var source = Path.Combine(
            repoRoot, "apps", "repocontext", "Hosting", "RepoContextHeapCeilingMeter.cs");

        Assert.That(
            File.Exists(source),
            Is.True,
            "the meter source was not found, so this guard inspected nothing.");

        var text = File.ReadAllText(source);

        Assert.Multiple(() =>
        {
            Assert.That(
                text,
                Does.Contain("GC.GetGCMemoryInfo()"),
                "the ceiling has to be asked of the runtime. Any other source is a transcription of "
                + "whichever machine the author was on, and this image already carries a set of those.");
            Assert.That(
                text,
                Does.Contain("info.TotalAvailableMemoryBytes"),
                "TotalAvailableMemoryBytes is the field that resolves to whichever ceiling actually "
                + "binds - container limit, configured hard limit, or physical memory - so the same "
                + "code is correct on a 12 GiB container and on a 56 GiB developer box.");
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
                Does.Contain("var heapCeilingMeter = new RepoContextHeapCeilingMeter();"),
                "an observable instrument that nobody resolves is never published, so a singleton "
                + "registered behind a factory would leave the ceiling present in the source and absent "
                + "from every scrape. Matched on the eager assignment statement rather than on the "
                + "constructor call alone, because a lazy factory contains the constructor call too: "
                + "an arm that replaced this line with AddSingleton(_ => new RepoContextHeapCeilingMeter()) "
                + "stayed green against the looser match, which is the exact regression this guard names.");
            Assert.That(
                text,
                Does.Not.Contain("=> new RepoContextHeapCeilingMeter()"),
                "the deferred form, stated as its own arm so the failure names the defect rather than "
                + "reporting a missing line.");
            Assert.That(
                text,
                Does.Contain("new RepoContextMetricsCollector()"),
                "Control: this guard is matching on the real registration block. If the collector's own "
                + "eager construction is no longer here, the file has been restructured and the "
                + "assertion above is pinned to a shape that no longer exists.");
        });
    }

    /// <summary>
    /// Observes the three heap-ceiling instruments on the host meter, recording the
    /// most recent sampled value of each. Presence is tracked separately from value,
    /// because "reads zero" and "was never published" are the two readings the
    /// instruments exist to keep apart.
    /// </summary>
    private sealed class HostMeterObserver : IDisposable
    {
        private static readonly string[] Wanted =
        [
            RepoContextHeapCeilingMeter.LimitBytesGaugeName,
            RepoContextHeapCeilingMeter.CommittedBytesGaugeName,
            RepoContextHeapCeilingMeter.HighLoadThresholdBytesGaugeName,
        ];

        private readonly Dictionary<string, double> _values = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public HostMeterObserver()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextHostMeter.Name
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
