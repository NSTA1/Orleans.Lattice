using System.Diagnostics.Metrics;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextBackupMeter"/>, which exports the protection of the
/// durable agent-memory tree onto the container's scrape endpoint.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2640 records that <c>/metrics</c> carried no backup series of any kind
/// while every capture threw, so no alert rule could have been written. These tests
/// defend the two properties that make the export worth having.
/// </para>
/// <para>
/// <b>The series must carry the right value, not merely exist.</b> A sibling issue
/// on this bucket shipped a latency instrument that passed its whole suite with every
/// duration mutated to zero, because the assertions checked publication rather than
/// content. Every test here asserts an exact number, so a state gauge wired to a
/// constant fails.
/// </para>
/// <para>
/// <b>The series must exist before anything has happened.</b> An instrument created
/// on the first failure is absent during exactly the window an alert is meant to
/// cover, and the collector's series cap (issue #2480) can refuse a series first
/// created late. So a freshly built meter that has captured nothing is asserted to
/// report a real value already.
/// </para>
/// </remarks>
// NonParallelizable: a MeterListener is process-wide, so it observes instruments
// published by any concurrently running fixture. A sibling constructing the same meter
// publishes the same names, which would let a foreign measurement land on an arm this
// fixture asserts an exact value for.
[TestFixture]
[NonParallelizable]
public sealed class RepoContextBackupMeterTests
{
    private const string Tree = RepoContextHostTrees.Memory;

    private static RepoContextBackupStatus Enabled() => new(enabled: true, scopedTreeId: Tree);

    private static void Capture(RepoContextBackupStatus status, int entryCount, string id)
        => status.RecordCapture(
            backupId: id,
            capturedTreeId: Tree,
            entryCount: entryCount,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

    [Test]
    public void Every_instrument_reports_a_value_before_anything_has_been_captured()
    {
        var status = Enabled();
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        observed.Sample();

        // The from-startup requirement. Nothing has succeeded and nothing has failed,
        // and every series still reads, because an alert cannot fire on a series that
        // only appears once the thing it is watching for has happened.
        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Value(RepoContextBackupMeter.StateGaugeName),
                Is.EqualTo((double)(int)RepoContextBackupState.NeverCaptured),
                "A container that has captured nothing must report that state, not silence.");
            Assert.That(observed.Value(RepoContextBackupMeter.CapturesCounterName), Is.Zero);
            Assert.That(observed.Value(RepoContextBackupMeter.LastFullEntriesGaugeName), Is.Zero);
            Assert.That(
                observed.Value(RepoContextBackupMeter.SinkBackupsGaugeName),
                Is.EqualTo(-1d),
                "An unenumerated sink reports -1, which is a different fact from an empty sink's 0.");
            Assert.That(observed.Value(RepoContextBackupMeter.IncrementalFallbacksCounterName), Is.Zero);
        });
    }

    [Test]
    public void The_state_gauge_tracks_the_derived_state_rather_than_a_constant()
    {
        var status = Enabled();
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        observed.Sample();
        var beforeCapture = observed.Value(RepoContextBackupMeter.StateGaugeName);

        Capture(status, entryCount: 412, id: "b-1");
        observed.Sample();
        var afterCapture = observed.Value(RepoContextBackupMeter.StateGaugeName);

        status.RecordFailure("sink unreachable");
        observed.Sample();
        var afterFailure = observed.Value(RepoContextBackupMeter.StateGaugeName);

        // Three exact values, and each is the ordinal of a different state. A gauge
        // wired to any constant fails all three; one wired to a boolean fails the
        // third, because a failure after a success is not the same reading as a
        // container that never captured.
        Assert.Multiple(() =>
        {
            Assert.That(
                beforeCapture,
                Is.EqualTo((double)(int)RepoContextBackupState.NeverCaptured));
            Assert.That(
                afterCapture,
                Is.EqualTo((double)(int)RepoContextBackupState.Protected));
            Assert.That(
                afterFailure,
                Is.EqualTo((double)(int)RepoContextBackupState.FailingAfterCapture));
        });
    }

    [Test]
    public void A_disabled_host_reports_the_disabled_state_as_a_value()
    {
        var status = new RepoContextBackupStatus(enabled: false, scopedTreeId: Tree);
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        observed.Sample();

        // The meter is constructed unconditionally, so the deployment with no sink -
        // the one where nothing at all is protected - reports 0 rather than reporting
        // nothing. An absent series would be indistinguishable from a healthy one.
        Assert.That(
            observed.Value(RepoContextBackupMeter.StateGaugeName),
            Is.EqualTo((double)(int)RepoContextBackupState.Disabled));
    }

    [Test]
    public void The_capture_and_entry_series_carry_the_counts_they_claim_to()
    {
        var status = Enabled();
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        Capture(status, entryCount: 412, id: "b-1");
        Capture(status, entryCount: 37, id: "b-2");
        status.RecordSinkInventory(9, "b-2", DateTimeOffset.UnixEpoch);

        observed.Sample();

        // Exact numbers, all different from each other and from zero, so a series
        // reporting a constant or reading the wrong field cannot pass.
        Assert.Multiple(() =>
        {
            Assert.That(observed.Value(RepoContextBackupMeter.CapturesCounterName), Is.EqualTo(2d));
            Assert.That(
                observed.Value(RepoContextBackupMeter.LastFullEntriesGaugeName),
                Is.EqualTo(37d));
            Assert.That(observed.Value(RepoContextBackupMeter.SinkBackupsGaugeName), Is.EqualTo(9d));
        });
    }

    [Test]
    public void An_empty_capture_is_reported_as_a_distinct_state_beside_a_zero_entry_count()
    {
        var status = Enabled();
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        Capture(status, entryCount: 0, id: "b-1");
        observed.Sample();

        // The capture succeeded, so the capture counter rises; what makes the zero
        // readable is that the state gauge does not say Protected.
        Assert.Multiple(() =>
        {
            Assert.That(observed.Value(RepoContextBackupMeter.CapturesCounterName), Is.EqualTo(1d));
            Assert.That(observed.Value(RepoContextBackupMeter.LastFullEntriesGaugeName), Is.Zero);
            Assert.That(
                observed.Value(RepoContextBackupMeter.StateGaugeName),
                Is.EqualTo((double)(int)RepoContextBackupState.CapturedNothing));
        });
    }

    [Test]
    public void The_incremental_fallback_series_counts_silent_promotions()
    {
        var status = Enabled();
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        status.RecordCapture(
            backupId: "b-1",
            capturedTreeId: Tree,
            entryCount: 5,
            isFull: true,
            requestedIncremental: true,
            capturedAtUtc: DateTimeOffset.UnixEpoch);

        observed.Sample();

        // A promoted incremental succeeds and the data is safe, so no failure signal
        // fires; the cost is only visible if it is counted.
        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Value(RepoContextBackupMeter.IncrementalFallbacksCounterName),
                Is.EqualTo(1d));
            Assert.That(
                observed.Value(RepoContextBackupMeter.StateGaugeName),
                Is.EqualTo((double)(int)RepoContextBackupState.Protected));
        });
    }

    [Test]
    public void The_instruments_are_published_on_the_meter_the_collector_scrapes()
    {
        var status = Enabled();
        using var observed = new BackupMeterObserver();
        using var meter = new RepoContextBackupMeter(status);

        observed.Sample();

        // The observer only enables instruments on the host meter, so a non-empty
        // reading is itself the assertion that the meter name is the scraped one. A
        // series published on an unsubscribed meter would reach no endpoint.
        Assert.That(
            observed.Names,
            Is.EquivalentTo(new[]
            {
                RepoContextBackupMeter.StateGaugeName,
                RepoContextBackupMeter.CapturesCounterName,
                RepoContextBackupMeter.LastFullEntriesGaugeName,
                RepoContextBackupMeter.SinkBackupsGaugeName,
                RepoContextBackupMeter.IncrementalFallbacksCounterName,
            }));
    }

    /// <summary>
    /// Observes the backup instruments on the host meter, recording the most recent
    /// sampled value of each.
    /// </summary>
    /// <remarks>
    /// Presence is tracked apart from value: "this series reads zero" and "this
    /// series was never published" are the two readings this instrument exists to
    /// keep apart, so an absent key throws rather than defaulting to zero.
    /// </remarks>
    private sealed class BackupMeterObserver : IDisposable
    {
        private static readonly string[] Wanted =
        [
            RepoContextBackupMeter.StateGaugeName,
            RepoContextBackupMeter.CapturesCounterName,
            RepoContextBackupMeter.LastFullEntriesGaugeName,
            RepoContextBackupMeter.SinkBackupsGaugeName,
            RepoContextBackupMeter.IncrementalFallbacksCounterName,
        ];

        private readonly Dictionary<string, double> _values = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public BackupMeterObserver()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextDrainForecastService.MeterName
                    && Wanted.Contains(instrument.Name, StringComparer.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) => Record(instrument, measurement));
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) => Record(instrument, measurement));
            _listener.SetMeasurementEventCallback<double>((instrument, measurement, _, _) => Record(instrument, measurement));
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
        public double Value(string name)
        {
            lock (_values)
            {
                return _values.TryGetValue(name, out var value)
                    ? value
                    : throw new AssertionException(
                        $"The instrument '{name}' reported no measurement. An absent series is not a "
                        + "reading of zero, and treating it as one is the failure under test.");
            }
        }

        public void Dispose() => _listener.Dispose();

        private void Record(Instrument instrument, double measurement)
        {
            lock (_values)
            {
                _values[instrument.Name] = measurement;
            }
        }
    }
}
