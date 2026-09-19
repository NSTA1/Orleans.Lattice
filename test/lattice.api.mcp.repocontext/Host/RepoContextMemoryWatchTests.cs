using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the self-reported memory requirement added for issue #3255: the high-water
/// mark this run actually reached against the ceiling it was granted, the managed
/// exhaustion it observed, and the record it leaves for the next process.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is being pinned is that this measures rather than predicts.</b> The
/// container's memory grant is currently a deploy-time absolute fitted to one host,
/// and nothing afterwards reports what the corpus turned out to need. These tests
/// drive the watch with substituted readings and assert that what comes out is the
/// worst pair it was shown - not a modelled figure, and not the latest sample.
/// </para>
/// <para>
/// <see cref="NonParallelizableAttribute"/> because the watch subscribes to
/// <see cref="AppDomain.FirstChanceException"/> for the whole process, so a test that
/// throws an <see cref="OutOfMemoryException"/> would otherwise be counted by a watch
/// belonging to a test running beside it.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextMemoryWatchTests
{
    private const long NineGiB = 9_663_676_416;

    private static readonly DateTimeOffset Observed = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private List<RepoContextMemoryObservation> _written = null!;

    [SetUp]
    public void SetUp() => _written = [];

    private RepoContextMemoryWatch CreateWatch(
        Func<RepoContextHeapCeiling> read,
        RepoContextMemoryObservation? previous = null,
        RepoContextMemoryAdmissionDecision decision = default,
        bool writeSucceeds = true) =>
        new(
            NullLogger<RepoContextMemoryWatch>.Instance,
            decision,
            previous,
            observation =>
            {
                _written.Add(observation);
                return writeSucceeds;
            },
            read,
            () => Observed,
            // Never fires: every test drives Sample() explicitly, so nothing here
            // depends on wall-clock timing.
            Timeout.InfiniteTimeSpan);

    private static RepoContextHeapCeiling Ceiling(long limit, long committed) =>
        new(limit, committed, limit);

    [Test]
    public async Task The_peak_is_a_high_water_mark_and_not_the_latest_reading()
    {
        // An instantaneous gauge already exists. The reason this type is here at all
        // is that the peak happens during ingest, so a scrape landing after it
        // reports a comfortable number that says nothing about the margin actually
        // consumed.
        var committed = 1_000L;
        using var watch = CreateWatch(() => Ceiling(NineGiB, committed));

        await watch.StartAsync(CancellationToken.None);

        committed = 9_000L;
        watch.Sample();
        committed = 2_000L;
        watch.Sample();

        Assert.That(watch.Current.PeakCommittedBytes, Is.EqualTo(9_000L));
    }

    [Test]
    public async Task Peak_occupancy_is_the_peak_over_the_granted_ceiling()
    {
        using var watch = CreateWatch(() => Ceiling(1_000, 750));

        await watch.StartAsync(CancellationToken.None);

        Assert.That(watch.Current.PeakOccupancyRatio, Is.EqualTo(0.75d));
    }

    [Test]
    public async Task Peak_occupancy_is_absent_rather_than_infinite_when_no_ceiling_is_reported()
    {
        using var watch = CreateWatch(() => Ceiling(0, 750));

        await watch.StartAsync(CancellationToken.None);

        Assert.That(watch.Current.PeakOccupancyRatio, Is.Null);
    }

    [Test]
    public async Task Starting_records_an_admitted_marker_before_any_work_is_accepted()
    {
        // Written up front on purpose. If it were written only at stop, a process
        // killed outright would leave no trace at all, and the next start could not
        // tell "never ran" from "ran and was killed".
        using var watch = CreateWatch(() => Ceiling(NineGiB, 1_000));

        await watch.StartAsync(CancellationToken.None);

        Assert.That(_written, Has.Count.EqualTo(1));
        Assert.That(_written[0].Outcome, Is.EqualTo(RepoContextMemoryOutcome.Admitted));
    }

    [Test]
    public async Task A_clean_stop_with_no_exhaustion_records_completed()
    {
        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000));

        await watch.StartAsync(CancellationToken.None);
        await watch.StopAsync(CancellationToken.None);

        Assert.That(_written[^1].Outcome, Is.EqualTo(RepoContextMemoryOutcome.Completed));
    }

    [Test]
    public async Task A_caught_out_of_memory_exception_is_counted()
    {
        // The documented failure is CAUGHT: waves of OutOfMemoryException inside a
        // grain-state read, which Orleans reports as a STORAGE fault. Nothing
        // terminates, so an unhandled-exception hook would see none of it. This is
        // the property that makes a first-chance handler the only workable seam.
        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000));

        await watch.StartAsync(CancellationToken.None);

        try
        {
            throw new OutOfMemoryException("simulated");
        }
        catch (OutOfMemoryException)
        {
            // Swallowed exactly as the real failure is swallowed.
        }

        Assert.That(watch.Current.ExhaustionEvents, Is.EqualTo(1));
    }

    [Test]
    public async Task Exhaustion_is_recorded_with_the_ceiling_it_happened_at()
    {
        // The ceiling is the whole point: a refusal compares a grant against this
        // number, so an exhaustion recorded without one can never refuse anything.
        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000));

        await watch.StartAsync(CancellationToken.None);
        ThrowAndSwallowOutOfMemory();
        await watch.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_written[^1].Outcome, Is.EqualTo(RepoContextMemoryOutcome.Exhausted));
            Assert.That(_written[^1].ExhaustedAtLimitBytes, Is.EqualTo(NineGiB));
            Assert.That(_written[^1].ExhaustionEvents, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_exhausted_run_that_then_stops_cleanly_still_records_exhausted()
    {
        // How a run stopped does not unmake what it measured. If a clean stop
        // overwrote the outcome, every crash-looping container that happened to be
        // stopped by hand would erase its own evidence.
        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000));

        await watch.StartAsync(CancellationToken.None);
        ThrowAndSwallowOutOfMemory();
        await watch.StopAsync(CancellationToken.None);

        Assert.That(_written[^1].Outcome, Is.EqualTo(RepoContextMemoryOutcome.Exhausted));
    }

    [Test]
    public async Task A_successful_run_carries_a_previous_exhaustion_ceiling_forward()
    {
        // Without this, raising the grant erases the proof that the smaller one
        // failed, and dropping back to it would start happily into a configuration
        // already measured not to work.
        var previous = new RepoContextMemoryObservation(
            Observed,
            RepoContextMemoryOutcome.Exhausted,
            NineGiB,
            NineGiB,
            304,
            NineGiB,
            null);

        using var watch = CreateWatch(() => Ceiling(14_495_514_624, 5_000), previous);

        await watch.StartAsync(CancellationToken.None);
        await watch.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_written[^1].Outcome, Is.EqualTo(RepoContextMemoryOutcome.Completed));
            Assert.That(_written[^1].ExhaustedAtLimitBytes, Is.EqualTo(NineGiB));
        });
    }

    [Test]
    public async Task An_honoured_override_is_recorded_so_a_later_reader_knows_the_evidence_was_disbelieved()
    {
        var decision = new RepoContextMemoryAdmissionDecision(
            RepoContextMemoryVerdict.Warn,
            "overridden",
            NineGiB);

        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000), decision: decision);

        await watch.StartAsync(CancellationToken.None);

        Assert.That(_written[0].OverriddenAtLimitBytes, Is.EqualTo(NineGiB));
    }

    [Test]
    public async Task A_failing_write_does_not_throw()
    {
        // Failing to record a diagnostic must never become a failure to run. This is
        // called on the startup path, so a throw here would be the admission check
        // causing the outage it exists to prevent.
        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000), writeSucceeds: false);

        Assert.DoesNotThrowAsync(async () =>
        {
            await watch.StartAsync(CancellationToken.None);
            await watch.StopAsync(CancellationToken.None);
        });
    }

    [Test]
    public async Task A_failing_reading_does_not_fail_host_startup_or_shutdown()
    {
        // This is a hosted service, so an exception escaping StartAsync fails host
        // startup - which would make the diagnostic that exists to explain an
        // unexplained failure to start into a cause of one. The stop path is guarded
        // for the same reason plus one more: it runs inside the stop grace period the
        // drain also has to fit into.
        using var watch = CreateWatch(() => throw new InvalidOperationException("runtime unavailable"));

        Assert.DoesNotThrowAsync(async () =>
        {
            await watch.StartAsync(CancellationToken.None);
            await watch.StopAsync(CancellationToken.None);
        });

        await Task.CompletedTask;
    }

    [Test]
    public async Task Stopping_unsubscribes_so_a_later_exception_is_not_counted()
    {
        // The handler runs on every throw process-wide. Leaving it attached after a
        // stop would both leak and attribute another component's exceptions to a
        // watch that is no longer measuring anything.
        using var watch = CreateWatch(() => Ceiling(NineGiB, 5_000));

        await watch.StartAsync(CancellationToken.None);
        await watch.StopAsync(CancellationToken.None);

        ThrowAndSwallowOutOfMemory();

        Assert.That(watch.Current.ExhaustionEvents, Is.Zero);
    }

    [Test]
    public async Task The_meter_publishes_every_instrument_it_declares()
    {
        using var watch = CreateWatch(() => Ceiling(1_000, 750));
        await watch.StartAsync(CancellationToken.None);

        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => watch.Current, () => NineGiB);

        observer.Sample();

        Assert.That(
            observer.Names,
            Is.EquivalentTo(new[]
            {
                RepoContextMemoryWatchMeter.PeakCommittedBytesGaugeName,
                RepoContextMemoryWatchMeter.PeakOccupancyRatioGaugeName,
                RepoContextMemoryWatchMeter.ExhaustionEventsCounterName,
                RepoContextMemoryWatchMeter.InsufficientLimitBytesGaugeName,
            }));
    }

    [Test]
    public async Task The_published_peak_and_occupancy_are_what_the_watch_measured()
    {
        using var watch = CreateWatch(() => Ceiling(1_000, 750));
        await watch.StartAsync(CancellationToken.None);

        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => watch.Current, () => null);

        observer.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observer.Value(RepoContextMemoryWatchMeter.PeakCommittedBytesGaugeName),
                Is.EqualTo(750d));
            Assert.That(
                observer.Value(RepoContextMemoryWatchMeter.PeakOccupancyRatioGaugeName),
                Is.EqualTo(0.75d));
        });
    }

    [Test]
    public void The_recorded_exhaustion_ceiling_reports_nothing_at_all_when_none_is_recorded()
    {
        // The one instrument here that declines to report. A zero would read as "this
        // deployment exhausted at a ceiling of zero bytes", which no grant is smaller
        // than - so an operator would see a refusal threshold that is met by
        // everything, and the series would be worse than absent.
        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => default, () => null);

        observer.Sample();

        Assert.That(
            observer.Names,
            Does.Not.Contain(RepoContextMemoryWatchMeter.InsufficientLimitBytesGaugeName));
    }

    [Test]
    public void The_recorded_exhaustion_ceiling_reports_the_number_an_override_must_be_set_to()
    {
        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => default, () => NineGiB);

        observer.Sample();

        Assert.That(
            observer.Value(RepoContextMemoryWatchMeter.InsufficientLimitBytesGaugeName),
            Is.EqualTo((double)NineGiB));
    }

    [Test]
    public async Task The_exhaustion_counter_reads_zero_rather_than_being_absent_when_nothing_has_failed()
    {
        // Observable, so it is published from process start with a real value. That is
        // what makes "zero" and "never published" distinguishable here without any
        // zero-priming: an absent series can only mean the meter was not constructed.
        using var watch = CreateWatch(() => Ceiling(1_000, 750));
        await watch.StartAsync(CancellationToken.None);

        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => watch.Current, () => null);

        observer.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(
                observer.Names,
                Does.Contain(RepoContextMemoryWatchMeter.ExhaustionEventsCounterName));
            Assert.That(
                observer.Value(RepoContextMemoryWatchMeter.ExhaustionEventsCounterName),
                Is.Zero);
        });
    }

    [Test]
    public void Every_instrument_description_states_what_a_zero_on_it_means()
    {
        // The recurring failure in this epic is an instrument whose silence reads as a
        // measured zero. The description is the only place an operator reading a
        // scrape can learn which they are looking at.
        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => default, () => NineGiB);

        observer.Sample();

        Assert.Multiple(() =>
        {
            foreach (var name in observer.Names)
            {
                Assert.That(
                    observer.Description(name),
                    Does.Contain("ero").Or.Contain("NO measurement"),
                    name + " must say what an absent or zero reading means");
            }
        });
    }

    [Test]
    public void The_exhaustion_counter_description_refuses_to_claim_it_sees_cgroup_kills()
    {
        // The epic has repeatedly been bitten by an instrument whose scope excludes
        // the thing it is read as covering, reporting CLEAN where it should report
        // NOTHING. A cgroup out-of-memory kill raises no managed exception, so this
        // counter cannot see one, and the description must say so on the scrape
        // rather than only in the source.
        using var observer = new WatchMeterObserver();
        using var meter = new RepoContextMemoryWatchMeter(() => default, () => null);

        observer.Sample();

        var description = observer.Description(RepoContextMemoryWatchMeter.ExhaustionEventsCounterName);

        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain("SIGKILL"));
            Assert.That(description, Does.Contain("OOMKilled"));
        });
    }

    private static void ThrowAndSwallowOutOfMemory()
    {
        try
        {
            throw new OutOfMemoryException("simulated");
        }
        catch (OutOfMemoryException)
        {
            // Exactly as the real failure is swallowed, inside a grain-state read.
        }
    }

    /// <summary>
    /// Observes the watch meter's instruments, tracking presence separately from
    /// value because "reads zero" and "was never published" are the two readings
    /// these instruments exist to keep apart.
    /// </summary>
    private sealed class WatchMeterObserver : IDisposable
    {
        private static readonly string[] Wanted =
        [
            RepoContextMemoryWatchMeter.PeakCommittedBytesGaugeName,
            RepoContextMemoryWatchMeter.PeakOccupancyRatioGaugeName,
            RepoContextMemoryWatchMeter.ExhaustionEventsCounterName,
            RepoContextMemoryWatchMeter.InsufficientLimitBytesGaugeName,
        ];

        private readonly Dictionary<string, double> _values = new(StringComparer.Ordinal);
        private readonly Dictionary<string, string?> _descriptions = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public WatchMeterObserver()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextHostMeter.Name
                    && Wanted.Contains(instrument.Name, StringComparer.Ordinal))
                {
                    lock (_descriptions)
                    {
                        _descriptions[instrument.Name] = instrument.Description;
                    }

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

        /// <summary>The instrument names that have reported a measurement at all.</summary>
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

        /// <summary>The description one instrument was published with.</summary>
        public string? Description(string instrumentName)
        {
            lock (_descriptions)
            {
                return _descriptions.GetValueOrDefault(instrumentName);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
