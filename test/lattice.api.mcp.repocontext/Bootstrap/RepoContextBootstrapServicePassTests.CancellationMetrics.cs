using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Coverage for the mid-phase cancellation instrumentation on
/// <see cref="RepoContextBootstrapService"/>.
/// <para>
/// Before this instrumentation existed, a run cancelled mid-phase computed the
/// elapsed time it was discarding, wrote it into a log line, and dropped it. On
/// the acceptance rig two such cancellations threw away 208,190 ms and
/// 1,848,818 ms - just over half an hour between them - while every exported
/// series sat still.
/// </para>
/// <para>
/// Every assertion here drives a real cancellation through a real pass and
/// reads a real measurement. None assert on a tag set alone, which the
/// constructor's zero-prime would satisfy with the recording arm deleted.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    private const string PhaseCancelledInstrument = "repocontext.bootstrap.phase_cancelled";
    private const string PhaseCancelledDiscardedInstrument =
        "repocontext.bootstrap.phase_cancelled.discarded_time";

    private sealed record CancellationMeasurement(string Instrument, long Value, string? Phase);

    /// <summary>
    /// Starts a listener over the two cancellation instruments, matched by meter
    /// name and instrument name.
    /// <para>
    /// The <c>MeterListening</c> helpers take the meter or instrument by
    /// reference, which is what forces a static initialiser to complete before a
    /// listener exists. This package's meters are per-instance fields assigned in
    /// a constructor, not statics, so that hazard cannot arise here and there is
    /// no reference to pass: the service under test owns a private
    /// <see cref="Meter"/> created inside its own constructor, which is also
    /// where the zero-prime happens. Matching by name is therefore the only way
    /// to have a listener running before the instrument exists, which the
    /// zero-prime test requires.
    /// </para>
    /// </summary>
    private static MeterListener ListenForCancellations(List<CancellationMeasurement> sink)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (!string.Equals(instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal))
                {
                    return;
                }

                if (instrument.Name is PhaseCancelledInstrument or PhaseCancelledDiscardedInstrument)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            string? phase = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, "phase", StringComparison.Ordinal))
                {
                    phase = tag.Value?.ToString();
                }
            }

            lock (sink)
            {
                sink.Add(new CancellationMeasurement(instrument.Name, value, phase));
            }
        });

        listener.Start();
        return listener;
    }

    private static List<CancellationMeasurement> Snapshot(List<CancellationMeasurement> sink)
    {
        lock (sink)
        {
            return [.. sink];
        }
    }

    /// <summary>
    /// Runs one pass that is cancelled from inside the vector ingestor - the
    /// exact shape of the two cancellations observed on the rig, which both
    /// landed mid-Vectorising. The ingest hook idles for a measurable interval
    /// first so the discarded duration is a real reading rather than a zero that
    /// happens to be indistinguishable from an unrecorded one.
    /// </summary>
    private async Task<List<CancellationMeasurement>> RunCancelledMidVectorisingAsync(int idleMilliseconds = 25)
    {
        var sink = new List<CancellationMeasurement>();
        using var listener = ListenForCancellations(sink);
        using var cts = new CancellationTokenSource();

        _harness.WriteFile("src/a.cs", "class A { }");
        _harness.OnIngest = async (_, _) =>
        {
            await Task.Delay(idleMilliseconds, CancellationToken.None);
            await cts.CancelAsync();
            cts.Token.ThrowIfCancellationRequested();
            return 0;
        };

        Assert.That(
            async () => await _harness.Service.RunAsync(_harness.Request(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "The run must still propagate the cancellation; instrumenting it does not swallow it.");

        return Snapshot(sink);
    }

    // ------------------------------------------------------------------ counting

    [Test]
    public async Task A_run_cancelled_mid_phase_counts_the_cancellation()
    {
        // The load-bearing test. It drives a genuine cancellation through a
        // genuine pass and asserts the counter moved - not that a label exists,
        // which the constructor's zero-prime alone would satisfy.
        var measurements = await RunCancelledMidVectorisingAsync();

        var counted = measurements
            .Where(m => m.Instrument == PhaseCancelledInstrument && m.Value > 0)
            .ToList();

        Assert.That(counted, Has.Count.EqualTo(1));
        Assert.That(counted[0].Value, Is.EqualTo(1));
    }

    [Test]
    public async Task The_cancellation_is_tagged_with_the_phase_the_run_was_executing()
    {
        // "Cancelled" alone does not say what was lost. A walk abandoned after a
        // second and a vectorising pass abandoned after half an hour are the same
        // event without this tag.
        var measurements = await RunCancelledMidVectorisingAsync();

        var counted = measurements.Single(m => m.Instrument == PhaseCancelledInstrument && m.Value > 0);

        Assert.That(counted.Phase, Is.EqualTo(nameof(RepoIndexPhase.Vectorising)));
    }

    [Test]
    public async Task The_discarded_run_time_is_recorded_and_is_a_real_measurement()
    {
        // The operative half. The elapsed milliseconds were already computed at
        // this site and then dropped, so the deployment had no way to answer
        // "how much work has this thrown away" - which is precisely the epic's
        // definition-of-done question.
        const int idle = 60;

        var measurements = await RunCancelledMidVectorisingAsync(idle);

        var discarded = measurements
            .Where(m => m.Instrument == PhaseCancelledDiscardedInstrument && m.Value > 0)
            .ToList();

        Assert.That(discarded, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(discarded[0].Phase, Is.EqualTo(nameof(RepoIndexPhase.Vectorising)));
            Assert.That(discarded[0].Value, Is.GreaterThanOrEqualTo(idle / 2),
                "The recorded duration must track the run's real elapsed time, not a constant.");
        });
    }

    [Test]
    public async Task A_pass_that_completes_records_no_cancellation()
    {
        // The negative control. An arm that recorded unconditionally would pass
        // every other test in this fixture.
        var sink = new List<CancellationMeasurement>();
        using var listener = ListenForCancellations(sink);

        _harness.WriteFile("src/a.cs", "class A { }");
        await _harness.Service.RunAsync(_harness.Request());

        Assert.That(
            Snapshot(sink).Where(m => m.Value > 0),
            Is.Empty,
            "A run that finished discarded nothing.");
    }

    // ------------------------------------------------------------- zero-priming

    [Test]
    public void Both_cancellation_series_are_primed_at_zero_for_every_cancellable_phase()
    {
        // Without this, a deployment that has never cancelled a run exports
        // nothing at all for either instrument, and an operator cannot tell
        // "no work discarded" from "instrument never wired" - which is the exact
        // defect this issue fixes. The listener is started before the service is
        // constructed because the prime happens in the constructor.
        var sink = new List<CancellationMeasurement>();
        using var listener = ListenForCancellations(sink);

        using var harness = new BootstrapHarness();

        var primed = Snapshot(sink);
        var expectedPhases = new[]
        {
            nameof(RepoIndexPhase.Walking),
            nameof(RepoIndexPhase.Reconciling),
            nameof(RepoIndexPhase.Applying),
            nameof(RepoIndexPhase.Vectorising),
        };

        Assert.Multiple(() =>
        {
            Assert.That(primed, Is.Not.Empty, "The instruments must exist before anything is cancelled.");
            Assert.That(primed.All(m => m.Value == 0), Is.True, "A prime must not perturb the value.");

            foreach (var instrument in new[] { PhaseCancelledInstrument, PhaseCancelledDiscardedInstrument })
            {
                var phases = primed
                    .Where(m => m.Instrument == instrument)
                    .Select(m => m.Phase)
                    .ToList();
                Assert.That(phases, Is.EquivalentTo(expectedPhases),
                    $"{instrument} must prime exactly the phases a run can be cancelled in.");
            }
        });
    }

    [Test]
    public async Task The_primed_series_carries_the_same_tags_a_later_cancellation_does()
    {
        // A prime whose tag set differs from the recording's is worse than no
        // prime: it exports a permanent zero beside a series that appears from
        // nowhere on the first cancellation.
        var sink = new List<CancellationMeasurement>();
        using var listener = ListenForCancellations(sink);
        using var cts = new CancellationTokenSource();

        using var harness = new BootstrapHarness();
        harness.WriteFile("src/a.cs", "class A { }");
        harness.OnIngest = async (_, _) =>
        {
            await cts.CancelAsync();
            cts.Token.ThrowIfCancellationRequested();
            return 0;
        };

        try
        {
            await harness.Service.RunAsync(harness.Request(), cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected; the measurements are what this test is about.
        }

        var measurements = Snapshot(sink)
            .Where(m => m.Instrument == PhaseCancelledInstrument)
            .ToList();

        var prime = measurements.First(m => m.Value == 0 && m.Phase == nameof(RepoIndexPhase.Vectorising));
        var recorded = measurements.First(m => m.Value > 0);

        Assert.That(recorded.Phase, Is.EqualTo(prime.Phase));
    }
}
