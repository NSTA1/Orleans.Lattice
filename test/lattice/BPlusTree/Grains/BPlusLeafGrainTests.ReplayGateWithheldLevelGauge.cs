using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the withheld-replay-permit level gauge (issue #2784).
/// <para>
/// <b>What was missing was a boundary, not a number.</b> The withheld level is
/// already derivable from the two counter arms as
/// <c>withheld - restored</c>, and both are zero-primed, so the arithmetic was
/// available on any scrape. What no series carried was the moment a process
/// restart <em>discarded</em> that level: both counters reset to zero with the
/// process, so a restart that threw away six withheld permits and a process that
/// had never withheld any render identically.
/// </para>
/// <para>
/// <b>Why a gauge rather than an emission at the sizing site.</b> The original
/// scope for this work asked the successor process to report "a restart
/// discarded N permits, with N". It cannot: <c>_withheldReplayPermits</c> is a
/// process-wide static that the terminating process took with it, and the only
/// seam that re-sizes a gate in a live process is
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.ResetReplayConcurrencyGateForTest"/>,
/// which is test-only. Such an emission would therefore fire with <c>N = 0</c>
/// on every production start and <c>N &gt; 0</c> only under a fixture - an
/// unreachable clause wearing the appearance of a safety net, and worse than
/// silence because a run would then read <c>discarded = 0</c> across a restart
/// that discarded plenty and score a fabrication as a measurement.
/// </para>
/// <para>
/// Reporting the level continuously moves the measurement from the successor,
/// which cannot know it, to the decedent, which does. <b>The last sample before
/// the gap is the level that was lost, and the step down to zero is the
/// discard.</b> Nothing is inferred and nothing is fabricated, and every sample
/// is taken on the ordinary production path rather than on one only a fixture
/// reaches.
/// </para>
/// <para>
/// Every test here mutates process-wide statics, so each restores the gate to
/// the state it found it in and is marked <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string WithheldPermitsGaugeName = "orleans.lattice.wal.replay.permits_withheld";

    private const string PermitAdaptationsCounterName = "orleans.lattice.wal.replay.permit_adaptations";

    [Test]
    [NonParallelizable]
    public async Task Withheld_permit_level_is_published_even_when_nothing_is_withheld()
    {
        // The "measured zero, not an absent series" property, and it is the one
        // that makes a restart legible. A gauge that only published once
        // something had been withheld would leave a restarted process reporting
        // nothing at all - indistinguishable from a build that does not carry
        // the instrument, which is precisely the ambiguity this epic keeps
        // paying for.
        await QuiescentReplayGateAsync();

        var samples = new List<int>();
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [WithheldPermitsGaugeName],
            l => l.SetMeasurementEventCallback<int>(
                (_, measurement, _, _) => samples.Add(measurement)));

        listener.RecordObservableInstruments();

        Assert.Multiple(() =>
        {
            Assert.That(samples, Is.Not.Empty,
                "the gauge must publish a sample with nothing withheld; an absent series here would "
                + "be read as 'this build does not carry the instrument', which is the one reading it "
                + "exists to rule out");
            Assert.That(samples, Has.All.Zero,
                "and that sample must be zero, because the gate is quiescent");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task Withheld_permit_gauge_reports_the_live_level_and_agrees_with_the_counter_arms()
    {
        // This is the pre-registered falsifier for the whole change, in the form
        // that can actually be checked. The plausible-looking version - "the
        // gauge reads zero while withheld_total is positive" - is VACUOUS,
        // because a balanced withhold/restore pair legitimately returns the level
        // to zero and the counters keep climbing. The falsifier only bites when
        // both sides are read at the SAME sample, which is what this test does:
        // the counter callbacks accumulate while the listener is live, and the
        // gauge is sampled from that same listener.
        var gate = await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
        Assert.That(ceiling, Is.GreaterThan(2),
            "this test needs room to withhold more than one permit and still sit above the floor");

        var withheldTotal = 0L;
        var restoredTotal = 0L;
        var gaugeSamples = new List<int>();

        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [WithheldPermitsGaugeName, PermitAdaptationsCounterName],
            l =>
            {
                l.SetMeasurementEventCallback<int>(
                    (_, measurement, _, _) => gaugeSamples.Add(measurement));
                l.SetMeasurementEventCallback<long>(
                    (_, measurement, tags, _) =>
                    {
                        foreach (var tag in tags)
                        {
                            if (tag.Key != LatticeMetrics.PermitAdaptationRestored.Key)
                                continue;

                            if (Equals(tag.Value, LatticeMetrics.PermitAdaptationRestored.Value))
                                restoredTotal += measurement;
                            else
                                withheldTotal += measurement;
                        }
                    });
            });

        var taken = 0;
        try
        {
            // Mirror the real discipline: a permit is only ever withheld by a
            // replay that already holds it.
            while (taken < 2
                && BPlusLeafGrain.TryWithholdReplayPermitOnPressure(
                    LatticeMetrics.PermitAdaptationTriggerFault))
            {
                Assert.That(gate.Wait(0), Is.True,
                    "withholding claimed a permit the gate could not supply");
                taken++;
            }

            Assert.That(taken, Is.EqualTo(2),
                "the fixture must have withheld two permits; a lower figure means the floor was hit "
                + "and the readings below would be measuring the wrong thing");

            gaugeSamples.Clear();
            listener.RecordObservableInstruments();

            // Input and scanned counts asserted non-zero BEFORE the identity is
            // read. Without this the identity below degenerates to 0 == 0 - 0,
            // which is satisfied by an instrument that was never published, by a
            // listener that observed nothing, and by a gauge hard-coded to zero.
            Assert.Multiple(() =>
            {
                Assert.That(gaugeSamples, Is.Not.Empty,
                    "the gauge must have been observed, or the identity below is vacuous");
                Assert.That(withheldTotal, Is.EqualTo(2L),
                    "the counter arm must have observed both withholdings, or the identity below is "
                    + "vacuous");
            });

            Assert.Multiple(() =>
            {
                Assert.That(gaugeSamples[0], Is.EqualTo(2),
                    "the gauge must report the live withheld level");
                Assert.That(gaugeSamples[0], Is.EqualTo((int)(withheldTotal - restoredTotal)),
                    "and it must equal withheld - restored read at the same sample; the level is "
                    + "definitionally that difference, so a divergence means the gauge is reading "
                    + "something other than the accounting the gate actually runs on");
            });

            // Now the half the naive falsifier cannot see: restore one permit and
            // confirm the gauge FALLS while both counter arms only ever rise.
            Assert.That(BPlusLeafGrain.TryRestoreWithheldReplayPermit(), Is.True,
                "a withheld permit must be restorable");
            gate.Release();
            taken--;

            gaugeSamples.Clear();
            listener.RecordObservableInstruments();

            Assert.Multiple(() =>
            {
                Assert.That(gaugeSamples, Is.Not.Empty,
                    "the gauge must have been observed after the restore");
                Assert.That(restoredTotal, Is.EqualTo(1L),
                    "the restored arm must have observed the restore");
                Assert.That(gaugeSamples[0], Is.EqualTo(1),
                    "the level must fall when a permit is returned, which is the behaviour no "
                    + "monotonic counter can show on its own");
                Assert.That(gaugeSamples[0], Is.EqualTo((int)(withheldTotal - restoredTotal)),
                    "and the same-sample identity must still hold after a restore, which is the case "
                    + "the vacuous form of this falsifier cannot distinguish");
            });
        }
        finally
        {
            while (taken > 0)
            {
                Assert.That(BPlusLeafGrain.TryRestoreWithheldReplayPermit(), Is.True,
                    "the fixture must be able to return every permit it withheld");
                gate.Release();
                taken--;
            }
        }
    }
}
