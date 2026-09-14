using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the three replay-gate visibility gauges of issue #3047: the
/// gate's ceiling, its available permits, and the activations queued on it.
/// <para>
/// <b>The gap these close is a structural one, not a missing number.</b> Every
/// pre-existing permit series records at an <em>outcome</em>. The queue-wait
/// histogram records once a wait has ended; the replay counter records once a
/// permit is held. Both recording sites sit downstream of
/// <c>gate.WaitAsync</c>, so an activation that is <b>still queued</b>
/// contributes to neither, and a permanently saturated gate is silent across the
/// whole surface: no waits, no replays, nothing. That renders identically to a
/// gate nothing ever asked for a permit. The condition and its own absence of
/// evidence are the same observation, which is the failure mode that let a
/// wedged corpus sit unexplained.
/// </para>
/// <para>
/// <b>The three are a set, and the set is what carries the meaning.</b> The
/// ceiling is the denominator: one permit withheld from sixteen is noise and one
/// withheld from two is half the silo's throughput, and those are the same
/// number on a scrape. Availability says whether the door is open. The queued
/// level says how many are waiting at it, which neither of the others can say,
/// because <see cref="SemaphoreSlim.CurrentCount"/> saturates at zero and so
/// reports the same figure for one waiter and for a thousand.
/// </para>
/// <para>
/// Every test here mutates process-wide statics, so each restores the gate to
/// the state it found it in and is marked <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string PermitCeilingGaugeName = "orleans.lattice.wal.replay.permit_ceiling";

    private const string AvailablePermitsGaugeName = "orleans.lattice.wal.replay.permits_available";

    private const string QueuedPermitsGaugeName = "orleans.lattice.wal.replay.permits_queued";

    /// <summary>
    /// Samples the three visibility gauges once and returns them by metric name.
    /// Asserts that all three were observed, so a caller's comparison can never
    /// degenerate into a claim about a gauge that never published.
    /// </summary>
    private static Dictionary<string, int> SampleReplayGateGauges()
    {
        var observed = new ConcurrentBag<(string Name, int Value)>();

        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [PermitCeilingGaugeName, AvailablePermitsGaugeName, QueuedPermitsGaugeName],
            l => l.SetMeasurementEventCallback<int>(
                (instrument, measurement, _, _) => observed.Add((instrument.Name, measurement))));

        listener.RecordObservableInstruments();

        var byName = observed
            .GroupBy(s => s.Name)
            .ToDictionary(g => g.Key, g => g.First().Value);

        Assert.Multiple(() =>
        {
            Assert.That(byName.ContainsKey(PermitCeilingGaugeName), Is.True,
                "the ceiling gauge must publish; an absent series would be read as 'this build does "
                + "not carry the instrument', which is the reading it exists to rule out");
            Assert.That(byName.ContainsKey(AvailablePermitsGaugeName), Is.True,
                "the availability gauge must publish");
            Assert.That(byName.ContainsKey(QueuedPermitsGaugeName), Is.True,
                "the queued gauge must publish");
        });

        return byName;
    }

    [Test]
    [NonParallelizable]
    public async Task Permit_ceiling_gauge_publishes_the_ceiling_the_gate_was_sized_to()
    {
        // The denominator. Without it the withheld and queued levels are absolute
        // counts with nothing to be absolute against, and "one withheld" reads
        // the same whether it cost a sixteenth of the silo's replay throughput or
        // half of it.
        await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;

        Assert.That(ceiling, Is.GreaterThan(0),
            "instrument validation: a sized gate must have a positive ceiling, or the comparison "
            + "below cannot tell a real reading from the unsized sentinel");

        var samples = SampleReplayGateGauges();

        Assert.That(samples[PermitCeilingGaugeName], Is.EqualTo(ceiling),
            "the gauge must report the ceiling the gate was actually sized to");
    }

    [Test]
    [NonParallelizable]
    public async Task Permit_ceiling_gauge_reads_zero_only_before_the_gate_has_been_sized()
    {
        // The sentinel that lets this series be published unconditionally.
        // ResolveGateSizing returns the configured value when positive and the
        // derived default otherwise, and both are at least one, so a SIZED gate
        // can never report zero. That is what makes a flat zero mean "no leaf has
        // activated on this silo" rather than "the gate admits nothing" - two
        // readings that would otherwise be identical and are operationally
        // opposite.
        await QuiescentReplayGateAsync();

        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var unsized = SampleReplayGateGauges();

            Assert.That(unsized[PermitCeilingGaugeName], Is.Zero,
                "an unsized gate must report the zero sentinel rather than an absent series");
        }
        finally
        {
            // Re-size, or every later test in this process starts against a gate
            // that no longer exists.
            await QuiescentReplayGateAsync();
        }

        var resized = SampleReplayGateGauges();

        Assert.That(resized[PermitCeilingGaugeName], Is.GreaterThan(0),
            "and once re-sized it must leave the sentinel, so the zero is reachable ONLY while "
            + "unsized. If this arm read zero too, the sentinel would be indistinguishable from a "
            + "gauge hard-coded to zero and the reading rule above would be worthless");
    }

    [Test]
    [NonParallelizable]
    public async Task Available_permit_gauge_reports_the_headroom_a_new_activation_would_find()
    {
        var gate = await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;
        var heldPermits = 0;

        try
        {
            var quiescent = SampleReplayGateGauges();
            Assert.That(quiescent[AvailablePermitsGaugeName], Is.EqualTo(ceiling),
                "a quiescent gate has every permit in circulation");

            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: the gate must have had permits to take, or the saturated "
                + "reading below is the same observation as the quiescent one");

            var saturated = SampleReplayGateGauges();
            Assert.That(saturated[AvailablePermitsGaugeName], Is.Zero,
                "a fully drained gate must report no headroom");
        }
        finally
        {
            gate.Release(heldPermits);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Available_permits_at_zero_is_separated_from_an_unsized_gate_only_by_the_ceiling()
    {
        // This pins the documented reading rule rather than a mechanism, and it
        // is the reason the two gauges were added together instead of either
        // alone. Availability reads zero under BOTH an unsized gate and a
        // saturated one. Those are opposite conditions - "nothing has ever
        // activated here" against "everything is stuck" - and an operator who
        // charts availability without the ceiling beside it will read the first
        // as the second. Only the ceiling separates them, so the pair is the
        // instrument and neither half is self-sufficient.
        var gate = await QuiescentReplayGateAsync();
        var heldPermits = 0;

        try
        {
            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: the gate must actually be saturated");

            var saturated = SampleReplayGateGauges();

            Assert.Multiple(() =>
            {
                Assert.That(saturated[AvailablePermitsGaugeName], Is.Zero,
                    "saturated: no headroom");
                Assert.That(saturated[PermitCeilingGaugeName], Is.GreaterThan(0),
                    "but the ceiling is positive, which is what marks this as a MEASURED "
                    + "saturation rather than a gate that was never built");
            });
        }
        finally
        {
            gate.Release(heldPermits);
        }

        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var unsized = SampleReplayGateGauges();

            Assert.Multiple(() =>
            {
                Assert.That(unsized[AvailablePermitsGaugeName], Is.Zero,
                    "unsized: availability reads zero here TOO, which is the whole hazard");
                Assert.That(unsized[PermitCeilingGaugeName], Is.Zero,
                    "and only the ceiling distinguishes it from the saturated reading above. If "
                    + "this were positive the two conditions would be byte-identical on a scrape");
            });
        }
        finally
        {
            await QuiescentReplayGateAsync();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Queued_gauge_counts_an_activation_that_is_still_waiting_for_a_permit()
    {
        // THE ARM THAT JUSTIFIES THE WHOLE CHANGE. Everything else here is
        // satisfied by a build that reports static properties of the gate. This
        // one observes the population that no terminal instrument can: an
        // activation measured WHILE it waits, rather than after it stops waiting.
        var gate = await QuiescentReplayGateAsync();
        var heldPermits = 0;
        Task? activation = null;

        try
        {
            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: with permits still available the activation would be "
                + "admitted rather than queued, and there would be nothing to observe");

            Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero,
                "instrument validation: nothing may be queued before this test queues something, "
                + "or the count below is inherited rather than measured");

            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null,
                persistedCheckpoint: 0,
                walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            activation = LeafActivationHarness.ActivateAsync((IGrainBase)grain, CancellationToken.None);

            // Let it reach the queue and park there.
            await Task.Delay(TimeSpan.FromMilliseconds(250));

            var queued = SampleReplayGateGauges();

            Assert.Multiple(() =>
            {
                Assert.That(queued[QueuedPermitsGaugeName], Is.EqualTo(1),
                    "the queued activation must be visible WHILE it is queued. This is the "
                    + "observation no terminal instrument can make, and a zero here is the wedged "
                    + "gate rendering as an idle one");
                Assert.That(queued[AvailablePermitsGaugeName], Is.Zero,
                    "read beside it: no headroom");
                Assert.That(queued[PermitCeilingGaugeName], Is.GreaterThan(0),
                    "and a positive ceiling, so the pair reads as a real saturation");
            });

            // Now admit it and confirm the level falls. A level that only ever
            // rises is a leak wearing the appearance of a backlog.
            gate.Release(heldPermits);
            heldPermits = 0;

            await activation.WaitAsync(TimeSpan.FromSeconds(30));
            activation = null;

            var drained = SampleReplayGateGauges();
            Assert.That(drained[QueuedPermitsGaugeName], Is.Zero,
                "and the level must fall once the wait ends in an acquisition");
        }
        finally
        {
            if (heldPermits > 0)
                gate.Release(heldPermits);

            if (activation is not null)
                await activation.WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Queued_gauge_decrements_when_the_wait_ends_in_cancellation()
    {
        // The asymmetric-leak arm, and the most expensive way this change could
        // go wrong. The decrement sits in a `finally` rather than beside the two
        // recording calls precisely so that it cannot be skipped by the exit that
        // does NOT run the acquired path. Were it one-sided, this counter - a
        // process-wide static that is never rebuilt - would drift upward for the
        // life of the silo and eventually report a large standing backlog on a
        // completely idle gate. That is a FALSE POSITIVE for the exact condition
        // the instrument exists to detect, so a one-sided decrement is strictly
        // worse than having no instrument at all.
        var gate = await QuiescentReplayGateAsync();
        var heldPermits = 0;

        try
        {
            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: the activation must be forced to queue, or no cancellation "
                + "can be observed on the queued path");

            var baseline = BPlusLeafGrain.QueuedReplayPermitWaitersForTest;
            Assert.That(baseline, Is.Zero,
                "instrument validation: the level must start at zero, or a non-zero reading after "
                + "the cancellation could be inherited rather than leaked");

            using var cts = new CancellationTokenSource();
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null,
                persistedCheckpoint: 0,
                walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            var activation = LeafActivationHarness.ActivateAsync((IGrainBase)grain, cts.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(250));

            var whileQueued = SampleReplayGateGauges();
            Assert.That(whileQueued[QueuedPermitsGaugeName], Is.EqualTo(1),
                "instrument validation: the activation must genuinely be queued before it is "
                + "cancelled, or the decrement below has nothing to decrement and this test would "
                + "pass against a build that never increments at all");

            await cts.CancelAsync();

            var completed = await Task.WhenAny(activation, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(completed, Is.SameAs(activation),
                "the cancelled activation never returned; a cancellation that cannot unwind is a "
                + "hang rather than a failure");
            Assert.That(async () => await activation, Throws.InstanceOf<OperationCanceledException>(),
                "a cancellation while queued must surface, not be swallowed");

            var afterCancel = SampleReplayGateGauges();

            Assert.That(afterCancel[QueuedPermitsGaugeName], Is.Zero,
                "the level MUST return to zero on the cancel path. A decrement that only runs on "
                + "acquisition leaks one count per cancelled activation, permanently, and presents "
                + "as a saturated gate that is in fact idle");
            Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(baseline),
                "and the underlying static must be back at its baseline, not merely near it");
        }
        finally
        {
            gate.Release(heldPermits);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Queued_gauge_is_the_only_series_that_shows_a_gate_admitting_nothing()
    {
        // The motivating claim, stated as a test rather than as prose in a doc
        // comment. While an activation is parked on the gate it has ended no
        // wait and started no replay, so the terminal instruments record NOTHING
        // - and a reader seeing nothing cannot tell a saturated gate from an idle
        // one. This arm asserts both halves at the same sample: the terminal arm
        // silent, the non-terminal arm reporting the waiter. If the terminal arm
        // ever starts recording mid-wait this test fails, which is correct: the
        // justification for the new instrument would have changed.
        var gate = await QuiescentReplayGateAsync();
        var heldPermits = 0;
        Task? activation = null;

        try
        {
            heldPermits = DrainEveryReplayPermit(gate);
            Assert.That(heldPermits, Is.GreaterThan(0),
                "instrument validation: the gate must be saturated for the comparison to mean "
                + "anything");

            var waitSamples = new ConcurrentBag<(double Value, KeyValuePair<string, object?>[] Tags)>();

            using (ListenForQueueWaitSamples(waitSamples))
            {
                var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                    preloadedSnapshot: null,
                    persistedCheckpoint: 0,
                    walHead: 0);
                state.State.TreeId = UniqueReplayPermitTree();

                activation = LeafActivationHarness.ActivateAsync(
                    (IGrainBase)grain, CancellationToken.None);

                await Task.Delay(TimeSpan.FromMilliseconds(250));

                var queued = SampleReplayGateGauges();

                Assert.Multiple(() =>
                {
                    Assert.That(waitSamples, Is.Empty,
                        "the queue-wait histogram must be silent while the wait is still in "
                        + "progress; it records at an outcome, and there has been no outcome");
                    Assert.That(queued[QueuedPermitsGaugeName], Is.EqualTo(1),
                        "while the queued level reports the waiter at that same moment. This "
                        + "difference IS the instrument: without it the saturated gate above is "
                        + "indistinguishable from a gate nobody asked for a permit");
                });

                gate.Release(heldPermits);
                heldPermits = 0;

                await activation.WaitAsync(TimeSpan.FromSeconds(30));
                activation = null;
            }

            Assert.That(waitSamples, Is.Not.Empty,
                "instrument validation: once the wait ends the histogram DOES record, so its "
                + "silence above was a property of the wait being in flight rather than a broken "
                + "listener. Without this the first assertion passes against a listener that was "
                + "never wired up");
        }
        finally
        {
            if (heldPermits > 0)
                gate.Release(heldPermits);

            if (activation is not null)
                await activation.WaitAsync(TimeSpan.FromSeconds(30));
        }
    }
}
