using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Covers the in-flight replay-permit wait instruments added for issue #3044.
/// <para>
/// <see cref="LatticeMetrics.WalReplayPermitQueueWait"/> records on two arms and
/// both are terminal - <c>acquired</c> after the semaphore is entered,
/// <c>canceled</c> from the catch around the wait. A wait that never returns
/// records on neither, so the histogram is structurally silent about exactly the
/// state a saturated gate produces. No amount of zero-priming reaches that gap:
/// a primed <c>canceled</c> arm reading zero says "no cancellation completed",
/// which is true and irrelevant while a wait is still parked. The missing
/// observable is a level, not a terminal event.
/// </para>
/// <para>
/// These tests pin the level instruments that close it, and the boundary on
/// them: the count is zero-primed so an idle tree is a measured zero rather than
/// an absent series, while the age is deliberately <em>not</em> primed because
/// the age of the oldest waiter when there is no waiter is undefined, not zero.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class WalReplayPermitWaitInFlightTests
{
    /// <summary>
    /// A tree id unique to each test. The permit-wait registry is process-wide
    /// static, so a shared literal would let a sibling fixture's parked wait
    /// land in this test's count and read as a product defect.
    /// </summary>
    private static string FreshTree() => $"permit-wait-test-{Guid.NewGuid():N}";

    private static List<(string Tree, long Value)> ObserveInFlight()
    {
        var captured = new List<(string Tree, long Value)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayPermitWaitsInFlight,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                captured.Add((TreeTag(tags), value))));

        listener.RecordObservableInstruments();
        return captured;
    }

    private static List<(string Tree, double Value)> ObserveOldestAge()
    {
        var captured = new List<(string Tree, double Value)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayPermitWaitOldestAge,
            l => l.SetMeasurementEventCallback<double>((_, value, tags, _) =>
                captured.Add((TreeTag(tags), value))));

        listener.RecordObservableInstruments();
        return captured;
    }

    private static string TreeTag(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
            {
                return tag.Value as string ?? string.Empty;
            }
        }

        return string.Empty;
    }

    [Test]
    public void In_flight_gauge_counts_a_wait_that_has_not_terminated()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterWalReplayPermitWait(tree))
        {
            var observed = ObserveInFlight();

            // Method rule 2: an absence produced by machinery that never ran is
            // byte-identical to a measured absence. Assert the observation
            // callback actually fired before reading anything off it, so a
            // listener that enabled no instrument cannot pass as a clean zero.
            Assert.That(observed, Is.Not.Empty,
                "the in-flight gauge produced no measurements at all, so the assertion below would " +
                "be vacuous rather than informative");

            var mine = observed.Where(m => m.Tree == tree).ToList();
            Assert.That(mine, Has.Count.EqualTo(1),
                "expected exactly one series for the tree under test");
            Assert.That(mine[0].Value, Is.EqualTo(1),
                "a permit wait that has not terminated must be counted while it is parked - this is " +
                "the state both arms of the queue-wait histogram are silent about");
        }
    }

    [Test]
    public void In_flight_gauge_reports_a_measured_zero_once_the_wait_drains()
    {
        var tree = FreshTree();

        LatticeMetrics.EnterWalReplayPermitWait(tree).Dispose();

        var observed = ObserveInFlight();
        Assert.That(observed, Is.Not.Empty, "the in-flight gauge produced no measurements at all");

        var mine = observed.Where(m => m.Tree == tree).ToList();

        // The whole point of the priming: without it this series is absent, and
        // "nothing is queued" becomes byte-identical to "the instrument never
        // ran" - the exact ambiguity that makes the terminal arms unusable as
        // evidence.
        Assert.That(mine, Has.Count.EqualTo(1),
            "a tree that has queued at least once must keep reporting, so a zero is a measured zero " +
            "rather than an absent series");
        Assert.That(mine[0].Value, Is.Zero);
    }

    [Test]
    public void In_flight_gauge_does_not_report_a_tree_that_never_queued()
    {
        var neverQueued = FreshTree();

        var observed = ObserveInFlight();
        Assert.That(observed, Is.Not.Empty, "the in-flight gauge produced no measurements at all");

        // The boundary on the priming, and it is part of the finding: the gauge
        // primes trees it has OBSERVED queueing, not every tree in the estate.
        // A zero therefore means "this tree has queued before and is not queued
        // now"; it never means "this tree cannot queue".
        Assert.That(observed.Any(m => m.Tree == neverQueued), Is.False,
            "priming must not fabricate a series for a tree the process has never seen queue");
    }

    [Test]
    public async Task Oldest_age_climbs_while_a_wait_stays_parked()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterWalReplayPermitWait(tree))
        {
            await Task.Delay(25);

            var observed = ObserveOldestAge();
            Assert.That(observed, Is.Not.Empty, "the oldest-age gauge produced no measurements at all");

            var mine = observed.Where(m => m.Tree == tree).ToList();
            Assert.That(mine, Has.Count.EqualTo(1));
            Assert.That(mine[0].Value, Is.GreaterThan(0d),
                "a parked wait must report a positive age - a count alone cannot separate three " +
                "healthy short waits from one parked for minutes, which is the distinction the " +
                "instrument exists to draw");
        }
    }

    [Test]
    public void Oldest_age_reports_no_series_when_nothing_is_parked()
    {
        var tree = FreshTree();

        LatticeMetrics.EnterWalReplayPermitWait(tree).Dispose();

        var observed = ObserveOldestAge();
        var mine = observed.Where(m => m.Tree == tree).ToList();

        // Deliberately asymmetric with the count, and the asymmetry is the
        // finding: priming is free for an instrument whose empty state is a
        // level and wrong for one whose empty state is undefined. The age of the
        // oldest waiter when there is no waiter is not zero, and reporting zero
        // would publish the healthiest possible value for the emptiest possible
        // state.
        Assert.That(mine, Is.Empty,
            "the oldest-age gauge must not prime, because zero is a meaningful (healthy) age and " +
            "would be indistinguishable from a fabricated one");
    }

    [Test]
    public void Oldest_age_tracks_the_oldest_of_several_waits_not_the_newest()
    {
        var tree = FreshTree();

        var first = LatticeMetrics.EnterWalReplayPermitWait(tree);
        try
        {
            Thread.Sleep(30);
            using (LatticeMetrics.EnterWalReplayPermitWait(tree))
            {
                var ages = ObserveOldestAge().Where(m => m.Tree == tree).ToList();
                var counts = ObserveInFlight().Where(m => m.Tree == tree).ToList();

                Assert.That(counts, Has.Count.EqualTo(1));
                Assert.That(counts[0].Value, Is.EqualTo(2),
                    "both parked waits must be counted");

                Assert.That(ages, Has.Count.EqualTo(1));
                Assert.That(ages[0].Value, Is.GreaterThan(0.02d),
                    "the gauge must report the age of the OLDEST parked wait; reporting the newest " +
                    "would hide a long-parked activation behind a stream of short ones");
            }
        }
        finally
        {
            first.Dispose();
        }
    }

    [Test]
    public void Queue_wait_histogram_declares_explicit_bucket_boundaries()
    {
        // Without advice the histogram exports with no bucket series, leaving a
        // bare mean that cannot separate a uniformly slow gate from a few
        // pathological holds.
        //
        // This also pins an ordering hazard that fails SILENTLY: the advice
        // field must be declared above the histogram that consumes it, because
        // static initialisers run in declaration order and the advice parameter
        // is nullable. Declared below, it is read as null, the histogram is
        // built bucketless, and nothing throws.
        var advice = LatticeMetrics.WalReplayPermitQueueWait.Advice;

        Assert.That(advice, Is.Not.Null,
            "the queue-wait histogram must carry bucket advice; a null here is the silent " +
            "declaration-order failure, not merely a missing feature");

        var boundaries = advice!.HistogramBucketBoundaries;
        Assert.That(boundaries, Is.Not.Null.And.Not.Empty);

        Assert.That(boundaries!, Is.Ordered.Ascending,
            "OpenTelemetry requires strictly ascending boundaries");

        // The two boundaries that were chosen rather than merely spaced.
        Assert.That(boundaries!, Does.Contain(30_000d),
            "30s is the Orleans response deadline, so a boundary there isolates waits that " +
            "outlived the caller waiting on them");
        Assert.That(boundaries!.Max(), Is.GreaterThanOrEqualTo(900_000d),
            "the observed range reaches minutes, so the top boundary must not truncate it into " +
            "a single overflow bucket");
    }

    [Test]
    public void Acquire_replay_permit_enters_the_scope_around_the_gate_wait()
    {
        // A structural guard, and deliberately labelled as one: the behavioural
        // tests above pin the instruments, but nothing in them reaches the only
        // caller that registers a real wait. Exercising it properly needs a
        // cluster AND a saturated process-wide static gate, which would leak
        // permit pressure into every other fixture in the process - a worse
        // trade than this.
        //
        // Guarding it at all is not ceremony: an unwired scope leaves both
        // gauges reporting "nobody is queued" forever, which is silent, is
        // indistinguishable from a healthy gate, and is the precise failure
        // class these instruments were added to eliminate.
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src", "lattice", "BPlusTree", "Grains", "BPlusLeafGrain.Activation.cs");

        Assert.That(File.Exists(path), Is.True, $"expected the emission site at {path}");
        var source = File.ReadAllText(path);

        var enter = source.IndexOf("LatticeMetrics.EnterWalReplayPermitWait(", StringComparison.Ordinal);
        var wait = source.IndexOf("await gate.WaitAsync(", StringComparison.Ordinal);
        var dispose = source.IndexOf("permitWaitScope.Dispose();", StringComparison.Ordinal);

        // Method rule 2: assert every anchor was actually located before
        // comparing them. Two missing anchors both yield -1 and would compare
        // equal, so an unfound emission site could otherwise pass as a satisfied
        // ordering constraint.
        Assert.Multiple(() =>
        {
            Assert.That(enter, Is.GreaterThanOrEqualTo(0), "scope entry not found at the emission site");
            Assert.That(wait, Is.GreaterThanOrEqualTo(0), "gate wait not found at the emission site");
            Assert.That(dispose, Is.GreaterThanOrEqualTo(0), "scope disposal not found at the emission site");
        });

        Assert.That(enter, Is.LessThan(wait),
            "the scope must be entered BEFORE the gate wait; entered after, it measures a wait that " +
            "has already finished and reports zero for exactly the parked state it exists to surface");

        var between = source[wait..dispose];
        Assert.That(between, Does.Contain("finally"),
            "disposal must be in a finally, so the wait is deregistered on the acquired path and the " +
            "canceled path alike - while a wait with no terminal path at all keeps its entry live, " +
            "which is the whole observable");
    }
}
