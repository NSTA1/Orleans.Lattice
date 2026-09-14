using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers the properties of the #2967 leaf-split completion gauges that
/// <see cref="LeafSplitCompletionInFlightTests"/> cannot reach (issue #3053).
/// <para>
/// <b>This fixture exists because of a structural limit in the other one, not
/// because that one is weak.</b> <see cref="LeafSplitCompletionInFlightTests"/>
/// drives a real division through a real grain, which is the right way to prove
/// the scope is taken on the live split path and released inescapably on the
/// throw path. But a single division puts exactly <em>one</em> entry on
/// <em>one</em> tree into the registry, and with one entry every aggregate the
/// gauges compute is degenerate: the oldest entry is also the newest, a count of
/// one is indistinguishable from a constant one, and a per-tree partition is
/// indistinguishable from a global one. Every property below is invisible at
/// n=1 and needs n&gt;1 on more than one tree, which is why these arms drive the
/// registry directly through <see cref="LatticeMetrics.EnterLeafSplitCompletion"/>
/// rather than through a division.
/// </para>
/// <para>
/// <b>The gap was measured, not assumed.</b> Before these arms existed, five
/// independent faults were injected into the observer callbacks and the existing
/// fixture was run against each: reporting the newest completion instead of the
/// oldest, reporting an entry count in place of an elapsed duration, reporting
/// one tree's age for another's, saturating the in-flight count at one, and
/// merging every tree into a single series. It passed 2/2 against four of the
/// five. That is not a criticism of it - each of those faults is invisible to
/// any test that only ever creates one entry.
/// </para>
/// <para>
/// <b>Why an untested observable gauge is worse than an untested counter.</b> A
/// callback that misbehaves inside a <see cref="MeterListener"/> does not throw
/// at the offending line; it surfaces as a wrong or missing series at scrape
/// time, which reads exactly like an idle system. Every fault listed above would
/// have shipped as a plausible-looking dashboard.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LeafSplitCompletionGaugeTests
{
    /// <summary>
    /// The completion registry is a process-wide static, so a shared literal
    /// would let a sibling fixture's in-flight division land in this test's
    /// count and read as a product defect. Every arm uses its own tree ids.
    /// </summary>
    private static string FreshTree() => $"leaf-split-gauge-{Guid.NewGuid():N}";

    /// <summary>
    /// A delay long enough that an age difference is unambiguous against clock
    /// granularity, and short enough to keep the fixture in the unit tier.
    /// </summary>
    private static readonly TimeSpan AgeSeparation = TimeSpan.FromMilliseconds(250);

    private static double SeparationSeconds => AgeSeparation.TotalSeconds;

    private static List<(string Tree, long Value)> ObserveInFlight()
    {
        var captured = new List<(string Tree, long Value)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitCompletionsInFlight,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                captured.Add((TreeTag(tags), value))));

        listener.RecordObservableInstruments();
        return captured;
    }

    private static List<(string Tree, double Value)> ObserveOldestAge()
    {
        var captured = new List<(string Tree, double Value)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitCompletionOldestAge,
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

    /// <summary>
    /// Method rule 2: an absence produced by machinery that never ran is
    /// byte-identical to a measured absence. Each arm asserts the callback fired
    /// at all, and separately that it produced a series for the tree under test,
    /// before reading any value off it - so a listener that enabled no
    /// instrument, or an observer that lost the tree tag, cannot pass as a clean
    /// reading.
    /// </summary>
    private static T SeriesFor<T>(List<(string Tree, T Value)> observed, string tree, string instrument)
    {
        Assert.That(observed, Is.Not.Empty,
            $"the {instrument} gauge produced no measurements at all, so every assertion below would " +
            "be vacuous rather than informative");

        var mine = observed.Where(m => m.Tree == tree).ToList();
        Assert.That(mine, Has.Count.EqualTo(1),
            $"expected exactly one {instrument} series tagged with the tree under test; a missing or " +
            "duplicated series means the observer is not partitioning by tree as its tag set claims");

        return mine[0].Value;
    }

    [Test]
    public void In_flight_gauge_counts_every_concurrent_completion_on_a_tree()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        {
            var value = SeriesFor(ObserveInFlight(), tree, "in-flight");

            Assert.That(value, Is.EqualTo(3),
                "the in-flight gauge must count every completion concurrently suspended on a tree; a " +
                "gauge that saturates at one reports a single wedged division and a silo-wide pile-up " +
                "identically, and the pile-up is the one that needs an operator");
        }
    }

    [Test]
    public void In_flight_gauge_reports_each_tree_separately()
    {
        var busy = FreshTree();
        var quiet = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(busy))
        using (LatticeMetrics.EnterLeafSplitCompletion(busy))
        using (LatticeMetrics.EnterLeafSplitCompletion(quiet))
        {
            var observed = ObserveInFlight();

            Assert.That(SeriesFor(observed, busy, "in-flight"), Is.EqualTo(2),
                "the busy tree must report its own completions only");
            Assert.That(SeriesFor(observed, quiet, "in-flight"), Is.EqualTo(1),
                "and a second tree must not inherit the first tree's count - the tag exists so an " +
                "operator can attribute a pile-up to one tree, which a merged series cannot do");
        }
    }

    [Test]
    public void In_flight_gauge_drops_a_tree_once_its_completions_drain()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        {
            Assert.That(SeriesFor(ObserveInFlight(), tree, "in-flight"), Is.EqualTo(1),
                "precondition: the completion is in flight before it is released");
        }

        // This gauge is deliberately NOT zero-primed: it reports no series at
        // all for a tree with nothing in flight, which is what the assertion
        // below pins.
        //
        // The boundary is part of the reading and is not severable from it: an
        // absent series here means "nothing suspended", and must never be read
        // as "the instrument did not run". Those two are byte-identical at the
        // scrape, so this arm pins the absence in both directions rather than
        // only asserting the presence above.
        Assert.That(ObserveInFlight().Where(m => m.Tree == tree), Is.Empty,
            "a tree with no completions in flight must report no series at all, which is what makes a " +
            "present series unambiguous evidence of a live suspension");
    }

    [Test]
    public void Oldest_age_reports_the_oldest_completion_not_the_newest()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        {
            Thread.Sleep(AgeSeparation);

            using (LatticeMetrics.EnterLeafSplitCompletion(tree))
            {
                var age = SeriesFor(ObserveOldestAge(), tree, "oldest-age");

                Assert.That(age, Is.GreaterThanOrEqualTo(SeparationSeconds * 0.8),
                    "the age gauge must report the age of the OLDEST completion on the tree, not the " +
                    "newest; reporting the newest means a division wedged for hours reads as healthy " +
                    "the moment any fresh division starts beside it, which is precisely when a wedge " +
                    "is most likely and least visible");
            }
        }
    }

    [Test]
    public void Oldest_age_is_an_elapsed_duration_that_climbs_while_a_completion_stays_suspended()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        {
            var first = SeriesFor(ObserveOldestAge(), tree, "oldest-age");
            Thread.Sleep(AgeSeparation);
            var second = SeriesFor(ObserveOldestAge(), tree, "oldest-age");

            Assert.That(second, Is.GreaterThan(first + (SeparationSeconds * 0.5)),
                "the age gauge must be an elapsed duration that climbs while the completion stays " +
                "suspended, not a constant or a count of entries; 'a climbing value is a wedge' is the " +
                "documented reading, and a value that cannot climb makes that reading unfalsifiable");
        }
    }

    /// <summary>
    /// <b>This arm is not a clean discriminator on its own, and that overlap is
    /// intrinsic rather than a weakness to engineer away.</b> It goes red under a
    /// cross-tree leak, which is what it is for, but also under an age gauge that
    /// reports an entry count instead of a duration - because a count really
    /// does violate per-tree scoping as well, so the second failure is a true
    /// positive and not noise. The two faults stay distinguishable by their
    /// signature rather than by this message: a leak fails only this arm, while a
    /// count additionally fails
    /// <see cref="Oldest_age_is_an_elapsed_duration_that_climbs_while_a_completion_stays_suspended"/>,
    /// which no other fault trips. Read the pair, not this line alone.
    /// </summary>
    [Test]
    public void Oldest_age_is_scoped_to_its_own_tree()
    {
        var stale = FreshTree();
        var fresh = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(stale))
        {
            Thread.Sleep(AgeSeparation);

            using (LatticeMetrics.EnterLeafSplitCompletion(fresh))
            {
                var observed = ObserveOldestAge();

                Assert.That(SeriesFor(observed, stale, "oldest-age"),
                    Is.GreaterThanOrEqualTo(SeparationSeconds * 0.8),
                    "precondition: the stale tree's completion is genuinely old");
                Assert.That(SeriesFor(observed, fresh, "oldest-age"),
                    Is.LessThan(SeparationSeconds * 0.5),
                    "a tree's age must reflect only its own completions; leaking the oldest completion " +
                    "across trees would make one wedged tree raise the age on every healthy tree in " +
                    "the silo, which points an operator at the wrong subsystem entirely");
            }
        }
    }

    [Test]
    public void Both_gauges_carry_the_tenant_dimension_alongside_the_tree_tag()
    {
        var tree = FreshTree();

        using (LatticeMetrics.EnterLeafSplitCompletion(tree))
        {
            var countTags = CapturedTagKeys(LatticeMetrics.LeafSplitCompletionsInFlight);
            var ageTags = CapturedTagKeys(LatticeMetrics.LeafSplitCompletionOldestAge);

            Assert.That(countTags, Is.Not.Empty, "the in-flight gauge produced no measurements at all");
            Assert.That(ageTags, Is.Not.Empty, "the oldest-age gauge produced no measurements at all");

            // Both gauges are per-tree, so both must carry the tenant dimension
            // the repository-wide hygiene gate requires of a tree-tagged
            // instrument. Asserting it here as well keeps the requirement
            // legible at the instrument rather than only in a reflective scan.
            Assert.That(countTags, Does.Contain(LatticeMetrics.TagTree));
            Assert.That(ageTags, Does.Contain(LatticeMetrics.TagTree));
            Assert.That(countTags, Is.EqualTo(ageTags),
                "the two gauges are read together on one panel, so they must carry the same dimensions; " +
                "a tag present on one and absent from the other cannot be joined in a query");
        }
    }

    private static List<string> CapturedTagKeys(Instrument instrument)
    {
        var keys = new List<string>();

        void Capture(ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (keys.Count > 0)
            {
                return;
            }

            foreach (var tag in tags)
            {
                keys.Add(tag.Key);
            }
        }

        using var listener = MeterListening.StartForInstrument(
            instrument,
            l =>
            {
                l.SetMeasurementEventCallback<long>((_, _, tags, _) => Capture(tags));
                l.SetMeasurementEventCallback<double>((_, _, tags, _) => Capture(tags));
            });

        listener.RecordObservableInstruments();
        keys.Sort(StringComparer.Ordinal);
        return keys;
    }
}
