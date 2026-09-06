using System.Diagnostics;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the over-budget replay warning's <b>aggregate</b> bound (issue #2100).
/// <para>
/// The per-key throttle covered by
/// <see cref="BPlusLeafGrainOverBudgetLogThrottleTests"/> bounds each
/// (tree, leaf, partition) key, but a tree with L leaves and P WAL partitions has
/// L x P keys, so total volume was bounded only by the size of the tree. Measured on
/// one deployment, this single warning was 12,364 of 26,800 container log lines - 46
/// percent - which rolled away the older entries that were the evidence needed to
/// diagnose the condition producing it. The instrument destroyed its own record.
/// </para>
/// <para>
/// <b>What must NOT be traded away to achieve that.</b> The warning is currently the
/// only field-visible evidence of the condition in issue #2098, so quiet bought by
/// suppressing the signal is a regression, not a fix. The guarantee these tests exist
/// to pin down is that a genuinely novel over-budget condition - a leaf partition
/// that has not reported one before - is reported promptly and in full, no matter how
/// exhausted the aggregate budget is. Only repeats are ever withheld by the cap, and
/// when they are, the count withheld is surfaced as a summary line rather than
/// dropped silently.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainOverBudgetLogAggregateBoundTests
{
    private static string UniqueTree() => $"aggregate-{Guid.NewGuid():N}";

    private static string Leaf(int ordinal) => $"leaf/{ordinal}";

    /// <summary>
    /// Converts a <see cref="TimeSpan"/> into the <see cref="Stopwatch"/> tick delta
    /// the gate measures in, so a test can step over an interval instead of sleeping
    /// through it.
    /// </summary>
    private static long Ticks(TimeSpan elapsed) => (long)(elapsed.TotalSeconds * Stopwatch.Frequency);

    [Test]
    public void A_first_occurrence_is_logged_in_full()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();

        var decision = BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, now);

        Assert.Multiple(() =>
        {
            Assert.That(decision.LogDetail, Is.True,
                "The first time a leaf partition replays over budget it must be reported in full.");
            Assert.That(decision.SuppressedInClosedWindow, Is.Zero,
                "Nothing has been withheld yet, so no summary line is owed.");
        });
    }

    /// <summary>
    /// The requirement this whole item turns on, and the one an aggregate cap is most
    /// likely to break by accident. Far more distinct leaves report than the per-tree
    /// budget allows, and every single one of them is novel, so every single one must
    /// be logged in full. A cap that counted novel occurrences against the budget
    /// would silence leaves 9 and beyond here - which is precisely the "sampling that
    /// can drop a first occurrence" that is out of scope for this fix.
    /// </summary>
    [Test]
    public void A_novel_leaf_is_never_withheld_by_the_aggregate_cap()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();
        const int novelLeaves = BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow * 25;

        var logged = 0;
        for (var i = 0; i < novelLeaves; i++)
        {
            // Every occurrence is inside ONE window, so the budget is long gone by
            // the time the later leaves arrive.
            if (BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, now).LogDetail)
            {
                logged++;
            }
        }

        Assert.That(logged, Is.EqualTo(novelLeaves),
            $"All {novelLeaves} leaves are reporting for the FIRST time, so all {novelLeaves} must "
            + "be logged in full even though the per-tree budget is "
            + $"{BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow}. Bounding volume must never cost "
            + "the first report of a condition on a leaf that has not reported one before - that is "
            + "the only field-visible evidence the condition exists at all (issue #2098).");
    }

    /// <summary>
    /// The same guarantee, stated as the case an operator actually cares about: a new
    /// fault appearing in a tree that is already noisy. The noise must not swallow it.
    /// </summary>
    [Test]
    public void A_novel_leaf_still_surfaces_after_a_flood_of_repeats_has_exhausted_the_budget()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();
        const int noisyLeaves = BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow * 4;

        // Establish a set of leaves, then bring them all back due so they arrive as
        // REPEATS and exhaust the tree's aggregate budget.
        for (var i = 0; i < noisyLeaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, start);
        }

        var later = start + Ticks(BPlusLeafGrain.OverBudgetLogInterval) + Ticks(TimeSpan.FromSeconds(1));
        var repeatsLogged = 0;
        for (var i = 0; i < noisyLeaves; i++)
        {
            if (BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, later).LogDetail)
            {
                repeatsLogged++;
            }
        }

        // A leaf nobody has heard from before, arriving into that fully saturated
        // window.
        var newcomer = BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(9_999), 0, later);

        Assert.Multiple(() =>
        {
            Assert.That(repeatsLogged, Is.EqualTo(BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow),
                "Repeats are what the cap exists to bound, so exactly the budget is admitted "
                + "however many leaves are repeating.");
            Assert.That(newcomer.LogDetail, Is.True,
                "The budget is exhausted, but this leaf has never reported before. A novel "
                + "condition must still surface promptly, otherwise the fix has bought quiet by "
                + "destroying the signal.");
        });
    }

    /// <summary>
    /// The bound itself: volume in steady state must be a constant per tree per
    /// window rather than a function of tree size. This is the defect - a per-key
    /// throttle alone let L x P keys each emit at the per-key rate.
    /// </summary>
    [Test]
    public void Repeats_are_capped_per_tree_per_window_however_many_leaves_are_replaying()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();
        const int leaves = 500;

        for (var i = 0; i < leaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, start);
        }

        var later = start + Ticks(BPlusLeafGrain.OverBudgetLogInterval) + Ticks(TimeSpan.FromSeconds(1));
        var logged = 0;
        for (var i = 0; i < leaves; i++)
        {
            if (BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, later).LogDetail)
            {
                logged++;
            }
        }

        Assert.That(logged, Is.EqualTo(BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow),
            $"{leaves} leaves are repeating, but the tree may only spend "
            + $"{BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow} detail lines in a window. Without "
            + "this the total scales with the size of the tree, which is how the warning reached 46 "
            + "percent of all log lines.");
    }

    /// <summary>
    /// Capping must be visible. A bound that silently discards lines leaves an
    /// operator unable to tell a quiet tree from a capped one, which is the same
    /// failure as suppressing the signal outright.
    /// </summary>
    [Test]
    public void What_the_cap_withheld_is_reported_as_a_summary_when_the_window_closes()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();
        const int leaves = BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow * 3;

        for (var i = 0; i < leaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, start);
        }

        var second = start + Ticks(BPlusLeafGrain.OverBudgetLogInterval) + Ticks(TimeSpan.FromSeconds(1));
        for (var i = 0; i < leaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, second);
        }

        var expectedSuppressed = leaves - BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow;

        // The next occurrence to arrive after that window closes carries the tally.
        var third = second + Ticks(BPlusLeafGrain.OverBudgetLogInterval) + Ticks(TimeSpan.FromSeconds(1));
        var closing = BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, third);

        Assert.That(closing.SuppressedInClosedWindow, Is.EqualTo(expectedSuppressed),
            "Every line the cap withheld must be accounted for in the summary, so an operator "
            + "reading the log can tell that the sample is bounded and go to the metric for the "
            + "exact census.");
    }

    [Test]
    public void A_window_that_withheld_nothing_owes_no_summary()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();

        BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, start);

        var later = start + Ticks(BPlusLeafGrain.OverBudgetLogInterval) + Ticks(TimeSpan.FromSeconds(1));
        var decision = BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, later);

        Assert.Multiple(() =>
        {
            Assert.That(decision.LogDetail, Is.True,
                "One leaf well inside the budget still reports on schedule.");
            Assert.That(decision.SuppressedInClosedWindow, Is.Zero,
                "Nothing was withheld, so no summary line is emitted - the summary must not become "
                + "noise of its own.");
        });
    }

    [Test]
    public void Each_tree_carries_its_own_aggregate_budget()
    {
        var noisy = UniqueTree();
        var quiet = UniqueTree();
        var start = Stopwatch.GetTimestamp();
        const int leaves = BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow * 3;

        for (var i = 0; i < leaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(noisy, Leaf(i), 0, start);
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(quiet, Leaf(i), 0, start);
        }

        var later = start + Ticks(BPlusLeafGrain.OverBudgetLogInterval) + Ticks(TimeSpan.FromSeconds(1));
        for (var i = 0; i < leaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(noisy, Leaf(i), 0, later);
        }

        var quietRepeat = BPlusLeafGrain.ClassifyOverBudgetReplayLog(quiet, Leaf(0), 0, later);

        Assert.That(quietRepeat.LogDetail, Is.True,
            "A tree that has spent none of its own budget must not be silenced by a different "
            + "tree exhausting its budget - that would recreate the cross-contamination issue "
            + "#2023 fixed at the per-key level.");
    }

    /// <summary>
    /// The per-key backoff. The cap bounds a window; backoff is what stops a
    /// permanently replaying leaf costing a fixed rate forever, so a condition that
    /// persists for hours costs logarithmically many lines rather than linearly many.
    /// </summary>
    [Test]
    public void A_repeating_leaf_backs_off_so_its_sustained_rate_decays()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();
        var oneInterval = Ticks(BPlusLeafGrain.OverBudgetLogInterval);
        var margin = Ticks(TimeSpan.FromSeconds(1));

        // First line.
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, now).LogDetail, Is.True,
            "The first occurrence always reports.");

        // Second line, one interval later.
        now += oneInterval + margin;
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, now).LogDetail, Is.True,
            "One interval on, the second line is due.");

        // The third is NOT due one interval later - the interval has doubled.
        var oneMoreInterval = now + oneInterval + margin;
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, oneMoreInterval).LogDetail, Is.False,
            "After reporting twice the interval has doubled, so one more interval is not enough. "
            + "This is what makes the sustained cost decay instead of staying flat.");

        // It is due once the doubled interval has passed.
        var twoMoreIntervals = now + (2 * oneInterval) + margin;
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, twoMoreIntervals).LogDetail, Is.True,
            "The doubled interval elapses and the leaf reports again. Backoff must make repeats "
            + "rarer, never absent: the warning's own fault criterion needs at least two "
            + "comparable lines for the SAME leaf (issue #2023).");
    }

    /// <summary>
    /// Backoff must not become suppression. However long a leaf has been replaying,
    /// it keeps reporting at the ceiling interval, because "a checkpoint that does NOT
    /// advance across repeats for the SAME leaf is a fault" is only evaluable if the
    /// repeats keep coming.
    /// </summary>
    [Test]
    public void Backoff_saturates_at_the_ceiling_rather_than_growing_without_bound()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();
        var ceiling = Ticks(BPlusLeafGrain.OverBudgetLogIntervalCeiling);
        var margin = Ticks(TimeSpan.FromSeconds(1));

        // Drive the key far past the point where doubling would exceed the ceiling.
        for (var i = 0; i < 40; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, now);
            now += ceiling + margin;
        }

        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, now).LogDetail, Is.True,
            "A leaf that has been replaying over budget for a very long time must still report "
            + "once per ceiling interval. Backing off to silence would lose the signal the "
            + "warning exists to carry.");
    }
}
