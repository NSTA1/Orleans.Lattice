using System.Diagnostics;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the <b>lifecycle</b> of the over-budget replay warning's throttle
/// state (issue #2100, review of PR #2109).
/// <para>
/// <see cref="BPlusLeafGrainOverBudgetLogAggregateBoundTests"/> pins what the
/// gate decides. These pin what it <b>remembers</b> while deciding, which is
/// where the first implementation went wrong: the per-key stamp was committed
/// before the per-tree cap had decided whether a line would actually be
/// emitted, the sweep manufactured novelty by deleting stamps outright, and a
/// tree window that owed a summary could never be reclaimed.
/// </para>
/// <para>
/// Each of these is a way to lose the signal rather than bound it, which is the
/// one trade this whole change may not make. The warning's fault criterion
/// needs at least two comparable lines for the <b>same</b> leaf (issue #2023),
/// so state that silently retires, backs off, or pins the wrong thing turns a
/// bounded log into a useless one.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainOverBudgetLogStateLifecycleTests
{
    private const int Budget = BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow;

    private static string UniqueTree() => $"lifecycle-{Guid.NewGuid():N}";

    private static string Leaf(int ordinal) => $"leaf/{ordinal}";

    /// <summary>
    /// Converts a <see cref="TimeSpan"/> into the <see cref="Stopwatch"/> tick
    /// delta the gate measures in, so a test can step over an interval instead
    /// of sleeping through it.
    /// </summary>
    private static long Ticks(TimeSpan elapsed) => (long)(elapsed.TotalSeconds * Stopwatch.Frequency);

    private static long OneInterval => Ticks(BPlusLeafGrain.OverBudgetLogInterval);

    private static long Margin => Ticks(TimeSpan.FromSeconds(1));

    /// <summary>
    /// The load-bearing one. A repeat withheld by the per-tree cap emits
    /// nothing, so it must not pay the backoff that emitting would have cost.
    /// <para>
    /// Committing the stamp before the cap decided meant a busy tree charged
    /// every leaf for lines it never printed. On a tree with roughly a thousand
    /// partitions every key reaches the one-hour ceiling within about seven
    /// rounds while the tree as a whole still prints only eight lines an hour,
    /// so a specific livelocked leaf gets named roughly once ever - and one
    /// line can never satisfy a criterion that compares two.
    /// </para>
    /// </summary>
    [Test]
    public void A_repeat_withheld_by_the_cap_does_not_pay_the_backoff_it_never_spent()
    {
        var tree = UniqueTree();
        var t0 = Stopwatch.GetTimestamp();

        // The leaf under test, plus exactly enough others to spend the tree's
        // whole per-window budget on repeats later. All are novel now, and a
        // novel key is never withheld, so all of them report.
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t0).LogDetail, Is.True,
            "Precondition: the leaf under test has reported once, so it is no longer novel.");
        for (var i = 1; i <= Budget; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, t0);
        }

        // One interval on, the filler leaves arrive first and take the entire
        // budget, so the leaf under test is due but is withheld.
        var t1 = t0 + OneInterval + Margin;
        for (var i = 1; i <= Budget; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, t1);
        }

        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t1).LogDetail, Is.False,
            "Precondition: the window's budget is spent, so this due repeat is withheld by the cap.");

        // One further interval on it arrives into a fresh window ahead of the
        // fillers. Its backoff must still be one interval, because it has
        // emitted exactly one line in its life.
        var t2 = t1 + OneInterval + Margin;

        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t2).LogDetail, Is.True,
            "The cap withheld the previous occurrence, so no line was printed and no backoff was "
            + "earned. Charging it anyway is what stopped a livelocked leaf ever getting the "
            + "second comparable line the fault criterion in issue #2023 compares against.");
    }

    /// <summary>
    /// Novelty is a property of the key's history, not of whether the sweep
    /// happens to have kept its stamp. A swept key is due again - backoff
    /// decays, which is the point of sweeping it - but it is still a repeat, so
    /// the per-tree cap still applies to it.
    /// </summary>
    [Test]
    public void A_key_retired_by_the_sweep_is_due_again_but_is_not_novel_again()
    {
        var tree = UniqueTree();
        var t0 = Stopwatch.GetTimestamp();
        var t1 = t0 + OneInterval + Margin;

        // The key under test reports once, so it has a history.
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t0).LogDetail, Is.True,
            "Precondition: the key under test has reported, so it is no longer novel.");

        // Age it out and sweep. The old sweep deleted the stamp outright, which
        // left the key indistinguishable from one never seen before.
        BPlusLeafGrain.PruneOverBudgetLogStamps(t1);

        // Spend the tree's whole budget on repeats, so from here only a key the
        // gate believes is novel could still report.
        for (var i = 1; i <= Budget; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, t0);
        }

        for (var i = 1; i <= Budget; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, t1);
        }

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t1).LogDetail, Is.False,
                "The sweep retired this key's backoff, it did not erase the fact that the key has "
                + "reported before. Granting the novel-key exemption to a swept key makes the "
                + "one-time novel burst recur every time the map churns, which at the scale this "
                + "bound exists for is continuously.");

            Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(9_999), 0, t1).LogDetail, Is.True,
                "Control: a key with no history at all is genuinely novel and must still be exempt, "
                + "otherwise the fix has bought quiet by suppressing the signal.");
        });
    }

    /// <summary>
    /// The other half of that bound, stated honestly: retirement is not
    /// permanent. Once a key has been silent for the whole retention its next
    /// occurrence really is new information, and treating it as novel is
    /// correct rather than an artefact.
    /// </summary>
    [Test]
    public void A_key_silent_for_the_whole_retention_is_novel_again()
    {
        var subject = UniqueTree();
        var t0 = Stopwatch.GetTimestamp();

        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(subject, Leaf(0), 0, t0).LogDetail, Is.True,
            "Precondition: the key has reported once.");

        // Retire it, then let the whole retention elapse and sweep again.
        BPlusLeafGrain.PruneOverBudgetLogStamps(t0 + OneInterval + Margin);
        var farFuture = t0 + Ticks(BPlusLeafGrain.OverBudgetLogIntervalCeiling) + Margin;
        BPlusLeafGrain.PruneOverBudgetLogStamps(farFuture);

        // Saturate a fresh tree's budget at that instant, so only a key the
        // gate believes is novel can report.
        var tree = UniqueTree();
        for (var i = 1; i <= Budget; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, farFuture);
        }

        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, farFuture).LogDetail, Is.True,
            "An hour of silence is not a churning map: the key really has gone away and come back, "
            + "so the exemption is earned. This is what keeps retained state bounded instead of "
            + "growing with every key the process has ever seen.");
    }

    /// <summary>
    /// A tree window that withheld something owes a summary line, but the flush
    /// is occurrence-driven - so a tree that falls quiet owes it forever. With
    /// only a skip-if-owing rule the sweep pins exactly the entries that will
    /// never be reclaimed, and past the capacity the map grows monotonically
    /// with the number of distinct tree ids the process has ever seen.
    /// </summary>
    [Test]
    public void A_window_owing_a_summary_is_retained_while_fresh_and_freed_once_stale()
    {
        var fresh = UniqueTree();
        var stale = UniqueTree();
        var t0 = Stopwatch.GetTimestamp();
        var t1 = t0 + OneInterval + Margin;
        const int leaves = Budget * 3;
        const int expectedSuppressed = leaves - Budget;

        foreach (var tree in new[] { fresh, stale })
        {
            for (var i = 0; i < leaves; i++)
            {
                BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, t0);
            }

            // As repeats these exceed the budget, so the window ends up owing a
            // summary for what it withheld.
            for (var i = 0; i < leaves; i++)
            {
                BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(i), 0, t1);
            }
        }

        var soon = t1 + OneInterval + Margin;
        var stalePoint = t1 + Ticks(BPlusLeafGrain.OverBudgetLogIntervalCeiling) + Margin;

        BPlusLeafGrain.PruneOverBudgetTreeWindows(soon);
        var freshDecision = BPlusLeafGrain.ClassifyOverBudgetReplayLog(fresh, Leaf(0), 0, soon);

        BPlusLeafGrain.PruneOverBudgetTreeWindows(stalePoint);
        var staleDecision = BPlusLeafGrain.ClassifyOverBudgetReplayLog(stale, Leaf(0), 0, stalePoint);

        Assert.Multiple(() =>
        {
            Assert.That(freshDecision.SuppressedInClosedWindow, Is.EqualTo(expectedSuppressed),
                "A tally one interval old is still news, so the sweep must leave the entry alone "
                + "and the next occurrence must report it. Freeing it here would trade the "
                + "visibility of the cap for memory that is not yet under pressure.");

            Assert.That(staleDecision.SuppressedInClosedWindow, Is.Zero,
                "An hour-stale tally has no diagnostic value and the metric remains the exact "
                + "census, so the entry must be reclaimed rather than pinning the map forever.");
        });
    }

    /// <summary>
    /// Backoff describes a run of the condition, so a clean in-budget
    /// activation ends the run. Without this a leaf that misbehaved this
    /// morning and regresses this evening inherits up to an hour of accumulated
    /// backoff and reports late, which is precisely when the report matters.
    /// </summary>
    [Test]
    public void An_in_budget_activation_retires_the_backoff_so_a_regression_reports_promptly()
    {
        var tree = UniqueTree();
        var t0 = Stopwatch.GetTimestamp();

        BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t0);
        var t1 = t0 + OneInterval + Margin;
        BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t1);

        // Having reported twice, one interval is no longer enough.
        var t2 = t1 + OneInterval + Margin;
        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t2).LogDetail, Is.False,
            "Precondition: the interval has doubled, so this occurrence is not yet due.");

        // The leaf then activates within budget, which is what the call site
        // reports by retiring the stamp.
        BPlusLeafGrain.RetireOverBudgetLogStamp(tree, Leaf(0), 0);

        Assert.That(BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, Leaf(0), 0, t2).LogDetail, Is.True,
            "The run ended, so the next occurrence starts a new one at the base interval. Carrying "
            + "the old backoff across a clean activation delays the first line of a fresh fault by "
            + "up to the whole ceiling.");
    }
}
