using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the over-budget replay warning's per-leaf throttle.
/// <para>
/// A cold start on a large volume re-activates these leaves continuously, and one
/// warning per attempt buried a real deployment in 5,752 identical lines in fifteen
/// minutes - enough to make the log useless for spotting the genuine faults mixed in
/// among them. The counter
/// <c>orleans.lattice.leaf.activation_replays_over_budget</c> already records every
/// occurrence, so the log only has to say the condition is happening; the rate is a
/// metric concern.
/// </para>
/// <para>
/// The throttle is keyed by <b>leaf</b> as well as tree and WAL partition (issue
/// #2023). Keyed on (tree, partition) alone it suppressed every leaf but the first
/// to trip the budget, so consecutive warnings were one-per-minute samples of
/// arbitrary different leaves whose checkpoints were not comparable - which made the
/// "checkpoint that does not advance is a fault" criterion the warning states
/// unevaluable, and produced a false livelock report.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainOverBudgetLogThrottleTests
{
    private static string UniqueTree() => $"throttle-{Guid.NewGuid():N}";

    private static string Leaf(int ordinal) => $"leaf/{ordinal}";

    [Test]
    public void The_first_occurrence_for_a_leaf_is_logged()
    {
        var tree = UniqueTree();

        Assert.That(BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0), Is.True,
            "The first time a leaf partition replays over budget it must be reported.");
    }

    [Test]
    public void An_immediate_repeat_for_the_same_leaf_is_suppressed()
    {
        var tree = UniqueTree();

        var first = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);
        var second = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);
        var third = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);

        Assert.Multiple(() =>
        {
            // The positive case is asserted alongside the negatives so a throttle
            // that suppressed EVERYTHING - which would also make the negatives pass -
            // cannot masquerade as working.
            Assert.That(first, Is.True, "The first occurrence is logged,");
            Assert.That(second, Is.False, "the immediate repeat is suppressed,");
            Assert.That(third, Is.False, "and so is the one after it.");
        });
    }

    [Test]
    public void Each_partition_of_a_leaf_is_throttled_independently()
    {
        var tree = UniqueTree();

        var p0 = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);
        var p1 = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 1);
        var p0Again = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);

        Assert.Multiple(() =>
        {
            Assert.That(p0, Is.True, "WAL partition 0 reports,");
            Assert.That(p1, Is.True,
                "partition 1 reports too - each partition carries its own checkpoint, so a leaf "
                + "replaying several of them names each one once rather than only the first to arrive,");
            Assert.That(p0Again, Is.False, "while partition 0's own repeat stays suppressed.");
        });
    }

    /// <summary>
    /// The defect issue #2023 is about. <c>partition</c> is the WAL partition
    /// ordinal, iterated <c>[0, WalPartitions)</c> inside <b>every</b> leaf's
    /// activation - so a throttle that does not key on the leaf lets the first leaf
    /// to trip the budget silence every other leaf in that tree and partition for a
    /// full minute.
    /// </summary>
    [Test]
    public void A_second_leaf_is_not_suppressed_by_the_first_leaf_on_the_same_partition()
    {
        var tree = UniqueTree();

        var leafA = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);
        var leafB = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(1), 0);
        var leafC = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(2), 0);
        var leafARepeat = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(0), 0);

        Assert.Multiple(() =>
        {
            Assert.That(leafA, Is.True, "The first leaf reports,");
            Assert.That(leafB, Is.True,
                "a different leaf on the SAME WAL partition reports too - otherwise the warning's "
                + "own 'checkpoint that does not advance is a fault' criterion is unevaluable, "
                + "because consecutive lines would be samples of arbitrary different leaves,");
            Assert.That(leafC, Is.True, "and so does a third,");
            Assert.That(leafARepeat, Is.False,
                "while the first leaf's own repeat is still suppressed, so the flood the throttle "
                + "exists to stop is still stopped.");
        });
    }

    [Test]
    public void Each_tree_is_throttled_independently()
    {
        var treeA = UniqueTree();
        var treeB = UniqueTree();

        var a = BPlusLeafGrain.ShouldLogOverBudgetReplay(treeA, Leaf(0), 0);
        var b = BPlusLeafGrain.ShouldLogOverBudgetReplay(treeB, Leaf(0), 0);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.True);
            Assert.That(b, Is.True,
                "A different tree is a different leaf, so it is not suppressed by the first "
                + "tree's warning.");
        });
    }

    /// <summary>
    /// Keying by leaf means the stamp map grows with the number of distinct leaves
    /// that trip the budget, so it must stay correct well past the prune threshold.
    /// The sweep only drops stamps that have already aged out - and an aged-out
    /// stamp would permit its next warning anyway - so suppression must survive
    /// filling the map several times over.
    /// </summary>
    [Test]
    public void Suppression_survives_far_more_leaves_than_the_prune_threshold()
    {
        var tree = UniqueTree();
        const int leafCount = 20_000;

        for (var i = 0; i < leafCount; i++)
        {
            Assert.That(BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(i), 0), Is.True,
                $"Leaf {i} has never reported, so its first occurrence must be logged.");
        }

        var repeats = 0;
        for (var i = 0; i < leafCount; i++)
        {
            if (BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, Leaf(i), 0))
            {
                repeats++;
            }
        }

        Assert.That(repeats, Is.Zero,
            "Every one of those leaves reported within the last interval, so no stamp was "
            + "eligible for the aged-out sweep and every repeat must still be suppressed.");
    }
}
