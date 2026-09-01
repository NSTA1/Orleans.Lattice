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
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainOverBudgetLogThrottleTests
{
    private static string UniqueTree() => $"throttle-{Guid.NewGuid():N}";

    [Test]
    public void The_first_occurrence_for_a_leaf_is_logged()
    {
        var tree = UniqueTree();

        Assert.That(BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 0), Is.True,
            "The first time a partition replays over budget it must be reported.");
    }

    [Test]
    public void An_immediate_repeat_for_the_same_leaf_is_suppressed()
    {
        var tree = UniqueTree();

        var first = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 0);
        var second = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 0);
        var third = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 0);

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
    public void Each_partition_of_a_tree_is_throttled_independently()
    {
        var tree = UniqueTree();

        var p0 = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 0);
        var p1 = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 1);
        var p0Again = BPlusLeafGrain.ShouldLogOverBudgetReplay(tree, 0);

        Assert.Multiple(() =>
        {
            Assert.That(p0, Is.True, "Partition 0 reports,");
            Assert.That(p1, Is.True,
                "partition 1 reports too - suppression is per leaf, so a whole tree replaying "
                + "still names every partition once rather than only the first to arrive,");
            Assert.That(p0Again, Is.False, "while partition 0's own repeat stays suppressed.");
        });
    }

    [Test]
    public void Each_tree_is_throttled_independently()
    {
        var treeA = UniqueTree();
        var treeB = UniqueTree();

        var a = BPlusLeafGrain.ShouldLogOverBudgetReplay(treeA, 0);
        var b = BPlusLeafGrain.ShouldLogOverBudgetReplay(treeB, 0);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.True);
            Assert.That(b, Is.True,
                "A different tree's partition 0 is a different leaf, so it is not suppressed "
                + "by the first tree's warning.");
        });
    }
}
