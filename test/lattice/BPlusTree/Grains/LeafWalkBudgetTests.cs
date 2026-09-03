using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LeafWalkBudget"/>, the work bound that stops a
/// shard range-scan page fill from holding a non-reentrant shard for an
/// unbounded number of leaf visits (issue 1955).
/// </summary>
[TestFixture]
public class LeafWalkBudgetTests
{
    [Test]
    public void ShouldYield_is_false_before_the_leaf_budget_is_spent()
    {
        var budget = new LeafWalkBudget(maxLeaves: 4, maxDuration: null);

        budget.RecordLeafVisited();
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 10), Is.False);
    }

    [Test]
    public void ShouldYield_is_true_once_the_leaf_budget_is_spent()
    {
        var budget = new LeafWalkBudget(maxLeaves: 3, maxDuration: null);

        for (var i = 0; i < 3; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.True);
    }

    /// <summary>
    /// The forward-progress invariant. A caller derives its next continuation
    /// token from the last result in the page, so a page that is empty but
    /// claims more is available would leave it re-issuing an identical request
    /// forever. The budget must therefore never authorise yielding an empty
    /// page, no matter how far over budget the walk has run.
    /// </summary>
    [Test]
    public void ShouldYield_is_never_true_with_no_results_collected()
    {
        var budget = new LeafWalkBudget(maxLeaves: 1, maxDuration: null);

        for (var i = 0; i < 500; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 0), Is.False);
    }

    [Test]
    public void ShouldYield_treats_a_negative_result_count_as_no_results()
    {
        var budget = new LeafWalkBudget(maxLeaves: 1, maxDuration: null);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: -1), Is.False);
    }

    [Test]
    public void A_non_positive_leaf_budget_disables_the_leaf_bound()
    {
        var budget = new LeafWalkBudget(maxLeaves: 0, maxDuration: null);

        for (var i = 0; i < 10_000; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.False,
            "a misconfigured budget must degrade to the historical unbounded walk, " +
            "never to a silently truncated one");
    }

    [Test]
    public void An_elapsed_deadline_yields_once_a_result_has_been_collected()
    {
        var budget = new LeafWalkBudget(maxLeaves: int.MaxValue, maxDuration: TimeSpan.FromTicks(1));
        budget.RecordLeafVisited();

        Assert.Multiple(() =>
        {
            Assert.That(budget.ShouldYield(resultsCollected: 1), Is.True);
            Assert.That(budget.ShouldYield(resultsCollected: 0), Is.False,
                "the deadline must not override the forward-progress invariant");
        });
    }

    [Test]
    public void A_zero_duration_disables_the_deadline()
    {
        var budget = new LeafWalkBudget(maxLeaves: int.MaxValue, maxDuration: TimeSpan.Zero);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.False);
    }

    [Test]
    public void LeavesVisited_counts_every_recorded_leaf()
    {
        var budget = new LeafWalkBudget(maxLeaves: 100, maxDuration: null);

        for (var i = 0; i < 7; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.LeavesVisited, Is.EqualTo(7));
    }

    [Test]
    public void ForScanPage_reads_the_bound_from_options()
    {
        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = 2,
            MaxScanPageDuration = TimeSpan.Zero,
        };

        var budget = LeafWalkBudget.ForScanPage(options);
        budget.RecordLeafVisited();
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.True);
    }

    [Test]
    public void ForScanPage_rejects_a_null_options_instance()
    {
        Assert.That(() => LeafWalkBudget.ForScanPage(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_shipped_default_leaf_bound_is_well_above_a_dense_scan()
    {
        Assert.That(LatticeOptions.DefaultMaxLeavesPerScanPage, Is.EqualTo(64));
    }
}
