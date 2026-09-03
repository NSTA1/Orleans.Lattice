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

    // --- Background coordinator drains (issue 1973) ---

    [Test]
    public void ForBackgroundDrain_reads_the_bound_from_options()
    {
        var options = new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 2,
            BackgroundDrainMaxDuration = TimeSpan.Zero,
        };

        var budget = LeafWalkBudget.ForBackgroundDrain(options);
        budget.RecordLeafVisited();
        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.False);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.True);
    }

    [Test]
    public void ForBackgroundDrain_rejects_a_null_options_instance()
    {
        Assert.That(() => LeafWalkBudget.ForBackgroundDrain(null!), Throws.ArgumentNullException);
    }

    /// <summary>
    /// The tombstone compactor and the shard consolidator keep their own
    /// long-standing per-pass leaf knobs and inherit only the shared wall-clock
    /// net, so the explicit-cap overload must honour the cap it is given rather
    /// than the shared default.
    /// </summary>
    [Test]
    public void ForBackgroundDrain_with_an_explicit_cap_ignores_the_shared_leaf_default()
    {
        var options = new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 64,
            BackgroundDrainMaxDuration = TimeSpan.Zero,
        };

        var budget = LeafWalkBudget.ForBackgroundDrain(maxLeaves: 1, options);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.True);
    }

    [Test]
    public void ForBackgroundDrain_with_an_explicit_cap_rejects_a_null_options_instance()
    {
        Assert.That(() => LeafWalkBudget.ForBackgroundDrain(1, null!), Throws.ArgumentNullException);
    }

    /// <summary>
    /// The deliberately-atomic sweeps express their intent as an unbounded
    /// budget rather than as a second, un-budgeted copy of the walk, so they
    /// must never authorise a yield however many leaves they visit.
    /// </summary>
    [Test]
    public void Unbounded_never_yields()
    {
        var budget = LeafWalkBudget.Unbounded();

        for (var i = 0; i < 10_000; i++)
            budget.RecordLeafVisited();

        Assert.Multiple(() =>
        {
            Assert.That(budget.ShouldYield(resultsCollected: 1), Is.False);
            Assert.That(budget.LeavesVisited, Is.EqualTo(10_000));
        });
    }

    [Test]
    public void A_non_positive_background_leaf_bound_disables_the_bound()
    {
        var options = new LatticeOptions
        {
            BackgroundDrainLeavesPerPass = 0,
            BackgroundDrainMaxDuration = TimeSpan.Zero,
        };

        var budget = LeafWalkBudget.ForBackgroundDrain(options);
        for (var i = 0; i < 1_000; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(resultsCollected: 1), Is.False,
            "a misconfigured bound must degrade to the unbounded walk, never to a truncated one");
    }

    [Test]
    public void The_shipped_background_drain_defaults_are_the_documented_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeOptions.DefaultBackgroundDrainLeavesPerPass, Is.EqualTo(64));
            Assert.That(LatticeOptions.DefaultBackgroundDrainMaxDuration, Is.EqualTo(TimeSpan.FromSeconds(10)));
        });
    }
}
