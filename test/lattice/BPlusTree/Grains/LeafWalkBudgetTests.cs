using System.Diagnostics;
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

        Assert.That(budget.ShouldYield(), Is.False);
    }

    [Test]
    public void ShouldYield_is_true_once_the_leaf_budget_is_spent()
    {
        var budget = new LeafWalkBudget(maxLeaves: 3, maxDuration: null);

        for (var i = 0; i < 3; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(), Is.True);
    }

    /// <summary>
    /// The regression this budget exists for. An earlier revision gated
    /// <see cref="LeafWalkBudget.ShouldYield"/> behind a positive result count,
    /// which disarmed both the leaf cap and the deadline for precisely the run
    /// of leaves they are there to bound - a sterile run whose rows are all
    /// tombstoned, TTL-expired, moved away, or predicate-rejected. One page fill
    /// could then hold a non-reentrant shard for minutes (issue 1992). The
    /// budget is now a pure work question; naming a resume position is the call
    /// site's job.
    /// </summary>
    [Test]
    public void ShouldYield_is_true_on_a_sterile_run_that_collected_nothing()
    {
        var budget = new LeafWalkBudget(maxLeaves: 1, maxDuration: null);

        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(), Is.True,
            "the leaf cap must fire on a run of leaves that yields no rows at all");
    }

    /// <summary>
    /// The deadline arm of the same regression: an elapsed deadline must yield
    /// on a sterile run too.
    /// <para>
    /// The clock is started explicitly in the past rather than measuring a tiny
    /// budget from now. A one-tick budget resolves to a deadline roughly 100ns
    /// away, which the single <see cref="LeafWalkBudget.RecordLeafVisited"/>
    /// below can win the race against, so the test flaked on CI. Backdating the
    /// start makes the deadline unambiguously elapsed and removes the timing
    /// dependence entirely.
    /// </para>
    /// </summary>
    [Test]
    public void An_elapsed_deadline_yields_even_with_nothing_collected()
    {
        var budget = new LeafWalkBudget(
            maxLeaves: int.MaxValue,
            maxDuration: TimeSpan.FromMilliseconds(1),
            startTimestamp: Stopwatch.GetTimestamp() - Stopwatch.Frequency);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(), Is.True);
    }

    [Test]
    public void A_non_positive_leaf_budget_disables_the_leaf_bound()
    {
        var budget = new LeafWalkBudget(maxLeaves: 0, maxDuration: null);

        for (var i = 0; i < 10_000; i++)
            budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(), Is.False,
            "a misconfigured budget must degrade to the historical unbounded walk, " +
            "never to a silently truncated one");
    }

    [Test]
    public void A_zero_duration_disables_the_deadline()
    {
        var budget = new LeafWalkBudget(maxLeaves: int.MaxValue, maxDuration: TimeSpan.Zero);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(), Is.False);
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

        Assert.That(budget.ShouldYield(), Is.True);
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
        Assert.That(budget.ShouldYield(), Is.False);
        budget.RecordLeafVisited();

        Assert.That(budget.ShouldYield(), Is.True);
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

        Assert.That(budget.ShouldYield(), Is.True);
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
            Assert.That(budget.ShouldYield(), Is.False);
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

        Assert.That(budget.ShouldYield(), Is.False,
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

    [Test]
    public void ForScanPage_measures_the_deadline_from_the_supplied_start_clock()
    {
        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = int.MaxValue,
            MaxScanPageDuration = TimeSpan.FromMilliseconds(50),
        };

        // The shape of a page fill whose prologue - preparing the grain and
        // traversing down to the start leaf - already held the shard for longer
        // than the whole page budget before the loop was ever reached.
        var startedHalfASecondAgo =
            LeafWalkBudget.StartClock() - (long)(0.5 * System.Diagnostics.Stopwatch.Frequency);

        var budget = LeafWalkBudget.ForScanPage(options, startedHalfASecondAgo);

        Assert.That(budget.ShouldYield(), Is.True,
            "time already spent holding the shard must count against the page budget");
    }

    [Test]
    public void ForScanPage_without_a_start_clock_measures_from_construction()
    {
        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = int.MaxValue,
            MaxScanPageDuration = TimeSpan.FromSeconds(30),
        };

        var budget = LeafWalkBudget.ForScanPage(options);

        Assert.That(budget.ShouldYield(), Is.False);
    }

    [Test]
    public void A_zero_start_clock_is_treated_as_now_so_an_unstamped_caller_is_unchanged()
    {
        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = int.MaxValue,
            MaxScanPageDuration = TimeSpan.FromSeconds(30),
        };

        var explicitlyUnstamped = LeafWalkBudget.ForScanPage(options, startTimestamp: 0L);

        Assert.That(explicitlyUnstamped.ShouldYield(), Is.False,
            "0 means 'measure from now', not 'the epoch', which would expire every budget");
    }

    [Test]
    public void A_start_clock_does_not_disturb_the_leaf_bound()
    {
        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = 2,
            MaxScanPageDuration = TimeSpan.FromHours(1),
        };

        var budget = LeafWalkBudget.ForScanPage(options, LeafWalkBudget.StartClock());

        budget.RecordLeafVisited();
        Assert.That(budget.ShouldYield(), Is.False);

        budget.RecordLeafVisited();
        Assert.That(budget.ShouldYield(), Is.True);
    }
}
