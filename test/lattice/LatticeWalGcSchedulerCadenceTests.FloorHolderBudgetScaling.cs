using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the floor-holder remedy budgets of issue #3279: that they scale
/// with the tree's measured floor-holding pin population, and that they are
/// nonetheless hard-capped.
/// <para>
/// The defect these assert against is a <i>rate</i>, not a bound. A tree whose
/// WAL floor is held by a large pin population had its remedy issued at a fixed
/// four leaf drives per pass, sourced from a candidate pool fixed at eight. That
/// is a constant, and a constant cannot converge against a WAL head that is
/// moving. Measured in the field on the <c>repo-context-vector-index</c> tree:
/// the head advanced at 79.3 offsets per minute while the floor advanced at
/// 0.148, so the gap between them widened from 11,098 to 26,603 offsets over
/// 196 minutes rather than closing. Every one of the 88 drives in that window
/// succeeded and each carried its pin the entire gap in a single activation, so
/// per-drive effectiveness was already 100% and the deficit was entirely in how
/// many candidates a pass could consider.
/// </para>
/// <para>
/// The assertions are deliberately two-sided, for the same reason the sibling
/// throughput fixture of issue #2768 gives: a test that only asserted "the
/// budget grows" would be satisfied by an unbounded sweep, which is a far
/// larger blast radius than the defect, because a touch is a concurrent grain
/// activation and a WAL replay. A test that only asserted the cap would be
/// satisfied by the starved behaviour this change exists to remove. Both ends
/// are pinned, and so is the fail-safe middle: a tree with no measured
/// population gets exactly the historical constant.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The candidate-pool budget at a given population, named so the intent of
    /// each case reads as a population rather than as four positional numbers.
    /// </summary>
    private static int CandidateBudgetAt(int population) =>
        LatticeWalGcScheduler.ScaleFloorHolderBudget(
            population,
            LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep,
            LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep,
            LatticeWalGcScheduler.FloorHolderPinsPerRemedyCandidate);

    /// <summary>The leaf-drive budget at a given population.</summary>
    private static int TouchBudgetAt(int population) =>
        LatticeWalGcScheduler.ScaleFloorHolderBudget(
            population,
            LatticeWalGcScheduler.MaxReactivationTouchesPerPass,
            LatticeWalGcScheduler.MaxReactivationTouchesCeiling,
            LatticeWalGcScheduler.FloorHolderPinsPerReactivationTouch);

    // ------------------------------------------------------------ fail-safe

    [Test]
    public void An_unmeasured_tree_is_budgeted_at_exactly_the_historical_constant()
    {
        // The direction every unknown must fail in. The population is read from
        // the PREVIOUS sweep, because the pool has to be sized before the
        // enumeration that counts it completes, so the first sweep on any tree -
        // and every sweep on a tree whose sweep faulted before it could record -
        // has no measurement at all. That case must reproduce the shipped
        // behaviour exactly, not approximate it, so that this change cannot
        // alter a tree it has no evidence about.
        Assert.Multiple(() =>
        {
            Assert.That(
                CandidateBudgetAt(0),
                Is.EqualTo(LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep),
                "an unmeasured tree must admit exactly the historical candidate pool.");
            Assert.That(
                TouchBudgetAt(0),
                Is.EqualTo(LatticeWalGcScheduler.MaxReactivationTouchesPerPass),
                "an unmeasured tree must drive exactly the historical number of leaves.");
        });
    }

    [Test]
    public void A_negative_population_cannot_shrink_a_budget_below_the_historical_constant()
    {
        // A population is a count and cannot legitimately be negative, which is
        // exactly why it is worth pinning: the clamp is the whole safety
        // argument for scaling a budget at all, and an argument that holds only
        // for the inputs anyone expected is not one. Integer division of a
        // negative by a positive truncates toward zero, so an unclamped
        // implementation would return zero here - a budget that silently
        // disables the remedy outright rather than merely starving it.
        Assert.Multiple(() =>
        {
            Assert.That(
                CandidateBudgetAt(-1),
                Is.EqualTo(LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep));
            Assert.That(
                TouchBudgetAt(int.MinValue),
                Is.EqualTo(LatticeWalGcScheduler.MaxReactivationTouchesPerPass));
        });
    }

    [Test]
    public void A_healthy_tree_is_budgeted_at_exactly_the_historical_constant()
    {
        // The blast-radius clause. Every tree in the live estate other than the
        // floor-held ones carries a floor-holding pin population in the low
        // hundreds at most, so the scaling must be inert across all of them: the
        // change is meant to reach the pathological tree and nothing else. A
        // population of 511 is one short of a single additional touch and well
        // short of a ninth candidate, and must therefore be indistinguishable
        // from the shipped behaviour.
        Assert.Multiple(() =>
        {
            Assert.That(
                CandidateBudgetAt(511),
                Is.EqualTo(LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep));
            Assert.That(
                TouchBudgetAt(511),
                Is.EqualTo(LatticeWalGcScheduler.MaxReactivationTouchesPerPass));
        });
    }

    // --------------------------------------------------------------- scaling

    [Test]
    public void The_candidate_pool_scales_with_the_floor_holding_pin_population()
    {
        // The half of the fix that actually matters, because the candidate pool
        // was the binding constraint and the drive budget was not. Measured over
        // 43 consecutive sweeps on the live tree, the classification sample was
        // saturated at exactly 8 distinct leaves on 43 of 43, while leaves
        // driven averaged 1.79 against a cap of 4. Raising the drive budget
        // alone would therefore have achieved precisely nothing: the pass was
        // never short of permission to drive, it was short of candidates to
        // drive, because a pool of 8 refilled from the bottom of the offset
        // order holds mostly candidates too young to drive or still cooling.
        Assert.Multiple(() =>
        {
            Assert.That(CandidateBudgetAt(2_048), Is.EqualTo(16));
            Assert.That(CandidateBudgetAt(4_096), Is.EqualTo(32));
            Assert.That(
                CandidateBudgetAt(20_992),
                Is.EqualTo(164),
                "the live tree's measured population of 20,992 pins must widen the pool, not saturate the ceiling - a fix that is already at its cap on the tree that motivated it has no headroom left for a worse one.");
        });
    }

    [Test]
    public void The_drive_budget_scales_with_the_floor_holding_pin_population()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TouchBudgetAt(4_096), Is.EqualTo(8));
            Assert.That(TouchBudgetAt(8_192), Is.EqualTo(16));
            Assert.That(
                TouchBudgetAt(20_992),
                Is.EqualTo(LatticeWalGcScheduler.MaxReactivationTouchesCeiling),
                "the live tree's population is past the drive ceiling, which is the intended shape: the drive budget is a direct bound on concurrent grain activations and WAL replays, so it is capped far harder than the pool it draws from.");
        });
    }

    [Test]
    public void Both_budgets_are_monotone_in_the_population()
    {
        // A budget that could fall as the deficit grew would be a worse defect
        // than the one being fixed, and integer division makes that easy to
        // introduce by accident when a clamp is written as a subtraction. Walked
        // across the whole interesting range rather than sampled at the
        // boundaries, so a non-monotone step anywhere inside is caught.
        var previousCandidates = CandidateBudgetAt(0);
        var previousTouches = TouchBudgetAt(0);
        var steps = 0;

        for (var population = 0; population <= 200_000; population += 97)
        {
            var candidates = CandidateBudgetAt(population);
            var touches = TouchBudgetAt(population);
            steps++;

            Assert.That(
                candidates,
                Is.GreaterThanOrEqualTo(previousCandidates),
                $"the candidate pool fell as the population rose, at {population} pins.");
            Assert.That(
                touches,
                Is.GreaterThanOrEqualTo(previousTouches),
                $"the drive budget fell as the population rose, at {population} pins.");

            previousCandidates = candidates;
            previousTouches = touches;
        }

        Assert.That(steps, Is.GreaterThan(2_000), "the walk must actually have covered the range.");
    }

    [Test]
    public void The_candidate_pool_always_exceeds_the_drive_budget()
    {
        // The invariant the two budgets have to hold jointly, and the one that
        // stops a future edit to either constant from reintroducing the defect
        // from the other side. The pass drives leaves selected out of the
        // candidate pool, so a pool narrower than the drive budget would starve
        // the drive no matter how large the drive budget was - which is exactly
        // the shape the live tree was in, with a pool saturated at 8 and drives
        // averaging 1.79 against a cap of 4.
        for (var population = 0; population <= 200_000; population += 89)
        {
            Assert.That(
                CandidateBudgetAt(population),
                Is.GreaterThan(TouchBudgetAt(population)),
                $"at {population} pins the candidate pool was not wider than the drive budget it feeds.");
        }
    }

    // --------------------------------------------------------------- ceiling

    [Test]
    public void An_absurd_population_cannot_lift_either_budget_past_its_ceiling()
    {
        // The bound clause, asserted against populations no tree will ever
        // carry. A touch is a concurrent grain activation and a WAL replay
        // issued under Task.WhenAll, so an uncapped drive budget would convert a
        // WAL-growth defect into a thundering-herd one against the very silo
        // already struggling to keep its floor moving. int.MaxValue is included
        // because a scaling expression that multiplied before dividing would
        // overflow to a negative there and, unclamped, produce a budget of zero.
        var absurd = new[] { 1_000_000, 100_000_000, int.MaxValue };

        Assert.Multiple(() =>
        {
            foreach (var population in absurd)
            {
                Assert.That(
                    CandidateBudgetAt(population),
                    Is.EqualTo(LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep),
                    $"the candidate pool escaped its ceiling at {population} pins.");
                Assert.That(
                    TouchBudgetAt(population),
                    Is.EqualTo(LatticeWalGcScheduler.MaxReactivationTouchesCeiling),
                    $"the drive budget escaped its ceiling at {population} pins.");
            }
        });
    }

    [Test]
    public void The_remedy_candidate_pool_is_severed_from_the_diagnostic_read_budget()
    {
        // The root error, stated as an assertion rather than only in the commit
        // message. MaxFloorHolderClassificationsPerSweep is a DIAGNOSTIC budget:
        // it bounds how many durable pin reads a sweep spends to be able to name
        // what holds the floor, and 8 is a reasonable sample for that. It was
        // also, and only incidentally, the set the remedy drew its candidates
        // from - so a bound chosen to limit reads silently became a bound on how
        // fast the tree could be repaired. The two are now distinct quantities,
        // and this pins that they cannot collapse back into one.
        Assert.That(
            LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep,
            Is.GreaterThan(LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep),
            "the remedy's candidate ceiling must not be the diagnostic read budget.");
    }

    // ----------------------------------------------------------- behavioural

    /// <summary>
    /// Large enough that both budgets scale off it: 4,096 pins is 32 candidates
    /// and 8 drives, against the historical 8 and 4. Chosen as the smallest
    /// population that doubles <i>both</i>, so the fixture cannot pass by
    /// widening one and leaving the other inert.
    /// </summary>
    private const int ScalingPopulation = 4_096;

    /// <summary>
    /// Seeds <see cref="ScalingPopulation"/> repairable dormant floor holders,
    /// which is the live tree's shape in miniature.
    /// </summary>
    private static (FakePinStore Pins, LeafStateBook Storage) ScalingPopulationSeed()
    {
        var storage = new LeafStateBook();
        var pins = new FakePinStore();

        for (var i = 0; i < ScalingPopulation; i++)
        {
            storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(i), UnusablePin);
        }

        return (pins, storage);
    }

    [Test]
    public async Task A_large_floor_holding_population_widens_the_pool_and_the_drive_beyond_the_historical_constants()
    {
        // The half of this change that could silently be inert, and therefore
        // the half that has to be asserted end to end rather than in arithmetic.
        // The budgets are scaled by a population the sweep itself records, so if
        // that recording is ever dropped - or sited where an early return skips
        // it - every budget falls back to its historical constant, the whole
        // change becomes a no-op, and every arithmetic assertion above still
        // passes. Only driving a real sweep against a real population can tell
        // the two apart.
        //
        // The population is deliberately read from the PREVIOUS sweep, because
        // the pool must be sized before the enumeration that counts it has
        // finished, so the widening appears from the second classifying sweep
        // onward rather than the first. That is why this asserts over a window
        // of passes and not on the first one.
        var (pins, storage) = ScalingPopulationSeed();
        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);

        var afterFirstDrive = leaves.Touched.Distinct().Count();

        // Walk further passes, recording how many leaves each one drove, so the
        // drive budget can be asserted as a per-pass RATE. A cumulative total
        // cannot distinguish a widened per-pass budget from the historical one
        // applied over more passes, and it is the rate that the moving WAL head
        // has to be outrun by.
        var running = leaves.Touched.Count;
        var busiestPass = 0;
        var guard = 0;

        while (guard < 500
            && (leaves.Touched.Distinct().Count() <= FloorHolderCap || busiestPass <= TouchesPerPass))
        {
            await TickAsync(time);
            guard++;

            var total = leaves.Touched.Count;
            var drovenThisPass = total - running;
            running = total;

            if (drovenThisPass > busiestPass)
            {
                busiestPass = drovenThisPass;
            }
        }

        var distinct = leaves.Touched.Distinct().Count();
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                afterFirstDrive,
                Is.GreaterThan(0),
                "precondition: the repair path must be running at all. A zero here means the fixture is "
                    + "measuring nothing and the assertions below would be vacuous.");
            Assert.That(
                distinct,
                Is.GreaterThan(FloorHolderCap),
                "a tree carrying 4,096 floor-holding pins must draw candidates from a pool wider than the "
                    + "diagnostic sample of 8. This is the defect itself: measured on the live tree the "
                    + "classification sample was saturated at exactly 8 distinct leaves on 43 of 43 "
                    + "consecutive sweeps while drives averaged 1.79 against a cap of 4, so the pass was "
                    + "never short of permission to drive - it was short of candidates to drive.");
            Assert.That(
                busiestPass,
                Is.GreaterThan(TouchesPerPass),
                "and at least one pass must drive more leaves than the historical per-pass constant. "
                    + "Widening the pool without widening the drive would leave the second half of the "
                    + "change inert, and a cumulative count could not tell the two apart.");
            Assert.That(
                busiestPass,
                Is.LessThanOrEqualTo(TouchBudgetAt(ScalingPopulation)),
                "without exceeding the scaled drive budget. The bound is what keeps a WAL-growth defect "
                    + "from becoming a thundering-herd one: every drive is a concurrent grain activation "
                    + "ending in a durable write.");
            Assert.That(
                distinct,
                Is.LessThanOrEqualTo(CandidateBudgetAt(ScalingPopulation)),
                "and the driven set must stay inside the scaled pool it was selected from.");
        });
    }

    [Test]
    public async Task A_small_floor_holding_population_leaves_the_historical_drive_cap_exactly_where_it_was()
    {
        // The control, and the blast-radius clause asserted behaviourally rather
        // than only in arithmetic. Sixty repairable pins is the population every
        // pre-existing fixture in this file was written against, and it must
        // drive exactly the historical four per pass - unchanged, not merely
        // close. Without this, a scaling expression that quietly widened every
        // tree in the estate would pass the fixture above and be caught by
        // nothing here.
        //
        // The EXACTNESS is deliberate, not an oversight. Asserting equality
        // means that retuning MaxReactivationTouchesPerPass fails this test,
        // which is the point: a fleet-wide scheduler changing behaviour on every
        // healthy tree in the estate is exactly the outcome that should not pass
        // silently. Delete or relax this assertion on purpose or not at all.
        const int Population = 60;

        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(i), UnusablePin);
        }

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);

        var guard = 0;
        while (leaves.Touched.Count == 0)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(500), "the sweep never drove anything at all.");
        }

        var firstPass = leaves.Touched.Count;
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                TouchBudgetAt(Population),
                Is.EqualTo(TouchesPerPass),
                "precondition: a population this size must not scale at all.");
            Assert.That(
                firstPass,
                Is.EqualTo(TouchesPerPass),
                "a healthy tree must drive exactly the shipped number of leaves per pass. This change is "
                    + "meant to reach the pathological tree and nothing else.");
        });
    }
}
