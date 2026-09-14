using System;

using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Drives <see cref="ShardHealingArmingVerdict"/> through the states a CI runner
/// can put the two-arm healing observation into, including the degraded state
/// that a real run reached and that a bare differential misreads as a product
/// defect.
/// <para>
/// These are pure in-process tests by design. The behaviour under examination is
/// what the fixture concludes when its control arm is unhealthy, and a control
/// arm is only unhealthy when the machine underneath it is - which is not a
/// condition a test can create. Passing the observation in as a value is what
/// makes the conclusion testable at all.
/// </para>
/// </summary>
[TestFixture]
public class ShardHealingArmingVerdictTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(20);

    [Test]
    public void Both_arms_observed_promptly_is_armed()
    {
        var verdict = ShardHealingArmingVerdict.Classify(
            new ShardHealingArmObservation(Swept: true, TimeSpan.FromMilliseconds(250)),
            new ShardHealingArmObservation(Swept: true, TimeSpan.FromMilliseconds(250)),
            Budget);

        Assert.That(verdict, Is.EqualTo(ShardHealingArmingOutcome.Armed));
    }

    [Test]
    public void A_prompt_control_beside_an_unswept_subject_is_the_product_defect()
    {
        // The positive control for the whole classifier: the issue #1877
        // signature must still be recognised when it is genuinely present,
        // otherwise the degraded-run handling below would have bought its
        // safety by making the fixture unable to fail at all.
        var verdict = ShardHealingArmingVerdict.Classify(
            new ShardHealingArmObservation(Swept: false, Budget),
            new ShardHealingArmObservation(Swept: true, TimeSpan.FromMilliseconds(250)),
            Budget);

        Assert.That(verdict, Is.EqualTo(ShardHealingArmingOutcome.ProductDefect));
    }

    [Test]
    public void A_control_that_never_armed_is_a_harness_fault()
    {
        var verdict = ShardHealingArmingVerdict.Classify(
            new ShardHealingArmObservation(Swept: false, Budget),
            new ShardHealingArmObservation(Swept: false, Budget),
            Budget);

        Assert.That(verdict, Is.EqualTo(ShardHealingArmingOutcome.HarnessDegraded));
    }

    [Test]
    public void A_control_observed_only_at_the_budget_edge_is_not_dispositive()
    {
        // Swept is True, so a bare differential reads this as a healthy control
        // and convicts the product. It is the case the classifier exists for.
        var verdict = ShardHealingArmingVerdict.Classify(
            new ShardHealingArmObservation(Swept: false, Budget),
            new ShardHealingArmObservation(Swept: true, Budget),
            Budget);

        Assert.That(verdict, Is.EqualTo(ShardHealingArmingOutcome.HarnessDegraded));
    }

    [Test]
    public void The_incident_on_run_34773520310_classifies_as_harness_degraded()
    {
        // Known-answer cross-check against a real failure rather than an
        // invented one. On run 34773520310 the fixture reported the read-only
        // tree unarmed beside a written tree that was armed, and the test took
        // 48.925s against a 20s-per-arm budget - so neither arm returned early
        // and the control was picked up only by the read that follows the
        // polling loop. The same fixture's passing re-run took 0.2955s in
        // total, so nominal for the control is the first poll.
        //
        // The control is modelled at exactly the budget, which is the *earliest*
        // elapsed value consistent with "observed only on the final read". If
        // the earliest consistent value classifies as degraded then so does
        // every later one, so the conclusion does not rest on the decomposition
        // of the 48.925s being exact.
        var verdict = ShardHealingArmingVerdict.Classify(
            new ShardHealingArmObservation(Swept: false, Budget),
            new ShardHealingArmObservation(Swept: true, Budget),
            Budget);

        Assert.That(
            verdict,
            Is.EqualTo(ShardHealingArmingOutcome.HarnessDegraded),
            "run 34773520310 was a degraded runner, not evidence about issue #1877");
    }

    [Test]
    public void The_control_margin_sits_far_above_the_measured_healthy_control_latency()
    {
        var margin = ShardHealingArmingVerdict.ControlMarginFor(Budget);

        // 0.2955s is the whole passing re-run of the integration fixture on run
        // 34773520310 - both arms and every cluster call inside it - so it is a
        // strict upper bound on a healthy control arm alone.
        var measuredHealthyUpperBound = TimeSpan.FromMilliseconds(295.5);

        Assert.Multiple(() =>
        {
            Assert.That(
                margin,
                Is.GreaterThan(measuredHealthyUpperBound * 10),
                "the margin must not sit close enough to nominal to convert a slow-but-sound run into a failure");
            Assert.That(
                margin,
                Is.LessThan(Budget),
                "a margin at the budget would admit a control observed only on the final read");
        });
    }

    [Test]
    public void A_subject_slower_than_a_prompt_control_but_within_budget_is_still_armed()
    {
        // The read-only tree arms through activation plus a sweep, so it is
        // legitimately slower than the written tree. Lateness alone is not a
        // defect while it stays inside the budget.
        var verdict = ShardHealingArmingVerdict.Classify(
            new ShardHealingArmObservation(Swept: true, TimeSpan.FromSeconds(12)),
            new ShardHealingArmObservation(Swept: true, TimeSpan.FromMilliseconds(250)),
            Budget);

        Assert.That(verdict, Is.EqualTo(ShardHealingArmingOutcome.Armed));
    }
}
