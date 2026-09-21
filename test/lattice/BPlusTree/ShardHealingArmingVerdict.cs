namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// What a single arm of the two-arm healing-arming observation saw.
/// </summary>
/// <param name="Swept">Whether a sweep was ever observed for that tree.</param>
/// <param name="ObservedAfter">
/// How long after the observation window opened the sweep was first seen. When
/// <paramref name="Swept"/> is <see langword="false"/> this is how long the arm
/// looked before giving up. A value at or beyond the budget means the sweep was
/// only picked up by the single read that follows the polling loop, which is the
/// latest moment the arm is capable of observing anything.
/// </param>
internal readonly record struct ShardHealingArmObservation(bool Swept, TimeSpan ObservedAfter);

/// <summary>
/// What a two-arm healing-arming observation actually established.
/// </summary>
internal enum ShardHealingArmingOutcome
{
    /// <summary>Both trees armed. Healing works, including on the read-only tree.</summary>
    Armed,

    /// <summary>
    /// The read-only tree did not arm while a healthy control did. This is the
    /// write-only bootstrap of issue #1877.
    /// </summary>
    ProductDefect,

    /// <summary>
    /// The observation is not dispositive about the product, because the control
    /// arm was itself unhealthy. Says nothing about issue #1877 either way.
    /// </summary>
    HarnessDegraded,
}

/// <summary>
/// Decides what a two-arm shard-healing-arming observation proves.
/// <para>
/// This judgement is deliberately separated from the act of observing it, and
/// that separation is the entire point of the type. The condition that makes the
/// judgement hard - a runner so degraded that the control arm barely arms - can
/// be described far more easily than it can be summoned. Extracted like this it
/// is an argument, so the classifier can be driven through states that would
/// otherwise require reproducing a specific CI machine on a specific evening.
/// </para>
/// <para>
/// The general form, which is why this shape is worth copying: when a control's
/// correctness depends on an environment you cannot summon on demand, extract
/// the judgement from the observation and test the judgement.
/// </para>
/// </summary>
internal static class ShardHealingArmingVerdict
{
    /// <summary>
    /// The share of the arming budget the control arm may consume before the run
    /// is treated as degraded.
    /// <para>
    /// A quarter is not a guess. In a healthy run the control is observed swept
    /// on the first poll: the whole fixture - both arms, cluster calls included -
    /// completed in 0.2955s on run 34773520310's passing re-run, against a 20s
    /// budget. A control that needs 5s has therefore already slowed by more than
    /// an order of magnitude, while still sitting four times below the ~20s the
    /// same fixture recorded when it actually misfired. The threshold has roughly
    /// 17x of headroom beneath it and 4x above it, so it separates the two
    /// populations without sitting near either.
    /// </para>
    /// </summary>
    internal const double ControlBudgetShare = 0.25;

    /// <summary>
    /// The longest the control arm may take to be observed before the observation
    /// stops being evidence about the product.
    /// </summary>
    internal static TimeSpan ControlMarginFor(TimeSpan budget) =>
        budget * ControlBudgetShare;

    /// <summary>
    /// Classifies a two-arm observation.
    /// <para>
    /// The control arm is examined first and on its own. A differential is only a
    /// control against a fault that reaches both arms equally, and these two arms
    /// are not equally reachable: the written tree arms through the write path,
    /// the read-only tree only through activation plus a sweep. So a fault that
    /// is merely *unlucky in its timing* - rather than uniform - lands on one arm
    /// and produces the exact one-swept-one-not shape that issue #1877's
    /// signature has. Establishing that the control was healthy before reading
    /// anything into the subject is what makes the comparison mean what it says.
    /// </para>
    /// </summary>
    /// <param name="readOnlyArm">The tree that only ever served reads - the subject.</param>
    /// <param name="writtenArm">The tree that was written once - the control.</param>
    /// <param name="budget">The arming budget each arm was given.</param>
    internal static ShardHealingArmingOutcome Classify(
        ShardHealingArmObservation readOnlyArm,
        ShardHealingArmObservation writtenArm,
        TimeSpan budget)
    {
        // A control that never armed says healing did not run at all - no
        // reminder service, a cluster that failed to come up, healing disabled.
        // The subject's result carries no information in that state.
        if (!writtenArm.Swept) return ShardHealingArmingOutcome.HarnessDegraded;

        // A control that armed only at the edge of its budget is the case this
        // classifier exists for. It is still a True, so a bare differential reads
        // it as a healthy control and convicts the product; but a control that
        // took four figures longer than nominal was not measuring the product.
        if (writtenArm.ObservedAfter > ControlMarginFor(budget))
            return ShardHealingArmingOutcome.HarnessDegraded;

        return readOnlyArm.Swept
            ? ShardHealingArmingOutcome.Armed
            : ShardHealingArmingOutcome.ProductDefect;
    }
}
