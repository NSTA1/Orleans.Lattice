using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// A Coyote concurrency model of the atomic-action saga's step-sequencing and
/// crash-resume safety, driving the <b>production</b>
/// <see cref="AtomicActionPlanCore"/> under systematic schedule exploration. It
/// runs a fixed plan whose last forward step faults, so the saga pivots to
/// compensation, and it injects a nondeterministic <i>crash before the status
/// mark is persisted</i> at every forward and every compensating effect. Because
/// the production grain persists the per-step status vector and re-derives its
/// next action purely from that vector, a crash is modelled faithfully as
/// "discard the un-persisted mark and re-decide" - an at-least-once effect whose
/// safety the core must still guarantee.
/// <para>
/// The model asserts three safety properties on every explored schedule:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>CompensateInReverse:</b> the index of each compensation decision is
///     non-increasing across the whole run, so committed steps are undone in
///     strict reverse order even when a crash re-attempts a step.
///   </description></item>
///   <item><description>
///     <b>CompensateOnce:</b> no step's compensating effect is persisted twice -
///     once a step is marked <see cref="AtomicActionStepStatus.Compensated"/> the
///     core never selects it again.
///   </description></item>
///   <item><description>
///     <b>NoPartialCommit:</b> the saga only commits when every step is
///     <see cref="AtomicActionStepStatus.ForwardDone"/>, and it only settles the
///     compensated terminal when every committed step has been compensated.
///   </description></item>
/// </list>
/// The <paramref name="useBrokenReverseOrder"/> toggle chooses the compensation
/// rule:
/// <list type="bullet">
///   <item><description>
///     <c>false</c> - the proven core.
///     <see cref="AtomicActionPlanCore.NextCompensationIndex"/> selects the
///     highest-indexed committed step, so compensation runs in strict reverse
///     order on every schedule.
///   </description></item>
///   <item><description>
///     <c>true</c> - the regression: compensate the <i>lowest</i>-indexed
///     committed step first (forward order). Coyote must find a schedule whose
///     compensation index increases and trips the reverse-order assertion, proving
///     the model genuinely exercises the ordering rule.
///   </description></item>
/// </list>
/// </summary>
internal sealed class AtomicActionExecutionModel(bool useBrokenReverseOrder) : ICoyoteModel
{
    private const int StepCount = 4;

    // The last forward step faults, so steps 0..2 commit and then the saga
    // pivots to compensation and must undo them in the order 2, 1, 0.
    private const int FaultAtIndex = StepCount - 1;

    // A generous per-iteration bound: a schedule that always chooses to crash
    // before persisting never makes progress, so we stop exploring it without
    // asserting liveness (only safety is under test here).
    private const int MaxLoopIterations = 128;

    public void Run(ICoyoteRuntime runtime)
    {
        var statuses = new AtomicActionStepStatus[StepCount];
        var persistedCompensated = new bool[StepCount];
        var phase = AtomicActionPhase.Forward;

        // Tracks strict reverse order: every compensation index must be <= the
        // previous one. Seeded above the top index so the first pick always holds.
        var lastCompensationIndex = StepCount;

        for (var iteration = 0; iteration < MaxLoopIterations; iteration++)
        {
            var decision = Decide(statuses, phase, useBrokenReverseOrder);

            if (decision.Kind == AtomicActionActionKind.RunForward)
            {
                if (decision.Index == FaultAtIndex)
                {
                    // The forward effect faults; the grain pivots to compensation.
                    phase = AtomicActionPhase.Compensate;
                    continue;
                }

                // The forward effect ran. A crash before the mark is persisted
                // leaves the step Pending, so the resumed saga re-runs it
                // (at-least-once forward), which is safe and not a violation.
                if (!runtime.RandomBoolean())
                {
                    statuses[decision.Index] = AtomicActionStepStatus.ForwardDone;
                }

                continue;
            }

            if (decision.Kind == AtomicActionActionKind.Commit)
            {
                for (var i = 0; i < statuses.Length; i++)
                {
                    Specification.Assert(
                        statuses[i] == AtomicActionStepStatus.ForwardDone,
                        "partial commit: the saga committed with a step that was not ForwardDone");
                }

                return;
            }

            if (decision.Kind == AtomicActionActionKind.Compensate)
            {
                Specification.Assert(
                    decision.Index <= lastCompensationIndex,
                    $"compensation ran out of reverse order: index {decision.Index} " +
                    $"came after index {lastCompensationIndex}");
                lastCompensationIndex = decision.Index;

                // The compensating effect ran. A crash before the mark is
                // persisted leaves the step ForwardDone, so the resumed saga
                // re-attempts the same index (at-least-once compensate).
                if (!runtime.RandomBoolean())
                {
                    Specification.Assert(
                        !persistedCompensated[decision.Index],
                        $"compensation ran twice for step {decision.Index}");
                    persistedCompensated[decision.Index] = true;
                    statuses[decision.Index] = AtomicActionStepStatus.Compensated;
                }

                continue;
            }

            if (decision.Kind == AtomicActionActionKind.SettleCompensated)
            {
                for (var i = 0; i < FaultAtIndex; i++)
                {
                    Specification.Assert(
                        persistedCompensated[i],
                        $"the saga settled Compensated but step {i} was never compensated");
                }

                return;
            }

            // AtomicActionActionKind.None - already terminal, nothing to do.
            return;
        }
    }

    /// <summary>
    /// Resolves the next action. The forward path and the terminal settlements are
    /// always the production core; only the compensation pick is swapped for the
    /// broken forward-order rule when <paramref name="broken"/> is set.
    /// </summary>
    private static AtomicActionDecision Decide(
        ReadOnlySpan<AtomicActionStepStatus> statuses,
        AtomicActionPhase phase,
        bool broken)
    {
        if (!broken || phase != AtomicActionPhase.Compensate)
        {
            return AtomicActionPlanCore.Decide(statuses, phase);
        }

        var lowest = LowestForwardDone(statuses);
        return lowest >= 0
            ? new AtomicActionDecision(AtomicActionActionKind.Compensate, lowest)
            : new AtomicActionDecision(AtomicActionActionKind.SettleCompensated, -1);
    }

    /// <summary>
    /// The BROKEN compensation rule (guard fixture only): the lowest-indexed
    /// committed step, i.e. compensation in forward order. Coyote surfaces a
    /// schedule whose compensation indices increase and trips the reverse-order
    /// assertion.
    /// </summary>
    private static int LowestForwardDone(ReadOnlySpan<AtomicActionStepStatus> statuses)
    {
        for (var i = 0; i < statuses.Length; i++)
        {
            if (statuses[i] == AtomicActionStepStatus.ForwardDone)
            {
                return i;
            }
        }

        return -1;
    }
}
