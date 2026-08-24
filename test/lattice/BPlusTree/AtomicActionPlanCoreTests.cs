using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="AtomicActionPlanCore"/> - the
/// shared correctness core the production atomic-action coordinator
/// (<c>AtomicActionGrain</c>) and the Coyote saga model both execute to decide
/// forward progress, reverse-order compensation, and idempotent crash-resume.
/// These pin the exact truth table (forward-in-order, compensate-in-reverse each
/// step once, commit only on a full forward set, terminal quiescence) so a change
/// to the rule is caught here (and by the Coyote model) rather than only by a slow
/// integration run.
/// </summary>
[TestFixture]
public sealed class AtomicActionPlanCoreTests
{
    private const AtomicActionStepStatus P = AtomicActionStepStatus.Pending;
    private const AtomicActionStepStatus F = AtomicActionStepStatus.ForwardDone;
    private const AtomicActionStepStatus C = AtomicActionStepStatus.Compensated;

    // --- NextForwardIndex ---

    [Test]
    public void NextForwardIndex_empty_span_returns_negative_one()
    {
        Assert.That(AtomicActionPlanCore.NextForwardIndex([]), Is.EqualTo(-1));
    }

    [Test]
    public void NextForwardIndex_all_pending_returns_zero()
    {
        Assert.That(AtomicActionPlanCore.NextForwardIndex([P, P, P]), Is.EqualTo(0));
    }

    [Test]
    public void NextForwardIndex_returns_first_pending_after_forward_done_prefix()
    {
        Assert.That(AtomicActionPlanCore.NextForwardIndex([F, F, P, P]), Is.EqualTo(2));
    }

    [Test]
    public void NextForwardIndex_all_forward_done_returns_negative_one()
    {
        Assert.That(AtomicActionPlanCore.NextForwardIndex([F, F, F]), Is.EqualTo(-1));
    }

    // --- NextCompensationIndex ---

    [Test]
    public void NextCompensationIndex_none_forward_done_returns_negative_one()
    {
        Assert.That(AtomicActionPlanCore.NextCompensationIndex([P, P]), Is.EqualTo(-1));
        Assert.That(AtomicActionPlanCore.NextCompensationIndex([C, C]), Is.EqualTo(-1));
    }

    [Test]
    public void NextCompensationIndex_returns_highest_forward_done()
    {
        Assert.That(AtomicActionPlanCore.NextCompensationIndex([F, F, F, P]), Is.EqualTo(2));
    }

    [Test]
    public void NextCompensationIndex_skips_already_compensated_tail()
    {
        // Steps 3,4 already compensated; the next reverse target is the highest
        // remaining ForwardDone, index 2.
        Assert.That(AtomicActionPlanCore.NextCompensationIndex([F, F, F, C, C]), Is.EqualTo(2));
    }

    // --- Decide: forward phase ---

    [Test]
    public void Decide_forward_with_pending_runs_next_forward()
    {
        var decision = AtomicActionPlanCore.Decide([F, P, P], AtomicActionPhase.Forward);
        Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.RunForward));
        Assert.That(decision.Index, Is.EqualTo(1));
    }

    [Test]
    public void Decide_forward_all_done_commits()
    {
        var decision = AtomicActionPlanCore.Decide([F, F, F], AtomicActionPhase.Forward);
        Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.Commit));
        Assert.That(decision.Index, Is.EqualTo(-1));
    }

    // --- Decide: compensate phase ---

    [Test]
    public void Decide_compensate_runs_highest_forward_done_first()
    {
        var decision = AtomicActionPlanCore.Decide([F, F, P], AtomicActionPhase.Compensate);
        Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.Compensate));
        Assert.That(decision.Index, Is.EqualTo(1));
    }

    [Test]
    public void Decide_compensate_no_forward_done_settles_compensated()
    {
        var decision = AtomicActionPlanCore.Decide([C, C, P], AtomicActionPhase.Compensate);
        Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.SettleCompensated));
        Assert.That(decision.Index, Is.EqualTo(-1));
    }

    // --- Decide: terminal phases ---

    [Test]
    public void Decide_terminal_phase_yields_none()
    {
        foreach (var phase in new[]
        {
            AtomicActionPhase.Committed,
            AtomicActionPhase.Compensated,
            AtomicActionPhase.CompensationFailed,
        })
        {
            var decision = AtomicActionPlanCore.Decide([F, F], phase);
            Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.None));
            Assert.That(decision.Index, Is.EqualTo(-1));
        }
    }

    // --- Reverse-order compensation, each step exactly once ---

    [Test]
    public void Compensation_visits_every_forward_done_step_exactly_once_in_reverse()
    {
        // Simulate the grain loop: all three forward steps committed, a fourth
        // faulted, so we compensate 2, 1, 0 in that order and never revisit.
        AtomicActionStepStatus[] statuses = [F, F, F, P];
        var visited = new List<int>();

        while (true)
        {
            var decision = AtomicActionPlanCore.Decide(statuses, AtomicActionPhase.Compensate);
            if (decision.Kind == AtomicActionActionKind.SettleCompensated)
            {
                break;
            }

            Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.Compensate));
            visited.Add(decision.Index);
            statuses[decision.Index] = C;
        }

        Assert.That(visited, Is.EqualTo(new[] { 2, 1, 0 }));
    }

    // --- Forward progress in ascending order ---

    [Test]
    public void Forward_progress_visits_every_step_once_in_ascending_order()
    {
        AtomicActionStepStatus[] statuses = [P, P, P];
        var visited = new List<int>();

        while (true)
        {
            var decision = AtomicActionPlanCore.Decide(statuses, AtomicActionPhase.Forward);
            if (decision.Kind == AtomicActionActionKind.Commit)
            {
                break;
            }

            Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.RunForward));
            visited.Add(decision.Index);
            statuses[decision.Index] = F;
        }

        Assert.That(visited, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    // --- Idempotent resume: the decision is a pure function of the vector ---

    [Test]
    public void Resume_in_forward_re_derives_the_interrupted_step()
    {
        // A crash after step 0 committed but before step 1: the persisted vector is
        // [F, P, P]; resume must re-attempt step 1, never re-run step 0.
        var decision = AtomicActionPlanCore.Decide([F, P, P], AtomicActionPhase.Forward);
        Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.RunForward));
        Assert.That(decision.Index, Is.EqualTo(1));
    }

    [Test]
    public void Resume_in_compensate_re_derives_the_interrupted_reverse_step()
    {
        // A crash mid-compensation: step 2 already compensated, steps 0,1 still
        // ForwardDone; resume must continue from the highest remaining, index 1.
        var decision = AtomicActionPlanCore.Decide([F, F, C], AtomicActionPhase.Compensate);
        Assert.That(decision.Kind, Is.EqualTo(AtomicActionActionKind.Compensate));
        Assert.That(decision.Index, Is.EqualTo(1));
    }

    [Test]
    public void Decide_is_deterministic_for_the_same_inputs()
    {
        for (var i = 0; i < 5; i++)
        {
            var decision = AtomicActionPlanCore.Decide([F, P, F], AtomicActionPhase.Forward);
            Assert.That(decision, Is.EqualTo(new AtomicActionDecision(AtomicActionActionKind.RunForward, 1)));
        }
    }
}
