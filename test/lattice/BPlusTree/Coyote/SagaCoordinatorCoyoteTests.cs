using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the atomic-write saga
/// coordinator's commit-vs-abort decision core (<see cref="SagaCoordinatorCore"/>,
/// issue #1589). They are tagged <c>[Category("Coyote")]</c> so the fast dev loop
/// and the per-package deterministic CI step skip them; a dedicated CI step runs
/// this category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class SagaCoordinatorCoyoteTests
{
    private static SagaParticipantOutcome[] AllAcked(int n)
    {
        var a = new SagaParticipantOutcome[n];
        Array.Fill(a, SagaParticipantOutcome.PreparedAck);
        return a;
    }

    /// <summary>
    /// The fix: with every participant acking, the proven core commits on every
    /// explored delivery order and never aborts.
    /// </summary>
    [Test]
    public void All_participants_acking_always_commits()
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new SagaCoordinatorModel(AllAcked(4), useBrokenDecision: false));
    }

    /// <summary>
    /// The fix: a single nacking participant makes the core abort on every
    /// explored delivery order - the commit-iff-all-acked property is
    /// order-independent, so no schedule produces a split decision.
    /// </summary>
    [Test]
    public void One_nack_always_aborts_regardless_of_delivery_order()
    {
        var assignment = new[]
        {
            SagaParticipantOutcome.PreparedAck,
            SagaParticipantOutcome.PreparedNack,
            SagaParticipantOutcome.PreparedAck,
            SagaParticipantOutcome.PreparedAck,
        };

        CoyoteModelHarness.AssertNoInterleavingViolation(
            new SagaCoordinatorModel(assignment, useBrokenDecision: false));
    }

    /// <summary>
    /// The fix: a mixed failure set (a nack and an unreachable participant among
    /// acks) aborts on every explored delivery order, and the coordinator never
    /// both commits and aborts the same saga.
    /// </summary>
    [Test]
    public void Nack_and_unreachable_participants_always_abort()
    {
        var assignment = new[]
        {
            SagaParticipantOutcome.PreparedAck,
            SagaParticipantOutcome.PreparedNack,
            SagaParticipantOutcome.Unreachable,
            SagaParticipantOutcome.PreparedAck,
        };

        CoyoteModelHarness.AssertNoInterleavingViolation(
            new SagaCoordinatorModel(assignment, useBrokenDecision: false));
    }

    /// <summary>
    /// The guard: an order-sensitive decision that commits on a partial ack set
    /// (whenever the last-delivered participant acked, ignoring an earlier nack)
    /// reintroduces the split-decision regression, and Coyote must find a delivery
    /// order that triggers it. This proves the model genuinely exercises the race,
    /// so the passing tests above are meaningful rather than vacuous.
    /// </summary>
    [Test]
    public void Order_sensitive_decision_committing_on_partial_ack_is_caught()
    {
        var assignment = new[]
        {
            SagaParticipantOutcome.PreparedNack,
            SagaParticipantOutcome.PreparedAck,
            SagaParticipantOutcome.PreparedAck,
            SagaParticipantOutcome.PreparedAck,
        };

        CoyoteModelHarness.AssertInterleavingViolationFound(
            new SagaCoordinatorModel(assignment, useBrokenDecision: true));
    }
}
