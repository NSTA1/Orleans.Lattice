using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// A Coyote concurrency model of the atomic-write saga coordinator's
/// commit-vs-abort decision, driving the <b>production</b>
/// <see cref="SagaCoordinatorCore"/> under systematic schedule exploration. It
/// fans a prepare out to <c>N</c> participants whose votes (ack / nack /
/// unreachable) are fixed per run, and delivers those votes in an order the
/// runtime explores via <see cref="ICoyoteRuntime.RandomBoolean()"/> - so every
/// interleaving of the prepare fan-out's completions is exercised.
/// <para>
/// The model folds each delivered vote into the coordinator's per-participant
/// outcome buffer through <see cref="SagaCoordinatorCore.OnParticipantResult"/>
/// and re-derives the verdict with <see cref="SagaCoordinatorCore.Decide"/> after
/// every delivery, asserting two safety properties:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>Agreement:</b> the terminal verdict is <see cref="SagaDecision.Commit"/>
///     if and only if every participant acked - independent of delivery order.
///   </description></item>
///   <item><description>
///     <b>No double outcome:</b> the coordinator never both commits and aborts the
///     same saga across the whole delivery sequence.
///   </description></item>
/// </list>
/// The <paramref name="useBrokenDecision"/> toggle chooses the decision rule:
/// <list type="bullet">
///   <item><description>
///     <c>false</c> - the proven core. Because <see cref="SagaCoordinatorCore.Decide"/>
///     treats a single nack / unreachable as decisive and commits only on a full
///     ack set, the verdict is order-independent and the properties hold for every
///     schedule.
///   </description></item>
///   <item><description>
///     <c>true</c> - the regression: an order-sensitive rule that commits on a
///     partial ack set whenever the <i>last-delivered</i> participant acked, even
///     though an earlier participant nacked. Coyote explores a delivery order that
///     ends on an ack and finds the resulting split decision.
///   </description></item>
/// </list>
/// </summary>
internal sealed class SagaCoordinatorModel(SagaParticipantOutcome[] assignment, bool useBrokenDecision) : ICoyoteModel
{
    private readonly SagaParticipantOutcome[] _assignment =
        assignment ?? throw new ArgumentNullException(nameof(assignment));

    public void Run(ICoyoteRuntime runtime)
    {
        var n = _assignment.Length;

        // The coordinator's in-flight saga state: one outcome slot per
        // participant, all Pending until its vote is delivered.
        var outcomes = new SagaParticipantOutcome[n];
        var delivered = new bool[n];

        var expectedCommit = true;
        foreach (var vote in _assignment)
        {
            if (vote != SagaParticipantOutcome.PreparedAck)
            {
                expectedCommit = false;
            }
        }

        var sawAbort = false;
        var sawCommit = false;
        var lastDelivered = -1;

        for (var step = 0; step < n; step++)
        {
            // Deliver the votes in an order the runtime explores: scan the
            // undelivered participants and let controlled nondeterminism pick
            // which one's prepare result lands next.
            var pick = SelectNextUndelivered(runtime, delivered);
            delivered[pick] = true;
            lastDelivered = pick;

            SagaCoordinatorCore.OnParticipantResult(outcomes, pick, _assignment[pick]);

            var decision = useBrokenDecision
                ? DecideBroken(outcomes, lastDelivered)
                : SagaCoordinatorCore.Decide(outcomes);

            if (decision == SagaDecision.Abort)
            {
                sawAbort = true;
            }
            else if (decision == SagaDecision.Commit)
            {
                sawCommit = true;
            }

            // No double outcome: the coordinator must never resolve the same saga
            // both ways across the delivery sequence.
            Specification.Assert(
                !(sawAbort && sawCommit),
                "saga coordinator resolved the same saga both Commit and Abort");
        }

        var finalDecision = useBrokenDecision
            ? DecideBroken(outcomes, lastDelivered)
            : SagaCoordinatorCore.Decide(outcomes);

        // Agreement: commit iff every participant acked, whatever the order.
        Specification.Assert(
            (finalDecision == SagaDecision.Commit) == expectedCommit,
            $"saga coordinator commit decision disagreed with the ack set: " +
            $"decision={finalDecision}, everyParticipantAcked={expectedCommit}");
    }

    /// <summary>
    /// Picks the next undelivered participant to deliver, driving the choice
    /// through the runtime's controlled nondeterminism so the harness explores
    /// distinct delivery orders. Always returns a valid undelivered index.
    /// </summary>
    private static int SelectNextUndelivered(ICoyoteRuntime runtime, bool[] delivered)
    {
        var firstUndelivered = -1;
        for (var i = 0; i < delivered.Length; i++)
        {
            if (delivered[i])
            {
                continue;
            }

            if (firstUndelivered < 0)
            {
                firstUndelivered = i;
            }

            // Take this candidate now, or defer to a later undelivered one.
            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        // Every undelivered candidate was deferred; take the first one.
        return firstUndelivered;
    }

    /// <summary>
    /// The BROKEN decision rule (guard fixture only): once no participant is still
    /// pending, it commits whenever the most recently delivered participant acked,
    /// ignoring earlier nacks / unreachables. This reintroduces a "commit on a
    /// partial ack set" regression whose visibility depends on delivery order, so
    /// Coyote's schedule exploration is what surfaces it.
    /// </summary>
    private static SagaDecision DecideBroken(ReadOnlySpan<SagaParticipantOutcome> outcomes, int lastDelivered)
    {
        foreach (var outcome in outcomes)
        {
            if (outcome == SagaParticipantOutcome.Pending)
            {
                return SagaDecision.Collecting;
            }
        }

        return outcomes[lastDelivered] == SagaParticipantOutcome.PreparedAck
            ? SagaDecision.Commit
            : SagaDecision.Abort;
    }
}
