using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// The saga outcome a <see cref="AtomicCommitLivenessModel"/> run drives towards:
/// a full-ack <see cref="Commit"/> or a one-nack <see cref="Abort"/>. The
/// scenario is fixed per run; the coordinator's verdict is still computed by the
/// production <see cref="SagaCoordinatorCore.Decide"/> so the model exercises the
/// real decision rule rather than hard-coding the outcome.
/// </summary>
public enum AtomicCommitLivenessScenario
{
    /// <summary>Every participant acked, so the saga must commit and drain on every leaf.</summary>
    Commit,

    /// <summary>One participant nacked, so the saga must abort and every leaf must release its prepared bucket.</summary>
    Abort,
}

/// <summary>
/// Which progress mechanism a <see cref="AtomicCommitLivenessModel"/> run keeps in
/// place, so a guard run can prove the mechanism is load-bearing by removing it
/// and asserting Coyote re-finds the liveness (progress) violation.
/// </summary>
public enum AtomicCommitLivenessMode
{
    /// <summary>
    /// The fix: after the fault-injected terminal broadcast, a durable-registry
    /// <b>backstop</b> sweep re-derives the saga's terminal from the registry
    /// decision (which survives every transport fault and participant restart) and
    /// applies it to any leaf that never received the broadcast. Progress is
    /// guaranteed for every schedule, so no liveness property is ever violated.
    /// </summary>
    DurableBackstop,

    /// <summary>
    /// The guard: the backstop sweep is removed, so a terminal lost to a drop or a
    /// participant restart is never recovered. A leaf then never drains (commit) or
    /// never releases its prepared bucket (abort), and Coyote finds the stalled
    /// schedule. This reintroduces the "no terminal retry / backstop" progress bug.
    /// </summary>
    NoBackstop,
}

/// <summary>
/// A Coyote <b>liveness</b> model of the atomic-commit protocol's terminal
/// broadcast under <b>fault injection</b>, driving the same production cores as
/// the safety models: the coordinator verdict is the real
/// <see cref="SagaCoordinatorCore.Decide"/>, the durable decision is recorded in a
/// real <see cref="TxRegistryDecisionCore"/>, each leaf's terminal disposition is
/// the real <see cref="MigrationTerminalCore.DecideBucketAction"/>, and the reader
/// fan-out resolves visibility through the real
/// <see cref="AtomicVisibilityGate.ResolveKey"/>. Because the model executes the
/// code Orleans runs, a progress failure Coyote finds is a progress failure of the
/// shipping protocol.
/// <para>
/// <b>Where the safety models stop and this one starts.</b> The safety models
/// (<see cref="AtomicCommitVisibilityModel"/>, <see cref="SagaCoordinatorModel"/>,
/// <see cref="ReshardMigrationModel"/>) prove <i>nothing bad happens</i> - no split
/// view, no both-commit-and-abort, no orphan shadow - over a reliable transport.
/// This model proves <i>something good eventually happens</i> - the protocol does
/// not get stuck - when the transport is <b>unreliable</b>: the terminal broadcast
/// is delivered through a <see cref="FaultDeliveryQueue{T}"/> that drops,
/// duplicates, and reorders messages, and participants restart, all bounded by a
/// <see cref="FaultBudget"/>.
/// </para>
/// <para>
/// <b>How liveness is encoded.</b> Because <see cref="CoyoteModelHarness"/> does
/// not apply <c>coyote rewrite</c>, real <c>Task</c>/<c>await</c> is not
/// controlled, so there is no fair infinite schedule for a temperature-based
/// Coyote liveness monitor to flag. Liveness is therefore encoded as
/// <b>bounded progress</b>: the finite <see cref="FaultBudget"/> is the fairness
/// assumption ("faults do not happen forever") made concrete, so once it is
/// exhausted the transport is reliable and a correct protocol must converge. The
/// run drives the fault-injected broadcast to completion, applies the fix's
/// backstop, then models the registry decision being garbage-collected once its
/// tombstone retention elapses (a real, inevitable event), and finally asserts the
/// good terminal state was reached. This is the pragmatic baseline the phase
/// chose over a Coyote liveness monitor, which the cooperative harness cannot
/// drive meaningfully.
/// </para>
/// <para>
/// <b>Why the registry garbage-collection step matters.</b> Atomic-commit
/// visibility is gated by the durable registry decision, so a committed saga is
/// visible the instant the decision is recorded - independent of whether the
/// terminal reached a leaf. The progress obligation is that every leaf
/// <b>drains</b> its prepared bucket into projected state <i>before</i> the
/// registry forgets the decision (its retention TTL elapses). If a leaf never
/// drains and the decision is then GC'd, the leaf resolves the txid to
/// <see cref="TxStatus.InFlight"/> and <see cref="AtomicVisibilityGate"/> falls the
/// undrained prepared value through to the pre-saga value - so a committed saga
/// becomes <b>invisible</b>. Modelling the GC is what turns "the terminal was not
/// delivered" from a harmless transient into an observable liveness violation, and
/// is what makes the backstop load-bearing.
/// </para>
/// </summary>
public sealed class AtomicCommitLivenessModel : ICoyoteModel
{
    private const int Pre = 0;
    private const int Post = 1;

    private readonly int _leafCount;
    private readonly AtomicCommitLivenessScenario _scenario;
    private readonly AtomicCommitLivenessMode _mode;
    private readonly FaultBudget _budget;

    /// <summary>
    /// Creates the liveness model for a <paramref name="leafCount"/>-leaf saga in
    /// the chosen <paramref name="scenario"/> and progress <paramref name="mode"/>,
    /// injecting up to <paramref name="drops"/> terminal drops,
    /// <paramref name="duplicates"/> terminal duplicates, and
    /// <paramref name="restarts"/> participant restarts per iteration.
    /// </summary>
    /// <param name="leafCount">The number of owning leaves (participants); at least two.</param>
    /// <param name="scenario">Whether the saga commits (all ack) or aborts (one nack).</param>
    /// <param name="mode">Whether the durable-registry backstop is present (fix) or removed (guard).</param>
    /// <param name="drops">The maximum number of terminal deliveries that may be dropped.</param>
    /// <param name="duplicates">The maximum number of terminal deliveries that may be duplicated.</param>
    /// <param name="restarts">The maximum number of participant restarts.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="leafCount"/> is less than two.</exception>
    public AtomicCommitLivenessModel(
        int leafCount,
        AtomicCommitLivenessScenario scenario,
        AtomicCommitLivenessMode mode,
        int drops = 1,
        int duplicates = 1,
        int restarts = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(leafCount, 2);
        _leafCount = leafCount;
        _scenario = scenario;
        _mode = mode;
        _budget = new FaultBudget(drops, duplicates, restarts);
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        // Cache the decision source once so the per-step fault choices do not
        // allocate a delegate each call.
        Func<bool> decide = runtime.RandomBoolean;

        var core = new TxRegistryDecisionCore(new Dictionary<Guid, TxStatus>(), 0L);
        var txid = Guid.NewGuid();

        // Every participant prepared and acked (nack scenario flips one vote). The
        // coordinator's commit-vs-abort verdict is the real production rule.
        var votes = new SagaParticipantOutcome[_leafCount];
        Array.Fill(votes, SagaParticipantOutcome.PreparedAck);
        if (_scenario == AtomicCommitLivenessScenario.Abort)
        {
            votes[0] = SagaParticipantOutcome.PreparedNack;
        }

        var committed = SagaCoordinatorCore.Decide(votes) == SagaDecision.Commit;

        // The commit point: the coordinator records the durable decision in the
        // registry before broadcasting terminals. This decision survives every
        // transport fault and participant restart below.
        core.Apply(txid, committed ? TxStatus.Committed : TxStatus.Aborted);

        // Per-leaf durable state. Every leaf holds a prepared bucket after prepare
        // and keeps it until its terminal is applied; projected state starts at the
        // pre-saga value and is drained to Post only on a commit terminal.
        var holdsPrepared = new bool[_leafCount];
        Array.Fill(holdsPrepared, true);
        var terminalApplied = new bool[_leafCount];
        var projected = new int[_leafCount];
        Array.Fill(projected, Pre);

        // Broadcast a terminal to every leaf through the fault-injecting transport.
        var queue = new FaultDeliveryQueue<int>(_budget);
        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            queue.Enqueue(leaf);
        }

        DriveFaultInjectedBroadcast(queue, decide, committed, holdsPrepared, terminalApplied, projected);

        if (_mode == AtomicCommitLivenessMode.DurableBackstop)
        {
            RunBackstop(core, txid, committed, holdsPrepared, terminalApplied, projected);
        }

        // The registry's tombstone retention eventually elapses and the decision is
        // garbage-collected. After this point an undrained leaf can no longer prove
        // the saga committed, which is what makes a missed drain a visible loss.
        core.Remove(txid);

        AssertLiveness(core, txid, committed, holdsPrepared, terminalApplied, projected);
    }

    /// <summary>
    /// Delivers the terminal broadcast under bounded drop / duplicate / reorder
    /// faults and participant restarts, applying each delivered terminal through
    /// the production disposition core. The loop terminates because every step
    /// removes an in-flight message and duplicates are budget-bounded.
    /// </summary>
    private void DriveFaultInjectedBroadcast(
        FaultDeliveryQueue<int> queue,
        Func<bool> decide,
        bool committed,
        bool[] holdsPrepared,
        bool[] terminalApplied,
        int[] projected)
    {
        while (queue.HasPending)
        {
            MaybeRestartParticipant(queue, decide);

            if (queue.TryDeliverNext(decide, out var leaf))
            {
                ApplyTerminal(leaf, committed, holdsPrepared, terminalApplied, projected);
            }
        }
    }

    /// <summary>
    /// Offers a participant restart: when the budget permits and the scheduler so
    /// chooses, a leaf is restarted, losing the volatile in-flight terminal
    /// targeted at it (removed from the transport) while its durable prepared
    /// bucket, projected state, and the durable registry decision are all
    /// preserved. A leaf whose terminal is lost this way can only be recovered by
    /// the backstop.
    /// </summary>
    private void MaybeRestartParticipant(FaultDeliveryQueue<int> queue, Func<bool> decide)
    {
        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            var target = leaf;
            if (_budget.RestartsRemaining <= 0)
            {
                return;
            }

            if (_budget.TryRestart(decide))
            {
                // Volatile in-flight terminals for this leaf are lost on restart;
                // durable state is untouched.
                queue.RemoveAll(m => m == target);
            }
        }
    }

    /// <summary>
    /// Applies a delivered saga terminal to one leaf through the production
    /// <see cref="MigrationTerminalCore.DecideBucketAction"/> rule. A committed
    /// terminal drains the prepared bucket into projected state; an aborted
    /// terminal discards it; a duplicate that arrives after the terminal already
    /// landed is a no-op orphan discard - so applying a terminal any number of
    /// times is idempotent.
    /// </summary>
    private static void ApplyTerminal(
        int leaf,
        bool committed,
        bool[] holdsPrepared,
        bool[] terminalApplied,
        int[] projected)
    {
        var action = MigrationTerminalCore.DecideBucketAction(
            hadPending: holdsPrepared[leaf],
            alreadyTerminal: terminalApplied[leaf],
            committed: committed);

        switch (action)
        {
            case MigrationTerminalBucketAction.DrainCommit:
                projected[leaf] = Post;
                holdsPrepared[leaf] = false;
                break;
            case MigrationTerminalBucketAction.DiscardAborted:
            case MigrationTerminalBucketAction.DiscardOrphan:
                holdsPrepared[leaf] = false;
                break;
            case MigrationTerminalBucketAction.None:
            default:
                break;
        }

        terminalApplied[leaf] = true;
    }

    /// <summary>
    /// The durable-registry backstop (the fix): every leaf that never applied its
    /// terminal re-derives the saga's outcome from the still-live registry decision
    /// and applies it, exactly as a reminder-driven reactivation would. Because the
    /// decision is durable, this recovers every leaf a transport fault or restart
    /// left behind, before the decision is garbage-collected.
    /// </summary>
    private void RunBackstop(
        TxRegistryDecisionCore core,
        Guid txid,
        bool committed,
        bool[] holdsPrepared,
        bool[] terminalApplied,
        int[] projected)
    {
        var decision = core.Resolve(txid);
        var decidedCommit = decision == TxStatus.Committed;
        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            if (!terminalApplied[leaf])
            {
                ApplyTerminal(leaf, decidedCommit, holdsPrepared, terminalApplied, projected);
            }
        }
    }

    /// <summary>
    /// Asserts the bounded-progress liveness properties over the terminal state,
    /// after fault injection, any backstop, and the registry garbage-collection.
    /// The reader visibility is resolved through the production
    /// <see cref="AtomicVisibilityGate"/> so the "eventually visible" property is a
    /// property of the shipping read gate.
    /// </summary>
    private void AssertLiveness(
        TxRegistryDecisionCore core,
        Guid txid,
        bool committed,
        bool[] holdsPrepared,
        bool[] terminalApplied,
        int[] projected)
    {
        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            // Progress property 1: every participant eventually reaches its terminal.
            Specification.Assert(
                terminalApplied[leaf],
                $"liveness: leaf {leaf} never reached the saga terminal " +
                $"(committed={committed}) - the protocol got stuck");

            if (committed)
            {
                // Progress property 3: a committed saga is eventually visible on
                // every owning leaf. Resolve through the production read gate; the
                // registry decision has been GC'd, so an undrained leaf falls
                // through to the pre-saga value and this fails.
                var visible = ResolveVisibleValue(core, txid, leaf, holdsPrepared, projected);
                Specification.Assert(
                    visible == Post,
                    $"liveness: committed saga not visible on leaf {leaf} " +
                    $"(observed pre-saga value after the registry decision was GC'd " +
                    $"because the leaf never drained)");
            }
            else
            {
                // Progress property 2: an aborted saga leaves no participant holding
                // a prepared value.
                Specification.Assert(
                    !holdsPrepared[leaf],
                    $"liveness: aborted saga left leaf {leaf} still holding a prepared value " +
                    "(its bucket was never released)");
            }
        }
    }

    /// <summary>
    /// Resolves the value a reader observes for a leaf. A drained leaf serves its
    /// projected state directly; an undrained leaf that still holds a prepared
    /// bucket consults the production <see cref="AtomicVisibilityGate.ResolveKey"/>
    /// against the (now GC'd) registry decision.
    /// </summary>
    private static int ResolveVisibleValue(
        TxRegistryDecisionCore core,
        Guid txid,
        int leaf,
        bool[] holdsPrepared,
        int[] projected)
    {
        if (!holdsPrepared[leaf])
        {
            return projected[leaf];
        }

        var outcome = AtomicVisibilityGate.ResolveKey(
            core.Resolve(txid),
            alreadyTerminal: false,
            preparedHiddenByTombstoneOrExpiry: false);

        return outcome == PendingReadOutcome.SurfacePrepared ? Post : Pre;
    }
}
