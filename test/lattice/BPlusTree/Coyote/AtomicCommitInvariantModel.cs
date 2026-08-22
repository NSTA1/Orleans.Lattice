using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// The saga outcome a <see cref="AtomicCommitInvariantModel"/> run drives towards.
/// The scenario is fixed per run; the coordinator's verdict is still computed by
/// the production <see cref="SagaCoordinatorCore.Decide"/> so the model exercises
/// the real decision rule rather than hard-coding the outcome.
/// </summary>
public enum AtomicCommitInvariantScenario
{
    /// <summary>Every participant acked, so the recorded decision is commit.</summary>
    Commit,

    /// <summary>One participant nacked, so the recorded decision is abort.</summary>
    Abort,
}

/// <summary>
/// Which correctness guard (if any) a <see cref="AtomicCommitInvariantModel"/> run
/// keeps in place. The fixed design is <see cref="None"/>; every other value
/// removes one load-bearing guard so a companion guard test can prove Coyote
/// re-finds the corresponding property violation - i.e. that the assertion for
/// that property is non-vacuous.
/// </summary>
public enum AtomicCommitInvariantGuard
{
    /// <summary>
    /// The fix: the reader resolves visibility through the real
    /// <see cref="AtomicVisibilityGate"/> against the real recorded decision, each
    /// leaf broadcasts its terminal only after the decision is recorded and derives
    /// the terminal kind from that single decision, a duplicate terminal is
    /// classified by the real <see cref="TerminalDecisionGuard"/> (so a conflicting
    /// flip is rejected), and the registry revision only ever advances. No schedule
    /// violates any catalogued invariant.
    /// </summary>
    None,

    /// <summary>
    /// The guard for <b>strict isolation</b> / <b>visibility-matches-decision</b>:
    /// the read gate surfaces an <see cref="TxStatus.InFlight"/> saga's prepared
    /// value as if it had committed, so a reader observes a post-saga value before
    /// (or without) a commit decision. Coyote must find the schedule where the
    /// observation precedes the decision.
    /// </summary>
    SurfaceInFlightAsPrepared,

    /// <summary>
    /// The guard for <b>linearized terminals</b> (decision-before-broadcast): a leaf
    /// is allowed to apply its commit terminal before the registry has recorded the
    /// decision, so a terminal exists while the tree-wide decision is still
    /// <see cref="TxStatus.InFlight"/>. Coyote must find the schedule where a
    /// broadcast precedes the decision.
    /// </summary>
    BroadcastBeforeDecision,

    /// <summary>
    /// The guard for <b>no mixed terminals</b>: each leaf derives its terminal kind
    /// from an independent coin instead of the single recorded decision, so one leaf
    /// can commit while a sibling aborts the same saga. Coyote must find the
    /// schedule that applies opposite terminals across leaves.
    /// </summary>
    MixedBroadcast,

    /// <summary>
    /// The guard for <b>decision durability</b> (and, transitively, monotonic
    /// visibility): a duplicate terminal delivery bypasses the
    /// <see cref="TerminalDecisionGuard"/> write-once classification and flips the
    /// recorded decision to the opposite terminal, so a committed decision an earlier
    /// read relied on becomes aborted. Coyote must find the schedule where the flip
    /// follows a decision.
    /// </summary>
    FlipDecision,

    /// <summary>
    /// The guard for <b>revision monotonicity</b>: a mutation lowers the registry
    /// revision counter (an unpaired rollback) instead of only ever advancing it, so
    /// the monotonic version probe the reader relies on regresses. Coyote must find
    /// the schedule where the revision decreases.
    /// </summary>
    DecrementRevision,
}

/// <summary>
/// A Coyote concurrency model of a single atomic-commit saga's full lifecycle -
/// prepare-vote fold, the tree-wide registry decision, the per-leaf terminal
/// broadcast, duplicate terminal re-deliveries, and interleaved reader probes -
/// driving the <b>production</b> cores under systematic schedule exploration. The
/// coordinator verdict is the real <see cref="SagaCoordinatorCore.Decide"/>, the
/// durable decision and its monotonic revision are a real
/// <see cref="TxRegistryDecisionCore"/>, a duplicate terminal is classified by the
/// real <see cref="TerminalDecisionGuard.Classify"/>, and every reader observation
/// is resolved through the real <see cref="AtomicVisibilityGate.ResolveKey"/>.
/// Because the model executes the same code Orleans runs, a violation Coyote finds
/// is a violation of the shipping protocol.
/// <para>
/// Where the sibling models stop and this one starts. <see cref="SagaCoordinatorModel"/>
/// proves the <i>verdict</i> is commit-iff-all-acked; <see cref="AtomicCommitVisibilityModel"/>
/// proves an N-key read is <i>all-or-nothing</i> against a stale-snapshot split;
/// <see cref="ReshardMigrationModel"/> proves a mid-migration orphan never shadows a
/// later saga. This model asserts the remaining <b>per-key point and ordering</b>
/// invariants of the catalogue (level-C Phase 6, #1595) continuously as the decision
/// write, the terminal broadcast, duplicate deliveries, and reader probes interleave:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>StrictIsolation</b> - a reader never observes a post-saga value unless the
///     recorded decision is <see cref="TxStatus.Committed"/>.
///   </description></item>
///   <item><description>
///     <b>VisibilityMatchesDecision</b> - a key is observed post-saga exactly when
///     the recorded decision is committed (the sharpened form of all-or-nothing).
///   </description></item>
///   <item><description>
///     <b>LinearizedTerminals</b> - a leaf's applied commit / abort terminal matches
///     the recorded decision, so no terminal precedes the decision.
///   </description></item>
///   <item><description>
///     <b>NoMixedTerminals</b> - one saga never applies a commit terminal on one leaf
///     and an abort terminal on another.
///   </description></item>
///   <item><description>
///     <b>DecisionDurability</b> - once the registry records a terminal decision it
///     never flips to the other terminal, across every duplicate delivery.
///   </description></item>
///   <item><description>
///     <b>MonotonicVisibility</b> - once a key is observed post-saga-visible it stays
///     visible for the rest of the schedule (no later committed write or tombstone is
///     modelled here, so any regression is a torn-visibility bug).
///   </description></item>
///   <item><description>
///     <b>RevisionMonotonic</b> - the registry revision counter never decreases.
///   </description></item>
/// </list>
/// Each invariant has a companion <see cref="AtomicCommitInvariantGuard"/> that
/// removes exactly the fix it depends on, so the guard tests prove none of the
/// assertions above is vacuous.
/// </summary>
public sealed class AtomicCommitInvariantModel : ICoyoteModel
{
    private const int TermNone = 0;
    private const int TermCommit = 1;
    private const int TermAbort = 2;

    private const int Pre = 0;
    private const int Post = 1;

    private const int DuplicateBudget = 2;
    private const int MaxSteps = 256;

    private readonly int _leafCount;
    private readonly AtomicCommitInvariantScenario _scenario;
    private readonly AtomicCommitInvariantGuard _guard;

    /// <summary>
    /// Creates the model for a <paramref name="leafCount"/>-leaf saga in the chosen
    /// <paramref name="scenario"/> under the chosen <paramref name="guard"/>.
    /// </summary>
    /// <param name="leafCount">The number of owning leaves (one key each); at least two.</param>
    /// <param name="scenario">Whether the saga commits (all ack) or aborts (one nack).</param>
    /// <param name="guard">The guard to keep (fix) or remove (regression).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="leafCount"/> is less than two.</exception>
    public AtomicCommitInvariantModel(
        int leafCount,
        AtomicCommitInvariantScenario scenario,
        AtomicCommitInvariantGuard guard)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(leafCount, 2);
        _leafCount = leafCount;
        _scenario = scenario;
        _guard = guard;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);

        var state = new RunState(_leafCount, _scenario);

        // Pre-decision reads: the decision is InFlight, so nothing may be visible.
        Probe(state);

        var steps = 0;
        while (steps++ < MaxSteps && !IsComplete(state))
        {
            if (TryDecide(state, runtime))
            {
                Probe(state);
                continue;
            }

            if (TryBroadcast(state, runtime))
            {
                Probe(state);
                continue;
            }

            if (TryDuplicate(state, runtime))
            {
                Probe(state);
                continue;
            }

            if (runtime.RandomBoolean())
            {
                Probe(state);
                continue;
            }

            // Nothing was chosen this round; force progress so the run terminates.
            ForceProgress(state);
            Probe(state);
        }

        Probe(state);
    }

    /// <summary>The run is done once the decision is recorded and every leaf has applied its terminal.</summary>
    private bool IsComplete(RunState state)
    {
        if (!state.DecideDone)
        {
            return false;
        }

        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            if (state.Terminal[leaf] == TermNone)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Records the tree-wide registry decision at the linearization point.</summary>
    private bool TryDecide(RunState state, ICoyoteRuntime runtime)
    {
        if (state.DecideDone || !runtime.RandomBoolean())
        {
            return false;
        }

        Decide(state);
        return true;
    }

    private void Decide(RunState state)
    {
        state.Core.Apply(state.Txid, state.Committed ? TxStatus.Committed : TxStatus.Aborted);
        state.DecideDone = true;
    }

    /// <summary>
    /// Offers to apply one leaf's terminal. In the fix a leaf may broadcast only
    /// after the decision is recorded; the <see cref="AtomicCommitInvariantGuard.BroadcastBeforeDecision"/>
    /// guard lifts that gate.
    /// </summary>
    private bool TryBroadcast(RunState state, ICoyoteRuntime runtime)
    {
        var mayBroadcastEarly = _guard == AtomicCommitInvariantGuard.BroadcastBeforeDecision;
        if (!state.DecideDone && !mayBroadcastEarly)
        {
            return false;
        }

        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            if (state.Terminal[leaf] == TermNone && runtime.RandomBoolean())
            {
                ApplyBroadcast(state, leaf, runtime);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Applies a leaf's terminal. The kind is derived from the single recorded
    /// decision (the fix), except under <see cref="AtomicCommitInvariantGuard.MixedBroadcast"/>,
    /// where each leaf tosses an independent coin, and under
    /// <see cref="AtomicCommitInvariantGuard.BroadcastBeforeDecision"/>, where the
    /// anticipated verdict is used even though no decision has been recorded yet.
    /// </summary>
    private void ApplyBroadcast(RunState state, int leaf, ICoyoteRuntime runtime)
    {
        bool commitTerminal;
        if (_guard == AtomicCommitInvariantGuard.MixedBroadcast)
        {
            commitTerminal = runtime.RandomBoolean();
        }
        else if (_guard == AtomicCommitInvariantGuard.BroadcastBeforeDecision && !state.DecideDone)
        {
            commitTerminal = state.Committed;
        }
        else
        {
            commitTerminal = state.Core.Resolve(state.Txid) == TxStatus.Committed;
        }

        if (commitTerminal)
        {
            state.Terminal[leaf] = TermCommit;
            state.Projected[leaf] = Post;
            state.HoldsPrepared[leaf] = false;
        }
        else
        {
            state.Terminal[leaf] = TermAbort;
            state.HoldsPrepared[leaf] = false;
        }

        CheckGlobalInvariants(state);
    }

    /// <summary>
    /// Offers a duplicate terminal re-delivery to the registry. A benign duplicate
    /// re-delivers the same terminal, which <see cref="TerminalDecisionGuard.Classify"/>
    /// treats as idempotent; the <see cref="AtomicCommitInvariantGuard.FlipDecision"/>
    /// guard delivers the opposite terminal and bypasses the classification, and the
    /// <see cref="AtomicCommitInvariantGuard.DecrementRevision"/> guard lowers the
    /// revision counter.
    /// </summary>
    private bool TryDuplicate(RunState state, ICoyoteRuntime runtime)
    {
        if (!state.DecideDone || state.DuplicatesUsed >= DuplicateBudget || !runtime.RandomBoolean())
        {
            return false;
        }

        state.DuplicatesUsed++;

        if (_guard == AtomicCommitInvariantGuard.DecrementRevision && state.Core.Revision > 0)
        {
            // An unpaired rollback that lowers the revision without restoring a map
            // change - the regression the RevisionMonotonic assertion must catch.
            state.Core.RollbackRevision(state.Core.Revision - 1);
            CheckGlobalInvariants(state);
            return true;
        }

        var incomingCommitted = _guard == AtomicCommitInvariantGuard.FlipDecision
            ? !state.Committed
            : state.Committed;

        var hasExisting = state.Core.TryResolve(state.Txid, out var existing);

        if (_guard == AtomicCommitInvariantGuard.FlipDecision)
        {
            // Bypass the write-once guard and flip the recorded decision.
            state.Core.Apply(state.Txid, incomingCommitted ? TxStatus.Committed : TxStatus.Aborted);
        }
        else
        {
            var action = TerminalDecisionGuard.Classify(hasExisting, existing, incomingCommitted);
            if (action == TerminalRecordAction.Record)
            {
                state.Core.Apply(state.Txid, incomingCommitted ? TxStatus.Committed : TxStatus.Aborted);
            }
        }

        CheckGlobalInvariants(state);
        return true;
    }

    /// <summary>Advances the run when no action was chosen in a round, so exploration terminates.</summary>
    private void ForceProgress(RunState state)
    {
        if (!state.DecideDone)
        {
            Decide(state);
            return;
        }

        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            if (state.Terminal[leaf] == TermNone)
            {
                ApplyBroadcastForced(state, leaf);
                return;
            }
        }
    }

    /// <summary>The forced-progress broadcast: derives the kind from the recorded decision, no coin.</summary>
    private void ApplyBroadcastForced(RunState state, int leaf)
    {
        var commitTerminal = state.Core.Resolve(state.Txid) == TxStatus.Committed;
        if (commitTerminal)
        {
            state.Terminal[leaf] = TermCommit;
            state.Projected[leaf] = Post;
        }
        else
        {
            state.Terminal[leaf] = TermAbort;
        }

        state.HoldsPrepared[leaf] = false;
        CheckGlobalInvariants(state);
    }

    /// <summary>
    /// Resolves each key's visibility through the production read gate and asserts
    /// the per-key point properties (strict isolation, visibility-matches-decision,
    /// monotonic visibility), then re-checks the global invariants.
    /// </summary>
    private void Probe(RunState state)
    {
        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            var post = ResolveVisible(state, leaf);
            var committedNow = state.Core.Resolve(state.Txid) == TxStatus.Committed;

            // StrictIsolation: a post-saga observation implies a committed decision.
            Specification.Assert(
                !post || committedNow,
                $"strict isolation: leaf {leaf} observed the post-saga value while the recorded " +
                $"decision is {state.Core.Resolve(state.Txid)} (an in-flight or aborted saga surfaced as committed)");

            // VisibilityMatchesDecision: observed post-saga exactly when committed.
            Specification.Assert(
                post == committedNow,
                $"visibility-matches-decision: leaf {leaf} observedPost={post} but decisionCommitted={committedNow}");

            // MonotonicVisibility: once visible, stays visible.
            Specification.Assert(
                !state.EverVisible[leaf] || post,
                $"monotonic visibility: leaf {leaf} was observed post-saga-visible earlier but is no longer visible " +
                "(a committed value reverted to the pre-saga value)");

            if (post)
            {
                state.EverVisible[leaf] = true;
            }
        }

        CheckGlobalInvariants(state);
    }

    /// <summary>
    /// The value a reader observes for a leaf. A leaf that has applied its terminal
    /// serves projected state directly (drained post-saga on commit, pre-saga on
    /// abort); a leaf still holding a prepared bucket consults the production gate
    /// against the recorded decision. The <see cref="AtomicCommitInvariantGuard.SurfaceInFlightAsPrepared"/>
    /// guard forces an in-flight decision to resolve as committed at the gate.
    /// </summary>
    private bool ResolveVisible(RunState state, int leaf)
    {
        if (!state.HoldsPrepared[leaf])
        {
            return state.Projected[leaf] == Post;
        }

        var status = state.Core.Resolve(state.Txid);
        if (_guard == AtomicCommitInvariantGuard.SurfaceInFlightAsPrepared && status == TxStatus.InFlight)
        {
            status = TxStatus.Committed;
        }

        var outcome = AtomicVisibilityGate.ResolveKey(
            status,
            alreadyTerminal: false,
            preparedHiddenByTombstoneOrExpiry: false);

        return outcome == PendingReadOutcome.SurfacePrepared;
    }

    /// <summary>
    /// Asserts the schedule-wide invariants: revision monotonicity, decision
    /// durability, terminal linearization, and no mixed terminals.
    /// </summary>
    private void CheckGlobalInvariants(RunState state)
    {
        // RevisionMonotonic: the revision counter never decreases.
        Specification.Assert(
            state.Core.Revision >= state.PreviousRevision,
            $"revision monotonic: revision fell from {state.PreviousRevision} to {state.Core.Revision}");
        state.PreviousRevision = state.Core.Revision;

        // DecisionDurability: once terminal, the recorded decision never flips.
        if (state.Core.TryResolve(state.Txid, out var decision)
            && decision != TxStatus.InFlight)
        {
            if (state.RecordedTerminal is { } recorded)
            {
                Specification.Assert(
                    decision == recorded,
                    $"decision durability: recorded decision flipped from {recorded} to {decision}");
            }
            else
            {
                state.RecordedTerminal = decision;
            }
        }

        var anyCommit = false;
        var anyAbort = false;
        for (var leaf = 0; leaf < _leafCount; leaf++)
        {
            switch (state.Terminal[leaf])
            {
                case TermCommit:
                    anyCommit = true;

                    // LinearizedTerminals: a commit terminal implies a committed decision.
                    Specification.Assert(
                        state.Core.Resolve(state.Txid) == TxStatus.Committed,
                        $"linearized terminals: leaf {leaf} applied a commit terminal while the recorded " +
                        $"decision is {state.Core.Resolve(state.Txid)} (a terminal preceded the decision)");
                    break;
                case TermAbort:
                    anyAbort = true;

                    // LinearizedTerminals: an abort terminal implies an aborted decision.
                    Specification.Assert(
                        state.Core.Resolve(state.Txid) == TxStatus.Aborted,
                        $"linearized terminals: leaf {leaf} applied an abort terminal while the recorded " +
                        $"decision is {state.Core.Resolve(state.Txid)} (a terminal preceded the decision)");
                    break;
                default:
                    break;
            }
        }

        // NoMixedTerminals: never a commit terminal on one leaf and an abort on another.
        Specification.Assert(
            !(anyCommit && anyAbort),
            "no mixed terminals: the saga applied both a commit terminal and an abort terminal across its leaves");
    }

    /// <summary>
    /// The mutable per-iteration state. The model rebuilds this on every
    /// <see cref="Run(ICoyoteRuntime)"/>, holding no state between explored schedules.
    /// </summary>
    private sealed class RunState
    {
        public RunState(int leafCount, AtomicCommitInvariantScenario scenario)
        {
            Core = new TxRegistryDecisionCore(new Dictionary<Guid, TxStatus>(), 0L);
            Txid = Guid.NewGuid();

            // The verdict is the real production fold over the fixed votes.
            var votes = new SagaParticipantOutcome[leafCount];
            Array.Fill(votes, SagaParticipantOutcome.PreparedAck);
            if (scenario == AtomicCommitInvariantScenario.Abort)
            {
                votes[0] = SagaParticipantOutcome.PreparedNack;
            }

            Committed = SagaCoordinatorCore.Decide(votes) == SagaDecision.Commit;

            Terminal = new int[leafCount];
            Projected = new int[leafCount];
            HoldsPrepared = new bool[leafCount];
            Array.Fill(HoldsPrepared, true);
            EverVisible = new bool[leafCount];
        }

        public TxRegistryDecisionCore Core { get; }

        public Guid Txid { get; }

        public bool Committed { get; }

        public bool DecideDone { get; set; }

        public int DuplicatesUsed { get; set; }

        public long PreviousRevision { get; set; }

        public TxStatus? RecordedTerminal { get; set; }

        public int[] Terminal { get; }

        public int[] Projected { get; }

        public bool[] HoldsPrepared { get; }

        public bool[] EverVisible { get; }
    }
}
