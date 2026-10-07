using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// The fix a <see cref="CrossTreeReceiverBarrierModel"/> run removes, if any.
/// </summary>
internal enum CrossTreeBarrierGuard
{
    /// <summary>The fixed design: every fix in place.</summary>
    None,

    /// <summary>
    /// The barrier decides once any wait-set tree has arrived, with the verdict
    /// of the trees that have (<c>RAllOrNothingBarrierDecidesOnFirstTree</c>).
    /// </summary>
    DecideOnFirstArrival,

    /// <summary>
    /// A tree notifies the barrier before it registers the delegation
    /// (<c>RAllOrNothingNotifyBeforeRegister</c>).
    /// </summary>
    NotifyBeforeRegister,

    /// <summary>
    /// A delegation whose barrier cannot be dialled reads InFlight rather than
    /// Indeterminate, as the snapshot read paths answered it before #4461 (issue
    /// #4448; <c>RAllOrNothingSnapshotReadsUnresolvableAsInFlight</c>).
    /// </summary>
    UndialledDelegationReadsInFlight,
}

/// <summary>
/// A Coyote model of the receiver-side cross-tree barrier for a replicated
/// cross-tree atomic write over <c>trees</c> trees (one source shard each),
/// driving the <b>production</b> <see cref="CrossTreeReceiverBarrier"/> core
/// (<c>LatticeCrossTreeReceiverGrain.NotifyTerminalAsync</c>'s completeness rule
/// and verdict), <see cref="TerminalArrivalTally"/> (each tree's own tally),
/// <see cref="TxRegistryDecisionCore"/> (each tree's receiver registry),
/// <see cref="MigrationTerminalCore"/> (the post-finalise fan-out) and
/// <see cref="AtomicVisibilityGate"/> (every reader probe).
/// <para>
/// Each tree receives its prepare and then its terminal (the per-shard order the
/// design assumes), in an interleaving across trees the runtime explores. When a
/// tree's tally completes it hands off in two steps, as
/// <c>LatticeGrain.ApplyTxTerminalAsync</c> does: it registers the delegation of
/// the saga's status to the barrier, then notifies the barrier. The barrier
/// decides once every wait-set tree has arrived, and every tree then finalises
/// separately - marking its registry, which drops the delegation, and fanning
/// out - so the window in which some trees are finalised and others still read
/// through the delegation is explored. The receiver registry's dial to the
/// barrier can fail at any point while a delegation exists (bounded by
/// <c>dialFaults</c>); a failed dial answers Indeterminate and the gate hides
/// the key.
/// </para>
/// <para>
/// Reader probes assert <c>[RAllOrNothing]</c> across trees and
/// <c>[RStrictIsolation]</c>; at the drained end the model asserts
/// <c>[RCommittedEventuallyVisible]</c> and <c>[RNoStrandedPrepare]</c>.
/// Every per-iteration object is a local of <see cref="Run"/>.
/// </para>
/// </summary>
internal sealed class CrossTreeReceiverBarrierModel(
    int trees,
    bool committed,
    int dialFaults,
    CrossTreeBarrierGuard guard) : ICoyoteModel
{
    private readonly int _trees = trees >= 2
        ? trees
        : throw new ArgumentOutOfRangeException(nameof(trees), "a cross-tree saga needs at least two trees");

    private enum Stage
    {
        Idle,
        Register,
        Notify,
        Done,
    }

    public void Run(ICoyoteRuntime runtime)
    {
        var txid = Guid.NewGuid();
        var treeIds = new string[_trees];
        for (var t = 0; t < _trees; t++)
        {
            treeIds[t] = "tree-" + t;
        }

        // Replication records per tree: prepare, then terminal (count 1).
        var prepareOutstanding = new bool[_trees];
        var terminalOutstanding = new bool[_trees];
        Array.Fill(prepareOutstanding, true);
        Array.Fill(terminalOutstanding, true);

        // Receiver leaves, one per tree.
        var pending = new bool[_trees];
        var terminalApplied = new bool[_trees];
        var materialised = new bool[_trees];

        // Receiver registries, one per tree, and their hand-off state.
        var registries = new TxRegistryDecisionCore[_trees];
        for (var t = 0; t < _trees; t++)
        {
            registries[t] = new TxRegistryDecisionCore(new Dictionary<Guid, TxStatus>(), revision: 0);
        }

        var stage = new Stage[_trees];
        var delegated = new bool[_trees];
        var dialFailing = new bool[_trees];
        var dialFaultBudget = dialFaults;
        var owedFinalise = new bool[_trees];
        var owedFanOut = new bool[_trees];

        // The barrier's persisted state.
        var waitSet = new List<string>(treeIds);
        var arrived = new Dictionary<string, CrossTreeReceiverTerminal>(StringComparer.Ordinal);
        var barrierDecided = false;
        var barrierCommitted = false;

        while (true)
        {
            var steps = new List<Action>();

            for (var t = 0; t < _trees; t++)
            {
                var tree = t;

                // Scope pin: a tree's prepare is always offered before its terminal,
                // each exactly once - no duplicate and no redelivery. Per-tree
                // ordering, duplicates and redelivery are the tally model's scope
                // and the shipper's terminal hold's; this model's is the hand-off.
                if (prepareOutstanding[tree])
                {
                    steps.Add(() =>
                    {
                        prepareOutstanding[tree] = false;
                        if (!terminalApplied[tree])
                        {
                            pending[tree] = true;
                        }
                    });
                }
                else if (terminalOutstanding[tree])
                {
                    steps.Add(() =>
                    {
                        terminalOutstanding[tree] = false;

                        // The tree's own tally, pinned to its first and only arrival:
                        // each tree here has one source shard, so its stamped count is 1.
                        // Multi-shard tallies, duplicates and the ungated path are
                        // CrossClusterReceiverTallyModel's scope; this model's is the
                        // hand-off after a tree's tally completes.
                        var expected = TerminalArrivalTally.MergeExpected(hadPrevious: false, previousExpected: 0, incomingExpected: 1);
                        if (TerminalArrivalTally.IsFinalArrival(arrivalCount: 1, expected) && stage[tree] == Stage.Idle)
                        {
                            stage[tree] = guard == CrossTreeBarrierGuard.NotifyBeforeRegister ? Stage.Notify : Stage.Register;
                        }
                    });
                }

                if (stage[tree] == Stage.Register)
                {
                    steps.Add(() =>
                    {
                        if (!registries[tree].TryResolve(txid, out _))
                        {
                            delegated[tree] = true;
                        }

                        stage[tree] = guard == CrossTreeBarrierGuard.NotifyBeforeRegister ? Stage.Done : Stage.Notify;
                    });
                }

                if (stage[tree] == Stage.Notify)
                {
                    steps.Add(() =>
                    {
                        arrived[treeIds[tree]] = new CrossTreeReceiverTerminal
                        {
                            OriginClusterId = "origin",
                            OperationId = "op",
                            TreeId = treeIds[tree],
                            TransactionId = txid,
                            // Scope pin: every tree carries the same outcome, so
                            // CommitsAll over a mixed arrival set is not explored
                            // here; CrossTreeReceiverBarrierTests pins it.
                            Committed = committed,
                            WaitSet = waitSet,
                            ObservedSourceShards = [0],
                            TerminalHlc = HybridLogicalClock.Zero,
                        };

                        var complete = guard == CrossTreeBarrierGuard.DecideOnFirstArrival
                            ? arrived.Count > 0
                            : CrossTreeReceiverBarrier.IsComplete(waitSet, arrived);
                        if (complete)
                        {
                            barrierDecided = true;
                            barrierCommitted = CrossTreeReceiverBarrier.CommitsAll(arrived);
                            Array.Fill(owedFinalise, true);
                        }

                        stage[tree] = guard == CrossTreeBarrierGuard.NotifyBeforeRegister ? Stage.Register : Stage.Done;
                    });
                }

                if (owedFinalise[tree])
                {
                    steps.Add(() =>
                    {
                        owedFinalise[tree] = false;
                        if (!registries[tree].TryResolve(txid, out _))
                        {
                            registries[tree].Apply(txid, barrierCommitted ? TxStatus.Committed : TxStatus.Aborted);
                        }

                        delegated[tree] = false;
                        dialFailing[tree] = false;
                        owedFanOut[tree] = true;
                    });
                }

                if (owedFanOut[tree])
                {
                    steps.Add(() =>
                    {
                        owedFanOut[tree] = false;
                        var isCommit = registries[tree].Resolve(txid) == TxStatus.Committed;
                        var action = MigrationTerminalCore.DecideBucketAction(pending[tree], terminalApplied[tree], isCommit);
                        if (action == MigrationTerminalBucketAction.DrainCommit)
                        {
                            materialised[tree] = true;
                        }

                        if (action != MigrationTerminalBucketAction.None)
                        {
                            pending[tree] = false;
                        }

                        terminalApplied[tree] = true;
                    });
                }

                if (delegated[tree] && dialFaultBudget > 0)
                {
                    steps.Add(() =>
                    {
                        dialFaultBudget--;
                        dialFailing[tree] = !dialFailing[tree];
                    });
                }
            }

            if (steps.Count == 0)
            {
                break;
            }

            steps[Pick(runtime, steps.Count)]();

            ProbeReaders(txid, registries, delegated, dialFailing, barrierDecided, barrierCommitted, pending, terminalApplied, materialised);
        }

        for (var t = 0; t < _trees; t++)
        {
            Specification.Assert(
                !pending[t],
                $"[RNoStrandedPrepare] receiver tree {t} still holds the saga's prepared bucket after the stream drained");
            if (committed)
            {
                Specification.Assert(
                    materialised[t],
                    $"[RCommittedEventuallyVisible] receiver tree {t} never materialised the committed saga's write");
            }
        }
    }

    /// <summary>
    /// What tree <paramref name="tree"/>'s receiver registry answers for the saga
    /// (<c>TxRegistryGrain.GetStatusAsync</c>): its local decision, else the
    /// barrier's published decision through the delegation - Indeterminate when
    /// the dial fails - else InFlight for a txid it has never heard of.
    /// </summary>
    private TxStatus RegistryAnswer(
        int tree,
        Guid txid,
        TxRegistryDecisionCore[] registries,
        bool[] delegated,
        bool[] dialFailing,
        bool barrierDecided,
        bool barrierCommitted)
    {
        if (registries[tree].TryResolve(txid, out var local))
        {
            return local;
        }

        if (!delegated[tree])
        {
            return TxStatus.InFlight;
        }

        if (dialFailing[tree])
        {
            return guard == CrossTreeBarrierGuard.UndialledDelegationReadsInFlight
                ? TxStatus.InFlight
                : TxStatus.Indeterminate;
        }

        return !barrierDecided ? TxStatus.InFlight
            : barrierCommitted ? TxStatus.Committed
            : TxStatus.Aborted;
    }

    private void ProbeReaders(
        Guid txid,
        TxRegistryDecisionCore[] registries,
        bool[] delegated,
        bool[] dialFailing,
        bool barrierDecided,
        bool barrierCommitted,
        bool[] pending,
        bool[] terminalApplied,
        bool[] materialised)
    {
        var anyPost = false;
        var anyPre = false;
        for (var t = 0; t < _trees; t++)
        {
            bool post;
            if (pending[t])
            {
                var status = RegistryAnswer(t, txid, registries, delegated, dialFailing, barrierDecided, barrierCommitted);
                // preparedHiddenByTombstoneOrExpiry is pinned false: a TTL or a
                // tombstone on the prepared VALUE is outside this model's scope (the
                // gate's Hidden arm for it is driven by AtomicCommitVisibilityModel).
                var outcome = AtomicVisibilityGate.ResolveKey(status, terminalApplied[t], preparedHiddenByTombstoneOrExpiry: false);
                if (outcome == PendingReadOutcome.Hidden)
                {
                    continue;
                }

                post = outcome == PendingReadOutcome.SurfacePrepared || materialised[t];
            }
            else
            {
                post = materialised[t];
            }

            anyPost |= post;
            anyPre |= !post;
        }

        Specification.Assert(
            !(anyPost && anyPre),
            "[RAllOrNothing] a receiver reader saw one tree's key post-saga and another's pre-saga");
        Specification.Assert(
            !anyPost || committed,
            "[RStrictIsolation] a receiver reader saw an uncommitted saga post-saga");
    }

    private static int Pick(ICoyoteRuntime runtime, int count)
    {
        for (var i = 0; i < count - 1; i++)
        {
            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return count - 1;
    }
}
