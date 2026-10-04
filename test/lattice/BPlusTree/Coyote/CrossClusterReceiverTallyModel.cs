using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// The fix a <see cref="CrossClusterReceiverTallyModel"/> run removes, if any.
/// Each guard removes exactly one, so a guard test that finds a violation shows
/// the model depends on that fix and on nothing else.
/// </summary>
internal enum CrossClusterTallyGuard
{
    /// <summary>The fixed design: every fix in place.</summary>
    None,

    /// <summary>
    /// The origin stamps no touched-shard count on its terminals, so the
    /// receiver takes the legacy "final on arrival" path for a multi-shard saga
    /// (the cross-cluster TLA+ module's <c>RAllOrNothingLegacyNoTallyMultiShard</c>).
    /// </summary>
    UnstampedTerminals,

    /// <summary>
    /// The receiver ignores the stamped count and treats every terminal as
    /// final (<c>RAllOrNothingMarksOnFirstTerminal</c>).
    /// </summary>
    MarkOnFirstTerminal,

    /// <summary>
    /// The transport may deliver a source shard's terminal before that shard's
    /// prepare, which production allowed until the shipper's terminal hold (issue #4480;
    /// <c>RAllOrNothingTerminalOvertakesPrepare</c>).
    /// </summary>
    TerminalMayOvertakePrepare,

    /// <summary>
    /// A receiver leaf stages a duplicate prepare that trails the saga's
    /// terminal instead of refusing it (<c>RNoStrandedPrepareLatePrepareStaged</c>).
    /// </summary>
    LatePrepareStaged,
}

/// <summary>
/// A Coyote model of the receiver half of cross-cluster atomic visibility for a
/// single-tree saga over several source shards, driving the <b>production</b>
/// cores the receiver runs: <see cref="TerminalArrivalTally"/> (the per-source-shard
/// tally in <c>TxRegistryGrain.RecordTerminalArrivalAsync</c>, including its
/// ungated legacy path), <see cref="TerminalDecisionGuard"/> (the mixed-outcome
/// guard on the same call), <see cref="TxRegistryDecisionCore"/> (the receiver
/// registry's decision map), <see cref="MigrationTerminalCore"/> (each receiver
/// leaf's bucket disposition when the post-gate fan-out reaches it) and
/// <see cref="AtomicVisibilityGate"/> (every reader probe).
/// <para>
/// The saga's replication records - one prepare and one terminal per source
/// shard - are delivered by a transport the model controls. Every choice is a
/// <see cref="ICoyoteRuntime.RandomBoolean()"/>: which outstanding record is
/// delivered next (reorder), whether its acknowledgement is lost so it is
/// delivered again (duplicate, bounded by <c>duplicates</c>), and when the
/// receiver's owed fan-out steps run. A lost delivery is a record that stays
/// outstanding, so it needs no choice of its own. The one ordering the
/// transport keeps is the design's: a shard's terminal is not delivered before
/// that shard's prepare (lifted by
/// <see cref="CrossClusterTallyGuard.TerminalMayOvertakePrepare"/>).
/// </para>
/// <para>
/// After every step a reader probes every key through the gate and the model
/// asserts the receiver-side invariants, each tagged so a guard test can name
/// the one it expects: <c>[RAllOrNothing]</c> (never one key post-saga and
/// another pre-saga) and <c>[RStrictIsolation]</c> (post-saga only for a
/// committed saga). When the transport has drained, it asserts bounded progress:
/// <c>[RCommittedEventuallyVisible]</c> (every key of a committed saga
/// materialised) and <c>[RNoStrandedPrepare]</c> (no bucket left). These are the
/// cross-cluster TLA+ module's properties of the same names.
/// </para>
/// <para>
/// Every per-iteration object is a local of <see cref="Run"/>; the fields are
/// immutable configuration only (issue #1664).
/// </para>
/// </summary>
internal sealed class CrossClusterReceiverTallyModel(
    int shards,
    bool committed,
    int duplicates,
    CrossClusterTallyGuard guard) : ICoyoteModel
{
    private readonly int _shards = shards >= 2
        ? shards
        : throw new ArgumentOutOfRangeException(nameof(shards), "a multi-shard saga needs at least two source shards");

    private enum RecordKind
    {
        Prepare,
        Terminal,
    }

    private readonly record struct ReplicationRecord(RecordKind Kind, int Shard, int Count);

    public void Run(ICoyoteRuntime runtime)
    {
        var txid = Guid.NewGuid();
        var stampedCount = guard == CrossClusterTallyGuard.UnstampedTerminals ? 0 : _shards;

        // The origin has written every prepare and, once decided, every terminal.
        // The origin side is AtomicCommit's concern; here it is the input.
        var outstanding = new List<ReplicationRecord>();
        for (var s = 0; s < _shards; s++)
        {
            outstanding.Add(new ReplicationRecord(RecordKind.Prepare, s, 0));
            outstanding.Add(new ReplicationRecord(RecordKind.Terminal, s, stampedCount));
        }

        var duplicateBudget = duplicates;
        var prepareDelivered = new bool[_shards];

        // Receiver leaves (one per source shard): pending bucket, applied
        // terminal (the recently-terminal memory), materialised projection.
        var pending = new bool[_shards];
        var terminalApplied = new bool[_shards];
        var materialised = new bool[_shards];

        // Receiver registry: decision map plus the per-saga tally state.
        var decisions = new Dictionary<Guid, TxStatus>();
        var registry = new TxRegistryDecisionCore(decisions, revision: 0);
        var arrivals = new HashSet<int>();
        var hadExpected = false;
        var expected = 0;
        var owedFanOut = new HashSet<int>();

        while (outstanding.Count > 0 || owedFanOut.Count > 0)
        {
            var fanOutNext = owedFanOut.Count > 0 && (outstanding.Count == 0 || runtime.RandomBoolean());
            if (fanOutNext)
            {
                var shard = Pick(runtime, owedFanOut);
                owedFanOut.Remove(shard);
                var recorded = registry.Resolve(txid);
                var isCommit = recorded == TxStatus.Committed;
                var action = MigrationTerminalCore.DecideBucketAction(pending[shard], terminalApplied[shard], isCommit);
                if (action == MigrationTerminalBucketAction.DrainCommit)
                {
                    materialised[shard] = true;
                }

                if (action != MigrationTerminalBucketAction.None)
                {
                    pending[shard] = false;
                }

                terminalApplied[shard] = true;
            }
            else
            {
                var index = PickDeliverable(runtime, outstanding, prepareDelivered);
                var record = outstanding[index];
                var ackLost = duplicateBudget > 0 && runtime.RandomBoolean();
                if (ackLost)
                {
                    duplicateBudget--;
                }
                else
                {
                    outstanding.RemoveAt(index);
                }

                if (record.Kind == RecordKind.Prepare)
                {
                    prepareDelivered[record.Shard] = true;

                    // A prepare trailing the terminal is refused
                    // (BPlusLeafGrain.IsLatePrepareForTerminalTransaction).
                    if (!terminalApplied[record.Shard] || guard == CrossClusterTallyGuard.LatePrepareStaged)
                    {
                        pending[record.Shard] = true;
                    }
                }
                else
                {
                    var hasExisting = registry.TryResolve(txid, out var existing);
                    if (TerminalDecisionGuard.Classify(hasExisting, existing, committed) == TerminalRecordAction.Conflict)
                    {
                        Specification.Assert(false, "[MixedOutcome] a terminal conflicted with the recorded decision");
                    }

                    bool final;
                    IEnumerable<int> observed;
                    if (TerminalArrivalTally.IsUngated(record.Count))
                    {
                        final = true;
                        observed = [record.Shard];
                    }
                    else
                    {
                        arrivals.Add(record.Shard);
                        expected = TerminalArrivalTally.MergeExpected(hadExpected, expected, record.Count);
                        hadExpected = true;
                        final = guard == CrossClusterTallyGuard.MarkOnFirstTerminal
                            || TerminalArrivalTally.IsFinalArrival(arrivals.Count, expected);
                        observed = guard == CrossClusterTallyGuard.MarkOnFirstTerminal ? [record.Shard] : arrivals.ToArray();
                    }

                    if (final)
                    {
                        if (!hasExisting)
                        {
                            registry.Apply(txid, committed ? TxStatus.Committed : TxStatus.Aborted);
                        }

                        owedFanOut.UnionWith(observed);
                    }
                }
            }

            ProbeReaders(registry.Resolve(txid), pending, terminalApplied, materialised, committed);
        }

        // Bounded progress: the transport has drained and nothing is owed.
        for (var s = 0; s < _shards; s++)
        {
            Specification.Assert(
                !pending[s],
                $"[RNoStrandedPrepare] receiver shard {s} still holds the saga's prepared bucket after the stream drained");
            if (committed)
            {
                Specification.Assert(
                    materialised[s],
                    $"[RCommittedEventuallyVisible] receiver shard {s} never materialised the committed saga's write");
            }
        }
    }

    /// <summary>
    /// Resolves every key through the production gate, as a multi-key reader
    /// would against one registry answer, and asserts the receiver invariants.
    /// </summary>
    private static void ProbeReaders(TxStatus status, bool[] pending, bool[] terminalApplied, bool[] materialised, bool committed)
    {
        var anyPost = false;
        var anyPre = false;
        for (var s = 0; s < pending.Length; s++)
        {
            bool post;
            if (pending[s])
            {
                // preparedHiddenByTombstoneOrExpiry is pinned false: a TTL or a
                // tombstone on the prepared VALUE is outside this model's scope (the
                // gate's Hidden arm for it is driven by AtomicCommitVisibilityModel).
                var outcome = AtomicVisibilityGate.ResolveKey(status, terminalApplied[s], preparedHiddenByTombstoneOrExpiry: false);
                if (outcome == PendingReadOutcome.Hidden)
                {
                    continue;
                }

                post = outcome == PendingReadOutcome.SurfacePrepared || materialised[s];
            }
            else
            {
                post = materialised[s];
            }

            anyPost |= post;
            anyPre |= !post;
        }

        Specification.Assert(
            !(anyPost && anyPre),
            "[RAllOrNothing] a receiver reader saw one key post-saga and another pre-saga");
        Specification.Assert(
            !anyPost || committed,
            "[RStrictIsolation] a receiver reader saw an uncommitted saga post-saga");
    }

    private static int Pick(ICoyoteRuntime runtime, HashSet<int> candidates)
    {
        var first = -1;
        foreach (var candidate in candidates)
        {
            if (first < 0)
            {
                first = candidate;
            }

            if (runtime.RandomBoolean())
            {
                return candidate;
            }
        }

        return first;
    }

    /// <summary>
    /// Picks the next record to deliver among those the transport may deliver
    /// now: any prepare, and any terminal whose shard's prepare has been
    /// delivered at least once (unless the guard lifts that ordering).
    /// </summary>
    private int PickDeliverable(ICoyoteRuntime runtime, List<ReplicationRecord> outstanding, bool[] prepareDelivered)
    {
        var first = -1;
        for (var i = 0; i < outstanding.Count; i++)
        {
            var record = outstanding[i];
            var deliverable = record.Kind == RecordKind.Prepare
                || guard == CrossClusterTallyGuard.TerminalMayOvertakePrepare
                || prepareDelivered[record.Shard];
            if (!deliverable)
            {
                continue;
            }

            if (first < 0)
            {
                first = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return first;
    }
}
