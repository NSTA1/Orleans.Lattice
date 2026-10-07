using Orleans.Concurrency;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Receiver-side coordinator for a replicated cross-tree atomic write. One
/// activation per <c>(originClusterId, operationId)</c> pair (this grain's
/// compound key) on a <b>receiver</b> cluster. It is the single global decision
/// authority for the cross-tree batch <i>on this receiver</i>, mirroring the
/// authoring-side <see cref="ILatticeCrossTreeTxGrain"/> but driven by the
/// terminals that arrive over replication rather than by a local saga.
/// <para>
/// <b>Why this exists.</b> Each participating tree replicates its own per-tree
/// saga terminals independently, so without a receiver-side barrier a remote
/// reader could observe tree A's slice of a cross-tree batch committed while
/// tree B's slice is still pre-saga - a partial cross-tree view that the
/// authoring cluster never exposes. This coordinator re-imposes the all-or-nothing
/// visibility flip: every participating tree's registry delegates its replicated
/// sub-saga's status to this grain (via
/// <c>RegisterReceiverDecisionAuthorityAsync</c>), so all delegated reads return
/// <see cref="TxStatus.InFlight"/> until the coordinator's wait set completes,
/// then flip to the global verdict together.
/// </para>
/// <para>
/// <b>Deadlock-freedom.</b> <see cref="NotifyTerminalAsync"/> never calls back
/// into a participant grain - it only returns the set of trees to finalize; the
/// one grain it calls, the trees' <see cref="ICrossTreeBarrierIndexGrain"/>,
/// calls nothing. The calling
/// <c>LatticeGrain</c> performs the finalizes after the call returns (self-tree
/// inline, sibling trees via their own apply grains), so no circular grain wait
/// is possible.
/// </para>
/// </summary>
[Alias(TypeAliases.ILatticeCrossTreeReceiverGrain)]
internal interface ILatticeCrossTreeReceiverGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records the arrival of one participating tree's fully-gated cross-tree
    /// terminal on this receiver. Idempotent and durable: the coordinator
    /// persists its state before returning, so the registration that precedes
    /// this call is linearized against a durable decision. The first terminal
    /// freezes the wait set (the participant tree-ids replicated on this
    /// receiver), and the frozen set is authoritative: a later terminal's
    /// differing wait set - the receiver's replicated trees changed mid-operation
    /// - is ignored, and a terminal for a tree outside the frozen set joins it,
    /// or, once the barrier has decided, is finalized with its verdict (issue
    /// #4692). Returns a <see cref="CrossTreeReceiverDecision"/> whose
    /// <see cref="CrossTreeReceiverDecision.Decided"/> is <c>false</c> while the
    /// wait set is incomplete, and otherwise carries the global commit/abort
    /// verdict plus the per-tree finalize records the caller must materialize.
    /// </summary>
    Task<CrossTreeReceiverDecision> NotifyTerminalAsync(CrossTreeReceiverTerminal terminal);

    /// <summary>
    /// Records that participating tree <paramref name="treeId"/> is no longer
    /// replicated on this receiver (issue #4692): its terminal was dropped at the
    /// receiver's enrollment gate, so it will never arrive. An undecided barrier
    /// that still waits for the tree removes it from its wait set and decides if
    /// every remaining tree has arrived, by the usual rule (commit iff every
    /// arrival committed). A tree that already arrived, a tree outside the wait
    /// set, a barrier that has not opened, and a decided barrier are left
    /// unchanged. Call it only for a tree that has really stopped being
    /// replicated here - never because a terminal was dropped for another reason.
    /// Returns the barrier's decision, including the finalize records the caller
    /// must materialize when this call decided it.
    /// </summary>
    Task<CrossTreeReceiverDecision> NotifyParticipantAbsentAsync(string treeId);

    /// <summary>
    /// The single global decision for this cross-tree batch on this receiver,
    /// dialled by every participating tree's registry when resolving a delegated
    /// txid. Returns <see cref="TxStatus.InFlight"/> while the wait set is
    /// incomplete (so delegated reads see the pre-saga view), then the recorded
    /// <see cref="TxStatus.Committed"/> / <see cref="TxStatus.Aborted"/> verdict
    /// the instant the barrier completes. Pure read, safe to interleave.
    /// </summary>
    [AlwaysInterleave]
    Task<TxStatus> GetDecisionAsync();

    /// <summary>
    /// Whether the barrier has opened and decided, its identity, frozen wait
    /// set and arrived trees (issue #4684). A decision still awaiting its
    /// persist reads as undecided. Pure read, safe to interleave.
    /// </summary>
    [AlwaysInterleave]
    Task<CrossTreeReceiverStatus> GetStatusAsync();

    /// <summary>
    /// Records the operation's decision stamps (issue #4684): per participating
    /// tree, that tree's snapshot export epoch read at the origin after the
    /// decision was durable. Every terminal and decision row of the operation
    /// carries the same stamps; the first recorded stand. Call it before the
    /// terminal or decision row that carried them is notified. Returns the
    /// barrier's decision, re-evaluated as <see cref="ReevaluateAsync"/> does.
    /// </summary>
    /// <param name="stamps">The decision stamps; empty for an operation decided before stamping.</param>
    /// <param name="sequences">The decision sequences (issue #4733), or <see langword="null"/>.</param>
    /// <param name="participants">Every tree the operation touched (issue #4733).</param>
    Task<CrossTreeReceiverDecision> RecordDecisionStampsAsync(
        IReadOnlyDictionary<string, long> stamps,
        IReadOnlyDictionary<string, long>? sequences = null,
        IReadOnlyList<string>? participants = null);

    /// <summary>
    /// Re-evaluates the barrier against its trees' latest snapshot imports
    /// (issue #4684), as it does whenever it opens or records an arrival. A tree
    /// of the wait set that has not arrived arrives with its siblings' verdict -
    /// one cross-tree operation has one verdict - when its latest import from
    /// the origin came from an export that opened after the operation's
    /// decision (an export epoch greater than the tree's decision stamp; an
    /// operation decided by a silo that predates stamping counts as decided
    /// before every export the origin serves) and named the operation on no
    /// row. Such an export carried the sub-saga's outcome as plain rows,
    /// because the origin had purged it, so the import left nothing to
    /// finalize. Called by an import after it records itself. A barrier that
    /// has not opened is left unchanged. Returns the barrier's decision.
    /// </summary>
    Task<CrossTreeReceiverDecision> ReevaluateAsync();

    /// <summary>
    /// Whether this barrier still holds the read fence of an import of
    /// <paramref name="treeId"/> (issues #4684, #4730): it has opened, still
    /// waits for the tree, and has no durable decision. Otherwise - it never
    /// opened (its open write failed after it indexed itself), it was cleared
    /// after deciding (a barrier arms its retention only once its decision is
    /// durable), it decided, or it stopped waiting for the tree - it durably
    /// withdraws its entry from the tree's barrier index and returns
    /// <see langword="false"/>, so a stale entry never pins the tree's fence.
    /// Deliberately not interleaved: it is serialized with every call that
    /// opens or decides the barrier, so it cannot withdraw an entry an
    /// in-flight open is about to rely on. A failed withdrawal throws, and
    /// the caller keeps the fence.
    /// </summary>
    Task<bool> SettleIndexEntryAsync(string treeId);

    /// <summary>
    /// Whether this barrier must still be kept (issue #4733). A decided
    /// tombstone - what the barrier's retention compacts it to - is dropped,
    /// durably, once <paramref name="frontiers"/>, the origin's advertised purge
    /// frontier per tree, has reached the operation's decision sequence on
    /// every participant: no arrival of the operation can reach this receiver
    /// any more, so it can never reopen. Returns <see langword="true"/> while
    /// the tombstone must be kept; <see langword="false"/> once it was dropped,
    /// or when this barrier is not a tombstone at all (it holds no state, or it
    /// reopened), so the caller removes its entry from the tree's tombstone
    /// list. Not interleaved: it never drops a barrier mid-call.
    /// </summary>
    Task<bool> SettleTombstoneAsync(IReadOnlyDictionary<string, long> frontiers);

    /// <summary>
    /// Abandons this barrier because its origin was decommissioned (issues
    /// #4742, #4736): durably withdraws it from the barrier index of every tree
    /// in its wait set and from the tombstone list of every participant, then
    /// clears it to unopened, deciding nothing. Every tree of a barrier is a
    /// replica of its one origin, so once that origin is gone there is no tree
    /// left to vote; deciding on the arrivals so far would serve a verdict the
    /// operation's other trees never reach. An abandoned sub-saga resolves
    /// through its registry as <see cref="TxStatus.InFlight"/>, so the
    /// decommission's per-tree pending clear aborts every tree of the
    /// operation alike. A later arrival opens a fresh barrier. The withdrawals
    /// precede the clear, so a failed withdrawal throws and leaves the barrier
    /// as it was. Returns whether the barrier held any state.
    /// </summary>
    Task<bool> AbandonAsync();
}

/// <summary>
/// One participating tree's fully-gated cross-tree terminal, handed to
/// <see cref="ILatticeCrossTreeReceiverGrain.NotifyTerminalAsync"/> after that
/// tree's per-shard arrival gate has completed on the receiver.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeReceiverTerminal)]
internal sealed record CrossTreeReceiverTerminal
{
    /// <summary>The id of the source cluster that authored the cross-tree batch.</summary>
    [Id(0)] public required string OriginClusterId { get; init; }

    /// <summary>The cross-tree operation id (the authoring coordinator's key).</summary>
    [Id(1)] public required string OperationId { get; init; }

    /// <summary>The receiver-side tree id whose terminal this is.</summary>
    [Id(2)] public required string TreeId { get; init; }

    /// <summary>The replicated sub-saga's transaction id on <see cref="TreeId"/>.</summary>
    [Id(3)] public required Guid TransactionId { get; init; }

    /// <summary><c>true</c> for a commit terminal; <c>false</c> for abort.</summary>
    [Id(4)] public required bool Committed { get; init; }

    /// <summary>
    /// The set of participant tree-ids that are replicated on this receiver
    /// (<c>participants ∩ trees-replicated-here</c>). Frozen on the first
    /// terminal; a later terminal's value is advisory (issue #4692). A tree that
    /// the cross-tree batch touched but which is <i>not</i> replicated on this
    /// receiver is absent, so the barrier completes without waiting for it -
    /// partial-replication cross-tree batches are valid and flip on the subset
    /// that is present here.
    /// </summary>
    [Id(5)] public required IReadOnlyList<string> WaitSet { get; init; }

    /// <summary>
    /// The receiver-side source-shard indices observed for this tree's saga
    /// (from the per-tree arrival tally), used to seed the deferred terminal
    /// fan-out when the barrier completes.
    /// </summary>
    [Id(6)] public required IReadOnlyList<int> ObservedSourceShards { get; init; }

    /// <summary>The HLC the source cluster stamped on this tree's terminal.</summary>
    [Id(7)] public required HybridLogicalClock TerminalHlc { get; init; }
}

/// <summary>
/// A single tree's deferred-materialization record, returned in
/// <see cref="CrossTreeReceiverDecision.TreesToFinalize"/> once the barrier
/// completes. The caller marks this tree's registry with the global verdict and
/// fans the terminal out to the tree's leaves.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeReceiverTreeFinalize)]
internal sealed record CrossTreeReceiverTreeFinalize
{
    /// <summary>The receiver-side tree id to finalize.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The replicated sub-saga's transaction id on <see cref="TreeId"/>.</summary>
    [Id(1)] public required Guid TransactionId { get; init; }

    /// <summary>The source-shard indices to seed the terminal fan-out for this tree.</summary>
    [Id(2)] public required IReadOnlyList<int> ObservedSourceShards { get; init; }

    /// <summary>The source cluster's terminal HLC for this tree, re-stamped verbatim on fan-out.</summary>
    [Id(3)] public required HybridLogicalClock TerminalHlc { get; init; }

    /// <summary>The id of the source cluster that authored the terminal.</summary>
    [Id(4)] public required string OriginClusterId { get; init; }
}

/// <summary>
/// The result of <see cref="ILatticeCrossTreeReceiverGrain.NotifyTerminalAsync"/>:
/// whether the cross-tree barrier has completed and, if so, the global verdict
/// plus the per-tree finalize records the caller must materialize.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeReceiverDecision)]
internal sealed record CrossTreeReceiverDecision
{
    /// <summary>
    /// <c>true</c> once every tree in the frozen wait set has notified its
    /// terminal and the global decision is recorded. While <c>false</c>,
    /// <see cref="TreesToFinalize"/> is empty and the caller returns without
    /// materializing anything (the next terminal re-evaluates).
    /// </summary>
    [Id(0)] public required bool Decided { get; init; }

    /// <summary>
    /// The global verdict: <c>true</c> iff every participating tree committed.
    /// Meaningful only when <see cref="Decided"/> is <c>true</c>.
    /// </summary>
    [Id(1)] public required bool Committed { get; init; }

    /// <summary>
    /// The per-tree finalize records the caller must materialize (mark registry
    /// + fan out terminals). Returned in full on every decided notify so a
    /// redelivered terminal re-heals materialization idempotently, mirroring the
    /// single-tree gate's redelivery-heal model. Empty when
    /// <see cref="Decided"/> is <c>false</c>.
    /// </summary>
    [Id(2)] public required IReadOnlyList<CrossTreeReceiverTreeFinalize> TreesToFinalize { get; init; }

    /// <summary>A not-yet-decided result with no trees to finalize.</summary>
    public static CrossTreeReceiverDecision InFlight { get; } = new()
    {
        Decided = false,
        Committed = false,
        TreesToFinalize = [],
    };
}
