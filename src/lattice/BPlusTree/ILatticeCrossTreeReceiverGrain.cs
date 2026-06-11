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
/// into any grain - it only returns the set of trees to finalize. The calling
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
    /// receiver); later terminals must carry an identical wait set or are
    /// rejected. Returns a <see cref="CrossTreeReceiverDecision"/> whose
    /// <see cref="CrossTreeReceiverDecision.Decided"/> is <c>false</c> while the
    /// wait set is incomplete, and otherwise carries the global commit/abort
    /// verdict plus the per-tree finalize records the caller must materialize.
    /// </summary>
    Task<CrossTreeReceiverDecision> NotifyTerminalAsync(CrossTreeReceiverTerminal terminal);

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
    /// terminal and validated for exact match on later terminals. A tree that
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
