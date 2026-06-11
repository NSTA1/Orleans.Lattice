using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Coordinator for a cross-tree atomic write. One activation per cross-tree
/// <c>operationId</c> (this grain's key). Drives a two-level saga: each
/// participating tree's <see cref="IAtomicWriteGrain"/> runs in prepare-and-pause
/// mode (stages its writes into hidden pending buckets, then pauses), and only
/// after <b>every</b> tree votes <see cref="CrossTreePrepareVote.Prepared"/>
/// does this grain record the single global commit decision and fan out the
/// per-tree finalize.
/// <para>
/// <b>Decision authority.</b> Per-tree registries delegate the status
/// resolution of a cross-tree saga's txid to this grain via
/// <see cref="GetDecisionAsync"/>. The
/// <see cref="CrossTreeTxPhase.Preparing"/> -&gt; <see cref="CrossTreeTxPhase.Committed"/>
/// transition is therefore the one atomic moment at which the cross-tree batch
/// becomes visible on every participating tree, giving readers the same
/// no-partial-view guarantee the single-tree saga provides within one tree.
/// </para>
/// </summary>
[Alias(TypeAliases.ILatticeCrossTreeTxGrain)]
internal interface ILatticeCrossTreeTxGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts (or resumes / re-attaches to) the cross-tree atomic write for
    /// <paramref name="batches"/>. Returns the terminal outcome:
    /// <see cref="CrossTreeAtomicWriteOutcome.Committed"/> when every tree's
    /// guard passed and all writes committed atomically, or
    /// <see cref="CrossTreeAtomicWriteOutcome.PreconditionFailed"/> when a guard
    /// failed and nothing was committed. Throws
    /// <see cref="InvalidOperationException"/> if a write fails and compensation
    /// completes, or if the same operationId is re-submitted with a different
    /// tree-set or key-set.
    /// </summary>
    /// <param name="batches">Per-tree slices to commit atomically. Tree ids must be distinct.</param>
    Task<CrossTreeAtomicWriteOutcome> CommitAsync(List<LatticeTreeBatch> batches);

    /// <summary>
    /// The single global decision for this cross-tree saga, dialled by every
    /// participating tree's registry when resolving a delegated txid. Returns
    /// <see cref="TxStatus.InFlight"/> while the coordinator is still preparing
    /// (so delegated reads see the pre-saga view), then the recorded
    /// <see cref="TxStatus.Committed"/> / <see cref="TxStatus.Aborted"/> verdict
    /// the instant the global decision is made. Pure read, safe to interleave.
    /// </summary>
    [AlwaysInterleave]
    Task<TxStatus> GetDecisionAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator has reached a terminal state
    /// (or was never started). Used by tests and idempotent re-attach.
    /// </summary>
    Task<bool> IsCompleteAsync();
}
