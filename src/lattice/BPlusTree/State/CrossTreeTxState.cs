namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.LatticeCrossTreeTxGrain"/>, the
/// coordinator of a cross-tree atomic write. One activation per
/// <c>operationId</c>. The coordinator is the <b>single global decision
/// authority</b>: per-tree registries delegate the status resolution of a
/// cross-tree saga's txid to this grain, so the
/// <see cref="CrossTreeTxPhase.Preparing"/> -&gt;
/// <see cref="CrossTreeTxPhase.Committed"/> / <see cref="CrossTreeTxPhase.Aborted"/>
/// transition flips the cross-tree batch's visibility on every participating
/// tree at one atomic moment.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeTxState)]
internal sealed class CrossTreeTxState
{
    /// <summary>Current lifecycle phase. Drives reminder-driven recovery.</summary>
    [Id(0)] public CrossTreeTxPhase Phase { get; set; } = CrossTreeTxPhase.NotStarted;

    /// <summary>
    /// The participating trees and their entries/guards, in submission order.
    /// Defensively copied from the caller's batch list before the first
    /// persist.
    /// </summary>
    [Id(1)] public List<CrossTreeParticipant> Participants { get; set; } = [];

    /// <summary>
    /// The caller-supplied cross-tree idempotency key (this grain's key).
    /// Persisted for logging and key-set-stability diagnostics.
    /// </summary>
    [Id(2)] public string OperationId { get; set; } = string.Empty;

    /// <summary>
    /// Stable fingerprint over the participating tree set and each tree's
    /// sorted key set, captured on the first submit. A re-submit of the same
    /// <see cref="OperationId"/> with a different tree-set or key-set is
    /// rejected, mirroring the single-tree saga's key-set-stability contract.
    /// </summary>
    [Id(3)] public byte[]? Fingerprint { get; set; }

    /// <summary>
    /// Memoized terminal outcome, set when the coordinator reaches
    /// <see cref="CrossTreeTxPhase.Completed"/>. Lets a delayed re-attach read
    /// back the original verdict without re-running the saga.
    /// </summary>
    [Id(4)] public CrossTreeAtomicWriteOutcome? Outcome { get; set; }

    /// <summary>
    /// Failure message for a saga that aborted on a genuine write failure (as
    /// opposed to a precondition miss). Re-thrown on re-attach so an idempotent
    /// retry observes the original failure rather than a false success.
    /// </summary>
    [Id(5)] public string? FailureMessage { get; set; }

    /// <summary>
    /// Wall-clock UTC tick stamped on first submit; drives the end-to-end
    /// cross-tree saga duration metric emitted on completion.
    /// </summary>
    [Id(6)] public long StartedAtTicks { get; set; }
}
