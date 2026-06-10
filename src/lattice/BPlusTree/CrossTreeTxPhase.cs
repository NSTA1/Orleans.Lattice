namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Lifecycle phase of a cross-tree atomic write, persisted in
/// <see cref="State.CrossTreeTxState"/> so the coordinator can resume after a
/// silo crash. The transition <see cref="Preparing"/> -&gt;
/// <see cref="Committed"/> / <see cref="Aborted"/> is the <b>single global
/// decision moment</b>: before it, every participating tree's delegated reads
/// resolve to invisible (pre-saga); after it, they all resolve to the recorded
/// verdict. Per-tree terminal fan-out (finalize) happens afterwards and is
/// invisible to readers because they dial the coordinator's already-recorded
/// decision.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeTxPhase)]
internal enum CrossTreeTxPhase
{
    /// <summary>Initial state - the coordinator has not yet started.</summary>
    NotStarted = 0,

    /// <summary>
    /// Dispatching prepare-and-pause to every participating tree's saga and
    /// collecting their votes. No global decision recorded yet, so all
    /// delegated reads resolve to invisible.
    /// </summary>
    Preparing = 1,

    /// <summary>
    /// Every participant voted <see cref="CrossTreePrepareVote.Prepared"/>; the
    /// global commit decision is recorded and now visible to every delegated
    /// reader. The coordinator is fanning out per-tree finalize (commit).
    /// </summary>
    Committed = 2,

    /// <summary>
    /// At least one participant failed to prepare (guard miss or genuine
    /// failure); the global abort decision is recorded and the coordinator is
    /// fanning out per-tree finalize (abort) to every prepared participant.
    /// </summary>
    Aborted = 3,

    /// <summary>
    /// Terminal - every participant has finalized. The memoized
    /// <see cref="State.CrossTreeTxState.Outcome"/> distinguishes a committed
    /// run from a precondition miss for idempotent re-attach.
    /// </summary>
    Completed = 4,
}
