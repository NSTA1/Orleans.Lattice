namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A participating tree's vote in the prepare phase of a cross-tree atomic
/// write, returned by
/// <see cref="IAtomicWriteGrain.PrepareForCoordinatorAsync"/>. The coordinator
/// commits the whole batch only if <b>every</b> participant votes
/// <see cref="Prepared"/>; any other vote forces a global abort.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreePrepareVote)]
internal enum CrossTreePrepareVote
{
    /// <summary>
    /// The tree's writes were staged into the leaf pending buckets (hidden from
    /// readers) and the per-tree registry now delegates the saga's status
    /// resolution to the coordinator. The sub-saga is paused, awaiting the
    /// coordinator's finalize call.
    /// </summary>
    Prepared = 0,

    /// <summary>
    /// The tree's guard predicate failed for at least one key (or a targeted
    /// key had no live pre-saga value). Nothing was staged; the sub-saga is
    /// terminal. Forces a global abort reported as
    /// <see cref="CrossTreeAtomicWriteOutcome.PreconditionFailed"/>.
    /// </summary>
    PreconditionFailed = 1,

    /// <summary>
    /// A genuine error occurred while staging the tree's writes; the sub-saga
    /// self-compensated and is terminal-failed. Forces a global abort surfaced
    /// to the caller as an exception.
    /// </summary>
    Failed = 2,
}
