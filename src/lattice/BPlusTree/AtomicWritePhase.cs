namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Lifecycle phase of an atomic multi-key write saga.
/// Persisted in <see cref="State.AtomicWriteState"/> so the saga can resume
/// after a silo crash.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicWritePhase)]
internal enum AtomicWritePhase
{
    /// <summary>Initial state - the saga has not yet started.</summary>
    NotStarted = 0,

    /// <summary>Reading pre-saga values for each key so compensation can roll back.</summary>
    Prepare = 1,

    /// <summary>Applying writes sequentially. <c>NextIndex</c> advances after each commit.</summary>
    Execute = 2,

    /// <summary>A write failed; rolling back previously-committed keys with fresh HLC ticks.</summary>
    Compensate = 3,

    /// <summary>Saga finished - either all writes committed or compensation completed.</summary>
    Completed = 4,

    /// <summary>
    /// Terminal: a guard predicate evaluated against the pre-saga snapshot
    /// failed for at least one key, so the saga aborted before any write and
    /// committed nothing. Distinct from <see cref="Completed"/> so a guarded
    /// caller can read back the memoized precondition-miss outcome on re-attach.
    /// </summary>
    PreconditionFailed = 5,

    /// <summary>
    /// Cross-tree prepare-and-pause: every write has been staged into the leaf
    /// pending buckets (hidden from readers) and the per-tree registry now
    /// delegates this saga's txid to the cross-tree coordinator, but the
    /// terminal decision has not been recorded. The saga stays in this phase
    /// until the coordinator calls <c>FinalizeAsync</c>, at which point it
    /// transitions to <see cref="Execute"/>-tail processing (commit) or
    /// <see cref="Compensate"/> (abort). Reminder ticks observed in this phase
    /// are a no-op - the coordinator drives the resume.
    /// </summary>
    Prepared = 6,
}
