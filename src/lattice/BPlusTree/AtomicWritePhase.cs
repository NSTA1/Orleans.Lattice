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
    /// <summary>Initial state — the saga has not yet started.</summary>
    NotStarted = 0,

    /// <summary>Reading pre-saga values for each key so compensation can roll back.</summary>
    Prepare = 1,

    /// <summary>Applying writes sequentially. <c>NextIndex</c> advances after each commit.</summary>
    Execute = 2,

    /// <summary>A write failed; rolling back previously-committed keys with fresh HLC ticks.</summary>
    Compensate = 3,

    /// <summary>Saga finished — either all writes committed or compensation completed.</summary>
    Completed = 4,
}
