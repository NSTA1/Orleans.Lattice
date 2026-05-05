namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Terminal outcome of a cross-cluster atomic-batch apply submitted via
/// <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicApplyOutcome)]
internal enum AtomicApplyOutcome
{
    /// <summary>Every entry in the batch was committed atomically.</summary>
    Committed = 0,

    /// <summary>
    /// The saga failed mid-flight; every already-committed entry was
    /// rolled back via LWW-winning compensation rewrites against the
    /// pre-saga values.
    /// </summary>
    Compensated = 1,
}

/// <summary>
/// Result returned by
/// <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/>. Mirrors
/// the shape of the existing replication-side <c>ApplyResult</c> so the
/// receiver-side adapter can route batched and per-entry applies through
/// a common outcome path.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicApplyResult)]
[Immutable]
internal readonly record struct AtomicApplyResult
{
    /// <summary>The terminal outcome of the saga.</summary>
    [Id(0)] public AtomicApplyOutcome Outcome { get; init; }

    /// <summary>
    /// Number of entries persisted to the local tree. Equals the input
    /// batch length on <see cref="AtomicApplyOutcome.Committed"/>;
    /// <c>0</c> on <see cref="AtomicApplyOutcome.Compensated"/> because
    /// every committed entry was rolled back via LWW-winning rewrites.
    /// </summary>
    [Id(1)] public int AppliedCount { get; init; }

    /// <summary>
    /// Diagnostic message captured from the failure that pivoted the
    /// saga into compensation; <see langword="null"/> on
    /// <see cref="AtomicApplyOutcome.Committed"/>.
    /// </summary>
    [Id(2)] public string? FailureReason { get; init; }
}
