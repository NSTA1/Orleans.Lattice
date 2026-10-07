namespace Orleans.Lattice;

/// <summary>
/// Why a saga decision registry refused a call while a snapshot capture held
/// its decision gate (issue #4485).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxDecisionGateRefusal)]
internal enum TxDecisionGateRefusal
{
    /// <summary>
    /// A call that would record a new commit/abort decision was refused while a
    /// capture held the gate. Retryable: the caller waits and re-issues the same
    /// idempotent call, which is admitted once the gate is released or its lease
    /// lapses.
    /// </summary>
    DecisionGated = 0,

    /// <summary>
    /// A new cross-tree delegation registration was refused while a
    /// cross-tree-consistent backup set held the capture fence. Not retried by
    /// the authoring sub-saga, which compensates and votes Failed so its
    /// coordinator aborts; a receiver defers the replicated entry.
    /// </summary>
    RegistrationFenced = 1,

    /// <summary>
    /// The capture's own gate token is no longer held (released, lapsed, or lost
    /// to a registry reactivation). The capture fails closed and retries.
    /// </summary>
    GateLapsed = 2,
}
