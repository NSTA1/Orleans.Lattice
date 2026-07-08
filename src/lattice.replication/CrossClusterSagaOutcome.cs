namespace Orleans.Lattice.Replication;

/// <summary>
/// Terminal outcome of a cross-cluster saga, memoized in
/// <see cref="Grains.CrossClusterSagaCoordinatorState.Outcome"/> when the
/// coordinator reaches <see cref="CrossClusterSagaPhase.Completed"/>. Lets a
/// delayed re-attach read back the original verdict without re-running the
/// saga.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaOutcome)]
internal enum CrossClusterSagaOutcome
{
    /// <summary>
    /// Every participant voted to commit; the global commit decision was
    /// recorded and every prepared participant committed.
    /// </summary>
    Committed = 0,

    /// <summary>
    /// At least one participant declined to prepare; the global abort decision
    /// was recorded and every prepared participant was compensated (rolled
    /// back).
    /// </summary>
    Aborted = 1,
}
