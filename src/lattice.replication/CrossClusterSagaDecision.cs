namespace Orleans.Lattice.Replication;

/// <summary>
/// The single global decision for a cross-cluster saga, dialled by readers
/// through
/// <see cref="Grains.ICrossClusterSagaCoordinatorGrain.GetDecisionAsync"/>. The
/// decision is the one source of truth for whether a participant commits or
/// compensates. It resolves to <see cref="InFlight"/> until the coordinator
/// records the global verdict, then to the recorded
/// <see cref="Committed"/> / <see cref="Aborted"/> value the instant the
/// decision is made.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaDecision)]
internal enum CrossClusterSagaDecision
{
    /// <summary>
    /// No global decision has been recorded yet (the coordinator has not
    /// started or is still preparing). Dialled readers see the pre-saga view.
    /// </summary>
    InFlight = 0,

    /// <summary>The coordinator recorded a global commit.</summary>
    Committed = 1,

    /// <summary>The coordinator recorded a global abort.</summary>
    Aborted = 2,
}
