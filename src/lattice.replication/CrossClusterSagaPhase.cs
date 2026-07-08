namespace Orleans.Lattice.Replication;

/// <summary>
/// Lifecycle phase of a cross-cluster saga, persisted in
/// <see cref="Grains.CrossClusterSagaCoordinatorState"/> so the coordinator can
/// resume after a silo crash. Generalises the intra-cluster
/// <c>CrossTreeTxPhase</c> to a set of cluster participants reached over the
/// cross-cluster saga control channel. The transition
/// <see cref="Preparing"/> -&gt; <see cref="Committed"/> / <see cref="Aborted"/>
/// is the <b>single global decision moment</b>: before it, readers that dial
/// <see cref="Grains.ICrossClusterSagaCoordinatorGrain.GetDecisionAsync"/> see
/// the in-flight (pre-saga) view; after it, they all resolve to the recorded
/// verdict. Per-participant terminal fan-out (finalize) happens afterwards and
/// is invisible to readers because they dial the coordinator's already-recorded
/// decision.
/// <para>
/// This is the coordinator <b>lifecycle</b> phase and is distinct from the
/// participant-observed wire <see cref="SagaPhase"/> carried on
/// <see cref="SagaControlResponse"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaPhase)]
internal enum CrossClusterSagaPhase
{
    /// <summary>Initial state - the coordinator has not yet started.</summary>
    NotStarted = 0,

    /// <summary>
    /// Dispatching prepare to every participant cluster and collecting their
    /// votes. No global decision is recorded yet, so every dialled read
    /// resolves to in-flight.
    /// </summary>
    Preparing = 1,

    /// <summary>
    /// Every participant voted <see cref="SagaVote.Commit"/>; the global commit
    /// decision is recorded and now visible to every dialled reader. The
    /// coordinator is fanning out per-participant commit.
    /// </summary>
    Committed = 2,

    /// <summary>
    /// At least one participant returned a non-<see cref="SagaVote.Commit"/>
    /// vote (or a genuine failure); the global abort decision is recorded and
    /// the coordinator is fanning out compensation (abort) to every prepared
    /// participant.
    /// </summary>
    Aborted = 3,

    /// <summary>
    /// Terminal - every participant has finalized. The memoized
    /// <see cref="Grains.CrossClusterSagaCoordinatorState.Outcome"/>
    /// distinguishes a committed run from an aborted one for idempotent
    /// re-attach.
    /// </summary>
    Completed = 4,
}
