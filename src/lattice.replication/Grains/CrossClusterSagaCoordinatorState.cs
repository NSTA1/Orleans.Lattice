namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="CrossClusterSagaCoordinatorGrain"/>, the
/// coordinator of a cross-cluster saga. One activation per saga id (this
/// grain's key), living in the initiating cluster. The coordinator is the
/// <b>single global decision authority</b>: the
/// <see cref="CrossClusterSagaPhase.Preparing"/> -&gt;
/// <see cref="CrossClusterSagaPhase.Committed"/> /
/// <see cref="CrossClusterSagaPhase.Aborted"/> transition records the one
/// verdict every participant commits or compensates against. Persisted after
/// every phase transition so the coordinator is crash-resumable and re-attach
/// is idempotent.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaCoordinatorState)]
internal sealed class CrossClusterSagaCoordinatorState
{
    /// <summary>Current lifecycle phase. Drives reminder-driven recovery.</summary>
    [Id(0)] public CrossClusterSagaPhase Phase { get; set; } = CrossClusterSagaPhase.NotStarted;

    /// <summary>
    /// The participant clusters and their recorded votes, in canonical
    /// (ordinal-sorted, de-duplicated) order. Defensively built from the
    /// caller's cluster-id list before the first persist.
    /// </summary>
    [Id(1)] public List<CrossClusterSagaParticipantRef> Participants { get; set; } = [];

    /// <summary>The saga id (this grain's key). Persisted for logging and diagnostics.</summary>
    [Id(2)] public string SagaId { get; set; } = string.Empty;

    /// <summary>
    /// Logical tree id the saga's mutation targets on each participant cluster.
    /// Carried on every control request.
    /// </summary>
    [Id(3)] public string TargetTree { get; set; } = string.Empty;

    /// <summary>
    /// Identifier of the content manifest describing the proposed mutation.
    /// Carried on every control request.
    /// </summary>
    [Id(4)] public string ManifestId { get; set; } = string.Empty;

    /// <summary>
    /// Stable id of the coordinator's own (initiating) cluster, stamped onto
    /// every control request so participants can attribute the decision and the
    /// peer-authorization gate can confirm the caller.
    /// </summary>
    [Id(5)] public string CoordinatorClusterId { get; set; } = string.Empty;

    /// <summary>
    /// Stable fingerprint over the participant cluster set, target tree, and
    /// manifest id, captured on first submit. A re-submit of the same saga id
    /// with a different participant set / target / manifest is rejected,
    /// mirroring the intra-cluster saga's key-set-stability contract.
    /// </summary>
    [Id(6)] public byte[]? Fingerprint { get; set; }

    /// <summary>
    /// Memoized terminal outcome, set when the coordinator reaches
    /// <see cref="CrossClusterSagaPhase.Completed"/>. Lets a delayed re-attach
    /// read back the original verdict without re-running the saga.
    /// </summary>
    [Id(7)] public CrossClusterSagaOutcome? Outcome { get; set; }

    /// <summary>
    /// Failure detail for a saga that aborted (for example the first
    /// non-committing participant's detail, or a prepare-progress deadline
    /// breach). Surfaced for diagnostics; the abort verdict itself is carried
    /// by <see cref="Outcome"/>.
    /// </summary>
    [Id(8)] public string? FailureMessage { get; set; }

    /// <summary>
    /// Wall-clock UTC tick stamped on first submit. Anchors the coordinator-side
    /// prepare-progress (build) deadline, which is distinct from each
    /// participant's short cutover fence timer.
    /// </summary>
    [Id(9)] public long StartedAtTicks { get; set; }
}
