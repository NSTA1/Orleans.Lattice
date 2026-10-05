namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="CrossClusterSagaParticipantGrain"/>, the
/// durable participant model on a participant cluster. One activation per saga
/// id (this grain's key). Records the durable phase the participant holds for
/// the saga, the recorded vote, and the cutover fence deadline that arms the
/// coordinator-loss auto-compensation safety net. Persisted across
/// deactivation so a duplicate control message is idempotent and the fence
/// survives a silo restart (the fence is anchored on an Orleans reminder, which
/// grain timers cannot provide).
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaParticipantState)]
internal sealed class CrossClusterSagaParticipantState
{
    /// <summary>
    /// The durable phase this participant holds for the saga, reported verbatim
    /// on <see cref="SagaControlResponse.Phase"/>. Starts at
    /// <see cref="SagaPhase.None"/>.
    /// </summary>
    [Id(0)] public SagaPhase Phase { get; set; } = SagaPhase.None;

    /// <summary>The saga id (this grain's key). Persisted for logging and diagnostics.</summary>
    [Id(1)] public string SagaId { get; set; } = string.Empty;

    /// <summary>Logical tree id the saga's mutation targets on this cluster.</summary>
    [Id(2)] public string TargetTree { get; set; } = string.Empty;

    /// <summary>Content-manifest id describing the prepared mutation.</summary>
    [Id(3)] public string ManifestId { get; set; } = string.Empty;

    /// <summary>Stable id of the coordinator cluster that owns the saga.</summary>
    [Id(4)] public string CoordinatorClusterId { get; set; } = string.Empty;

    /// <summary>
    /// The vote this participant cast on <c>Prepare</c>. Meaningful only once
    /// <see cref="Phase"/> has advanced past <see cref="SagaPhase.None"/>.
    /// </summary>
    [Id(5)] public SagaVote Vote { get; set; } = SagaVote.None;

    /// <summary>
    /// Wall-clock UTC tick at which the cutover fence expires. While
    /// <see cref="Phase"/> is <see cref="SagaPhase.Prepared"/>, a fence reminder
    /// that fires at or past this deadline auto-compensates (rolls back) the
    /// prepared resource set - the coordinator-loss safety net. Zero when no
    /// fence is armed.
    /// </summary>
    [Id(6)] public long FenceDeadlineTicks { get; set; }

    /// <summary>
    /// Optional detail describing why the participant reached its current
    /// phase/vote (for example an abort reason). Echoed on
    /// <see cref="SagaControlResponse.Detail"/>. May be <see langword="null"/>.
    /// </summary>
    [Id(7)] public string? Detail { get; set; }

    /// <summary>
    /// The backup set the saga restores as one unit, or <see langword="null"/>
    /// for a single-tree restore. Persisted at prepare so a decision this
    /// participant applies on its own - learned by querying the coordinator after
    /// its fence timer fired, or forced by an operator - reaches every member tree
    /// exactly as the coordinator's delivery would (issue #4637).
    /// </summary>
    [Id(8)] public string? SetId { get; set; }
}
