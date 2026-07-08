namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// One participant cluster's record inside
/// <see cref="CrossClusterSagaCoordinatorState"/>: the target cluster id and
/// the vote the coordinator recorded for it after the prepare phase. Mutable so
/// the coordinator can stamp the <see cref="Vote"/> after collecting the
/// prepare response without reallocating the participant list.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossClusterSagaParticipantRef)]
internal sealed class CrossClusterSagaParticipantRef
{
    /// <summary>The stable id of the participant cluster this record targets.</summary>
    [Id(0)] public string ClusterId { get; set; } = string.Empty;

    /// <summary>
    /// The vote recorded for this participant after the prepare phase, or
    /// <see langword="null"/> while the prepare dispatch is still in flight.
    /// Only a participant that voted <see cref="SagaVote.Commit"/> has prepared
    /// state to finalize (commit or compensate) in the finalize phase.
    /// </summary>
    [Id(1)] public SagaVote? Vote { get; set; }
}
