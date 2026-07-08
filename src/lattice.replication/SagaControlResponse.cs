namespace Orleans.Lattice.Replication;

/// <summary>
/// Wire-shaped response DTO returned from the cross-cluster saga control
/// RPCs (<c>Prepare</c>, <c>Commit</c>, <c>Abort</c>, <c>GetStatus</c>)
/// the <c>orleans.lattice.replication.LatticeSaga</c> service exposes. A
/// single response shape is reused across all four methods; the
/// <see cref="Vote"/> slot is only meaningful on the <c>Prepare</c>
/// reply.
/// <para>
/// Orleans serializer is the canonical encoder; the alias is stable
/// (<see cref="ReplicationTypeAliases.SagaControlResponse"/>).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SagaControlResponse)]
[Immutable]
public readonly record struct SagaControlResponse
{
    /// <summary>
    /// The saga id this response is for. Echoes
    /// <see cref="SagaControlRequest.SagaId"/> so the coordinator can
    /// correlate the reply.
    /// </summary>
    [Id(0)] public string SagaId { get; init; }

    /// <summary>
    /// The durable phase the participant holds for the saga at the
    /// moment the response is produced.
    /// </summary>
    [Id(1)] public SagaPhase Phase { get; init; }

    /// <summary>
    /// The participant vote. Only meaningful on the <c>Prepare</c>
    /// reply; carries <see cref="SagaVote.None"/> on the
    /// <c>Commit</c>, <c>Abort</c>, and <c>GetStatus</c> replies.
    /// </summary>
    [Id(2)] public SagaVote Vote { get; init; }

    /// <summary>
    /// Optional human-readable detail describing why the participant
    /// reached the reported phase/vote (for example an abort reason).
    /// May be empty.
    /// </summary>
    [Id(3)] public string Detail { get; init; }
}
