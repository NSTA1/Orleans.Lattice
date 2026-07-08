namespace Orleans.Lattice.Replication;

/// <summary>
/// Wire-shaped request DTO carrying the routing and identity arguments
/// for the cross-cluster saga control RPCs
/// (<c>Prepare</c>, <c>Commit</c>, <c>Abort</c>, <c>GetStatus</c>) the
/// <c>orleans.lattice.replication.LatticeSaga</c> service exposes. A
/// single request shape is reused across all four methods; the gRPC
/// method itself distinguishes the imperative operation, so no
/// discriminator field is carried on the DTO.
/// <para>
/// The DTO is independent of the gRPC binding so it can be reused by
/// any future transport that chooses to share the same wire shape.
/// Orleans serializer is the canonical encoder; the alias is stable
/// (<see cref="ReplicationTypeAliases.SagaControlRequest"/>).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SagaControlRequest)]
[Immutable]
public readonly record struct SagaControlRequest
{
    /// <summary>
    /// Stable identifier of the saga this control message targets. The
    /// participant keys its durable prepared/committed/aborted state by
    /// this id; must be non-empty.
    /// </summary>
    [Id(0)] public string SagaId { get; init; }

    /// <summary>
    /// Logical tree id the saga's mutation targets on the participant
    /// cluster. Must be non-empty.
    /// </summary>
    [Id(1)] public string TargetTree { get; init; }

    /// <summary>
    /// Identifier of the content manifest describing the mutation the
    /// saga proposes to apply. The participant uses it to correlate the
    /// prepared payload; may be empty for control messages that do not
    /// reference a manifest (for example a bare <c>GetStatus</c>).
    /// </summary>
    [Id(2)] public string ManifestId { get; init; }

    /// <summary>
    /// Stable cluster id of the coordinator that owns the saga. Carried
    /// so the participant can attribute the decision and so the
    /// transport-security peer-authorization gate can confirm the caller
    /// is a known peer before any participant state changes.
    /// </summary>
    [Id(3)] public string CoordinatorClusterId { get; init; }
}
