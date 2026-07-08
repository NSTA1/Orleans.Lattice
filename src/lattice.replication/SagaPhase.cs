namespace Orleans.Lattice.Replication;

/// <summary>
/// Participant-observed phase of a cross-cluster saga, reported by the
/// <c>Prepare</c>, <c>Commit</c>, <c>Abort</c>, and <c>GetStatus</c> RPCs
/// of the <c>orleans.lattice.replication.LatticeSaga</c> control channel.
/// The value describes the durable state the participant holds for the
/// saga id at the moment the response is produced.
/// </summary>
public enum SagaPhase
{
    /// <summary>
    /// The participant holds no record for the saga id. Returned by a
    /// participant that has never seen a <c>Prepare</c> for the saga, or
    /// by the safe default handler that ships until the durable
    /// coordinator/participant model is wired.
    /// </summary>
    None = 0,

    /// <summary>
    /// The participant has durably prepared the saga and is holding the
    /// prepared state pending a <c>Commit</c> or <c>Abort</c> decision.
    /// </summary>
    Prepared = 1,

    /// <summary>
    /// The participant has committed the saga; the mutation is applied
    /// and the prepared state is released.
    /// </summary>
    Committed = 2,

    /// <summary>
    /// The participant has aborted the saga; any prepared state is
    /// released without applying the mutation.
    /// </summary>
    Aborted = 3,
}
