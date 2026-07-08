namespace Orleans.Lattice.Replication;

/// <summary>
/// Participant vote returned from the <c>Prepare</c> RPC of the
/// <c>orleans.lattice.replication.LatticeSaga</c> control channel. The
/// coordinator collects a vote from every participant and commits only
/// when every vote is <see cref="Commit"/>.
/// </summary>
public enum SagaVote
{
    /// <summary>
    /// No vote has been cast. Carried on responses from RPCs other than
    /// <c>Prepare</c> (<c>Commit</c>, <c>Abort</c>, <c>GetStatus</c>),
    /// where the vote slot is not meaningful.
    /// </summary>
    None = 0,

    /// <summary>
    /// The participant has durably prepared and votes to commit. The
    /// coordinator may proceed to <c>Commit</c> once every participant
    /// votes <see cref="Commit"/>.
    /// </summary>
    Commit = 1,

    /// <summary>
    /// The participant cannot prepare and votes to abort. A single
    /// <see cref="Abort"/> vote forces the coordinator to abort the
    /// saga. This is the safe default when no participant is wired.
    /// </summary>
    Abort = 2,
}
