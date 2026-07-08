namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable participant model for a cross-cluster saga on a participant cluster.
/// One activation per saga id (this grain's key). Fronted by
/// <see cref="Orleans.Lattice.Replication.ILatticeSagaControlHandler"/>, which
/// the gRPC saga service delegates to; the grain resolves the local
/// <see cref="ISagaParticipant"/>(s) for the saga's target resource set and
/// drives them through prepare / commit / abort, holding a durable prepared
/// record and a bounded cutover fence between prepare and the coordinator
/// decision.
/// <para>
/// The interface is <c>internal</c>: only the package's own handler reaches it.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.ICrossClusterSagaParticipantGrain)]
internal interface ICrossClusterSagaParticipantGrain : IGrainWithStringKey
{
    /// <summary>
    /// Handles a <c>Prepare</c>. Runs the local participants' resumable prepare,
    /// durably records the prepared state and vote, arms the cutover fence
    /// reminder, and returns the vote. Idempotent: a duplicate prepare returns
    /// the already-recorded vote/phase without re-running.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <returns>The participant's prepare response.</returns>
    Task<SagaControlResponse> PrepareAsync(SagaControlRequest request);

    /// <summary>
    /// Handles a <c>Commit</c>. Delivers the coordinator's commit decision to
    /// the local participants, cancels the fence, and persists the committed
    /// phase. Idempotent.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <returns>The participant's commit response.</returns>
    Task<SagaControlResponse> CommitAsync(SagaControlRequest request);

    /// <summary>
    /// Handles an <c>Abort</c>. Compensates (rolls back) the local participants,
    /// cancels the fence, and persists the aborted phase. Idempotent.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <returns>The participant's abort response.</returns>
    Task<SagaControlResponse> AbortAsync(SagaControlRequest request);

    /// <summary>
    /// Handles a <c>GetStatus</c>. Returns the durable phase the participant
    /// currently holds without changing any state.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <returns>The participant's status response.</returns>
    Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request);
}
