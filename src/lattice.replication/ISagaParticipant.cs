namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal service-provider interface (SPI) for a local saga participant. A
/// participant acts over the <b>set</b> of resources it hosts for a given saga
/// (identified by the saga's target resource set on
/// <see cref="SagaControlRequest"/>), not a single fixed object - for the
/// restore epic the resource set is the subset of the backup set's trees
/// present on the local cluster.
/// <para>
/// The durable participant model (<see cref="Grains.ICrossClusterSagaParticipantGrain"/>)
/// resolves the local participants for a saga and drives them through this SPI:
/// a resumable <see cref="PrepareAsync"/>, then exactly one terminal
/// <see cref="CommitAsync"/> or <see cref="AbortAsync"/> delivered by the
/// coordinator decision (or an auto-compensating <see cref="AbortAsync"/> fired
/// by the participant's fence timer if the coordinator never returns).
/// </para>
/// <para>
/// The SPI is called in-process on the participant cluster, so its arguments
/// and results are plain CLR types and are never serialized over a grain
/// boundary. It stays <c>internal</c>: the only production implementation is
/// the restore participant, delivered by a later sub-issue.
/// </para>
/// </summary>
internal interface ISagaParticipant
{
    /// <summary>
    /// Prepares the resource set this participant hosts for the saga. The work
    /// can be long-running (for example, a shadow build) and must be
    /// <b>idempotent and resumable</b>: a retry after a mid-prepare restart
    /// resumes from a checkpoint rather than restarting from zero. Returns the
    /// participant's vote.
    /// </summary>
    /// <param name="request">The saga identity and target resource set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's prepare result.</returns>
    Task<SagaParticipantPrepareResult> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the prepared resource set, making the prepared mutation durable
    /// and releasing the prepared state. Idempotent: a duplicate commit after a
    /// prior commit is a no-op.
    /// </summary>
    /// <param name="request">The saga identity and target resource set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Compensates (rolls back) the prepared resource set without applying the
    /// mutation, restoring the pre-prepare view. Idempotent: a duplicate abort,
    /// or an abort of a resource set that was never prepared, is a no-op.
    /// </summary>
    /// <param name="request">The saga identity and target resource set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports the phase the participant currently holds for the saga's
    /// resource set without changing any state.
    /// </summary>
    /// <param name="request">The saga identity and target resource set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant-observed phase.</returns>
    Task<SagaPhase> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default);
}
