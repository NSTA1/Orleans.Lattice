namespace Orleans.Lattice.Replication;

/// <summary>
/// Public service-provider interface (SPI) for a local cross-cluster saga
/// participant. Implement it to enlist an application resource in a lattice
/// cross-cluster saga alongside the built-in restore participant, and register
/// the implementation with
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeSagaParticipant{TParticipant}(ISiloBuilder, string?)"/>.
/// <para>
/// A participant acts over the <b>set</b> of resources it hosts for a given saga
/// (identified by the saga's target resource set on
/// <see cref="SagaControlRequest"/>), not a single fixed object. A participant
/// that hosts nothing for a saga prepares vacuously (a no-op that votes
/// <see cref="SagaVote.Commit"/>) rather than blocking the saga.
/// </para>
/// <para>
/// The durable participant model (<see cref="Grains.ICrossClusterSagaParticipantGrain"/>)
/// resolves every local participant for a saga and drives them through this SPI:
/// a resumable <see cref="PrepareAsync"/>, then exactly one terminal
/// <see cref="CommitAsync"/> or <see cref="AbortAsync"/> delivered by the
/// coordinator decision (or an auto-compensating <see cref="AbortAsync"/> fired
/// by the participant model's bounded fence timer if the coordinator never
/// returns).
/// </para>
/// <para>
/// <b>Contract and guarantees.</b>
/// <list type="bullet">
///   <item><description><b>Unanimous prepare.</b> Every enlisted participant on
///   every cluster must vote <see cref="SagaVote.Commit"/> for the saga to
///   commit. A single <see cref="SagaVote.Abort"/> vote (from any participant on
///   any cluster) aborts the whole saga.</description></item>
///   <item><description><b>Single global decision moment.</b> The coordinator
///   reaches exactly one commit-or-abort decision after collecting every vote,
///   and that one decision is delivered to every prepared participant. A
///   participant never observes a mixed outcome.</description></item>
///   <item><description><b>Compensation on abort.</b> When the saga aborts, every
///   participant that voted to commit is compensated: <see cref="AbortAsync"/>
///   rolls its prepared resource set back to the pre-prepare view. Compensation
///   must be <b>total</b> - a participant that votes to commit must always be
///   able to undo that prepare (this matches the intra-cluster cross-tree saga
///   contract).</description></item>
///   <item><description><b>Bounded fence-timer auto-compensation.</b> A prepared
///   participant holds a bounded cutover fence while it waits for the decision.
///   If the coordinator never returns before the fence expires, the participant
///   model auto-compensates by calling <see cref="AbortAsync"/>, so a prepared
///   mutation can never leak after a coordinator loss.</description></item>
///   <item><description><b>Idempotent re-attach.</b> Every method must be
///   idempotent: a duplicate <see cref="PrepareAsync"/>, <see cref="CommitAsync"/>,
///   or <see cref="AbortAsync"/> (from a retry after a mid-flight restart, or a
///   re-attach after reactivation) must be safe and must not double-apply or
///   double-compensate.</description></item>
/// </list>
/// </para>
/// <para>
/// The SPI is called in-process on the participant cluster, so its arguments
/// and results are plain CLR types and are never serialized over a grain
/// boundary.
/// </para>
/// </summary>
public interface ISagaParticipant
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
    /// mutation, restoring the pre-prepare view. Compensation must be total: a
    /// participant that voted to commit must always be able to undo that prepare.
    /// Idempotent: a duplicate abort, or an abort of a resource set that was
    /// never prepared, is a no-op.
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
