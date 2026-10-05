namespace Orleans.Lattice.Replication;

/// <summary>
/// Client-side transport seam for the cross-cluster saga control
/// channel. A coordinator resolves this transport through DI to drive
/// the imperative saga RPCs against a participant cluster's
/// <c>orleans.lattice.replication.LatticeSaga</c> service over the
/// shared per-peer HTTP/2 channel. The gRPC binding
/// (<c>Orleans.Lattice.Replication.Grpc</c>) supplies the canonical
/// implementation; the seam keeps the coordinator independent of the
/// concrete transport.
/// </summary>
public interface ISagaControlChannel
{
    /// <summary>
    /// Invokes the <c>Prepare</c> RPC on the participant cluster
    /// identified by <paramref name="clusterId"/>.
    /// </summary>
    /// <param name="clusterId">Target participant cluster id.</param>
    /// <param name="request">The control request to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's prepare response.</returns>
    Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Invokes the <c>Commit</c> RPC on the participant cluster
    /// identified by <paramref name="clusterId"/>.
    /// </summary>
    /// <param name="clusterId">Target participant cluster id.</param>
    /// <param name="request">The control request to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's commit response.</returns>
    Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Invokes the <c>Abort</c> RPC on the participant cluster
    /// identified by <paramref name="clusterId"/>.
    /// </summary>
    /// <param name="clusterId">Target participant cluster id.</param>
    /// <param name="request">The control request to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's abort response.</returns>
    Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Invokes the <c>GetStatus</c> RPC on the participant cluster
    /// identified by <paramref name="clusterId"/>.
    /// </summary>
    /// <param name="clusterId">Target participant cluster id.</param>
    /// <param name="request">The control request to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's status response.</returns>
    Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Invokes the <c>GetDecision</c> RPC on the saga's <b>coordinator</b>
    /// cluster identified by <paramref name="coordinatorClusterId"/>: a prepared
    /// participant whose cutover-fence timer expired asks for the saga's durable
    /// decision instead of compensating on its own (issue #4637). The response's
    /// <see cref="SagaControlResponse.Phase"/> is <see cref="SagaPhase.Committed"/>
    /// or <see cref="SagaPhase.Aborted"/> once the coordinator has decided, and
    /// <see cref="SagaPhase.Prepared"/> while the decision is still pending.
    /// <para>
    /// The default implementation throws <see cref="NotSupportedException"/>, so a
    /// transport that predates the verb is treated by the participant as an
    /// unreachable coordinator: it keeps its fence up rather than guess.
    /// </para>
    /// </summary>
    /// <param name="coordinatorClusterId">The saga's coordinator cluster id.</param>
    /// <param name="request">The control request identifying the saga.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The coordinator's decision, carried in <see cref="SagaControlResponse.Phase"/>.</returns>
    Task<SagaControlResponse> GetDecisionAsync(string coordinatorClusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            $"This {nameof(ISagaControlChannel)} implementation does not support querying a saga coordinator's decision.");
}
