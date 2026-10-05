namespace Orleans.Lattice.Replication;

/// <summary>
/// Server-side delegation seam for the cross-cluster saga control
/// channel. The <c>orleans.lattice.replication.LatticeSaga</c> gRPC
/// service validates the inbound request, enforces peer authorization,
/// and then delegates each imperative RPC to this handler - exactly as
/// the snapshot gRPC service delegates to
/// <see cref="LatticeRemoteSnapshotService"/>.
/// <para>
/// This package ships a safe default,
/// <see cref="NoParticipantSagaControlHandler"/>, that reports no
/// participant record and votes to abort. The durable
/// coordinator/participant model replaces the default registration with
/// a real participant implementation; because the default is registered
/// with <c>TryAddSingleton</c>, that replacement wins without ceremony.
/// </para>
/// </summary>
public interface ILatticeSagaControlHandler
{
    /// <summary>
    /// Handles a <c>Prepare</c> RPC. The participant durably records the
    /// prepared state for the saga and returns its
    /// <see cref="SagaVote"/>.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's prepare response.</returns>
    Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Handles a <c>Commit</c> RPC. The participant applies the prepared
    /// mutation and releases the prepared state.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's commit response.</returns>
    Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Handles an <c>Abort</c> RPC. The participant releases any
    /// prepared state without applying the mutation.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's abort response.</returns>
    Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Handles a <c>GetStatus</c> RPC. The participant reports the
    /// durable phase it currently holds for the saga without changing
    /// any state.
    /// </summary>
    /// <param name="request">The validated control request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The participant's status response.</returns>
    Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Answers a participant's <c>GetDecision</c> query on the saga's
    /// coordinator cluster (issue #4637): the durable decision of the saga
    /// <see cref="SagaControlRequest.SagaId"/>, as
    /// <see cref="SagaPhase.Committed"/>, <see cref="SagaPhase.Aborted"/>, or
    /// <see cref="SagaPhase.Prepared"/> while it is still pending.
    /// <see cref="SagaControlRequest.RequesterClusterId"/> carries the
    /// authenticated origin the transport stamped; a requester that is not one
    /// of the saga's participants is refused. The default implementation throws
    /// <see cref="NotSupportedException"/>.
    /// </summary>
    /// <param name="request">The control request; its requester is set by the transport.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The decision, carried in <see cref="SagaControlResponse.Phase"/>.</returns>
    Task<SagaControlResponse> GetDecisionAsync(SagaControlRequest request, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            $"This {nameof(ILatticeSagaControlHandler)} implementation hosts no saga coordinator.");
}
