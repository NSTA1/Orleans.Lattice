namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side seam that decides what flow-control hints should be
/// stamped onto the <see cref="ReplicationAck"/> returned for a given
/// inbound push. Implementations inspect the supplied
/// <see cref="ReceiverFlowControlContext"/> (per-tree apply state, the
/// origin cluster id, the just-applied entry count, the wall-clock
/// apply duration, and so on) and return a
/// <see cref="ReceiverFlowControlHint"/> describing the requested
/// <see cref="ReplicationAck.SuggestedBatchSize"/> and
/// <see cref="ReplicationAck.PauseForMs"/> values.
/// <para>
/// The default registration is <see cref="NoOpReceiverFlowControlPolicy"/>,
/// which always returns <see cref="ReceiverFlowControlHint.None"/> -
/// the canonical "no preference" signal that preserves today's blind-
/// push behaviour for hosts that have not opted in. Production hosts
/// replace the registration via DI to surface back-pressure (e.g. queue
/// depth on a downstream materialiser, CPU saturation under load) as
/// concrete <see cref="ReplicationAck.SuggestedBatchSize"/> /
/// <see cref="ReplicationAck.PauseForMs"/> hints.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation across
/// distinct <c>(treeName, originClusterId)</c> pairs. The receiver-
/// side gRPC service invokes the policy on every successful push
/// without serialisation, so heavy per-call work belongs behind a
/// cached / observed state surface rather than inside the policy's
/// hot path.
/// </para>
/// </summary>
public interface IReceiverFlowControlPolicy
{
    /// <summary>
    /// Returns the flow-control hint the receiver wishes to stamp onto
    /// the ack for the just-applied batch described by
    /// <paramref name="context"/>. Failures throw - the caller logs
    /// and degrades the ack to <see cref="ReceiverFlowControlHint.None"/>
    /// rather than failing the apply.
    /// </summary>
    /// <param name="context">Per-batch evaluation context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<ReceiverFlowControlHint> EvaluateAsync(
        ReceiverFlowControlContext context,
        CancellationToken cancellationToken);
}
