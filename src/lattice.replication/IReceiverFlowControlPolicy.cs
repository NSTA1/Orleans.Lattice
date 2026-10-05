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
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>
/// installs <see cref="WalSaturationReceiverFlowControlPolicy"/> by default.
/// Hosts that want the old blind-push behaviour pre-register
/// <see cref="NoOpReceiverFlowControlPolicy"/>, and gRPC-only compositions
/// that do not call <c>AddLatticeReplication</c> receive the no-op fallback.
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
