namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side flow-control hint produced by
/// <see cref="IReceiverFlowControlPolicy.EvaluateAsync"/> and stamped
/// onto the <see cref="ReplicationAck"/> returned to the sender. Both
/// fields are nullable: a <see langword="null"/> value means "no
/// preference" and the sender resumes shipping at its configured
/// <see cref="LatticeReplicationOptions.ShipBatchSize"/> / normal
/// cadence, which is the canonical re-acceleration signal once the
/// receiver has recovered.
/// <para>
/// The hint is process-local: it never crosses the wire on its own.
/// The receiver-side gRPC service projects the
/// <see cref="SuggestedBatchSize"/> and <see cref="PauseForMs"/>
/// fields onto <see cref="ReplicationAck.SuggestedBatchSize"/> and
/// <see cref="ReplicationAck.PauseForMs"/> verbatim, and the
/// sender-side <c>ReplicationShipperGrain</c> consumes those slots.
/// </para>
/// </summary>
public readonly record struct ReceiverFlowControlHint
{
    /// <summary>
    /// Suggested per-tick batch cap to send to the receiver, in
    /// entries. The sender clamps the value to the closed interval
    /// <c>[1, LatticeReplicationOptions.ShipBatchSize]</c>; values
    /// less than or equal to zero are treated as "no preference".
    /// <see langword="null"/> restores the sender's configured
    /// batch size on the next pump tick.
    /// </summary>
    public int? SuggestedBatchSize { get; init; }

    /// <summary>
    /// Number of milliseconds the sender should pause before its
    /// next pump tick. Composes with the shipper's existing
    /// exponential-backoff retry budget by advancing the per-peer
    /// retry deadline to <c>max(currentBackoffDeadline, now + PauseForMs)</c>
    /// - a receiver-requested pause never shortens an in-progress
    /// backoff. <see langword="null"/> or a value less than or equal
    /// to zero means "no pause requested".
    /// </summary>
    public int? PauseForMs { get; init; }

    /// <summary>
    /// Canonical "no preference" instance. Returned by
    /// <see cref="NoOpReceiverFlowControlPolicy"/> and the default
    /// shape recommended for transient / no-back-pressure paths.
    /// </summary>
    public static ReceiverFlowControlHint None => default;
}
