namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-batch evaluation context passed to
/// <see cref="IReceiverFlowControlPolicy.EvaluateAsync"/>. Describes
/// the inbound push the receiver just applied so the policy can shape
/// its hint based on the actual batch the receiver finished
/// processing (rather than guessing from out-of-band signals).
/// <para>
/// The context is process-local only - it never crosses the wire and
/// has no serialisation alias. Implementations may capture and stash
/// fields freely; the lifetime ends at the end of the
/// <see cref="IReceiverFlowControlPolicy.EvaluateAsync"/> call.
/// </para>
/// </summary>
public readonly record struct ReceiverFlowControlContext
{
    /// <summary>Logical tree id the entries were applied to.</summary>
    public string TreeName { get; init; }

    /// <summary>
    /// Authoring cluster id of the just-applied entries. Mirrors
    /// <c>ReplicationBatchEnvelope.OriginClusterId</c>.
    /// </summary>
    public string OriginClusterId { get; init; }

    /// <summary>
    /// Number of entries the receiver received in this push. Includes
    /// every entry handed to <c>IReplicationApplier.ApplyBatchAsync</c>
    /// regardless of whether the apply path deduped, parked, or
    /// committed them - the policy decides how to weight each outcome
    /// against its back-pressure model.
    /// </summary>
    public int EntryCount { get; init; }

    /// <summary>
    /// Wall-clock duration of the just-applied
    /// <c>IReplicationApplier.ApplyBatchAsync</c> call, in milliseconds.
    /// A value of <c>0</c> indicates the receiver did not measure the
    /// duration for this push (e.g. an empty heartbeat batch).
    /// </summary>
    public double ApplyDurationMs { get; init; }
}
