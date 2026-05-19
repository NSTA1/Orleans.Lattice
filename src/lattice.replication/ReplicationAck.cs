using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side acknowledgement returned from
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>.
/// Carries the per-origin high-water-mark the receiver advanced to as a
/// result of applying the batch, so the sender can advance its own
/// per-peer cursor strictly to that point on success - the canonical
/// "advance-cursor-on-ack" semantic the design doc requires.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationAck)]
[Immutable]
public readonly record struct ReplicationAck
{
    /// <summary>
    /// <see langword="true"/> when the receiver successfully received and
    /// processed the batch. <see langword="false"/> when the receiver
    /// rejected the batch outright (transport-level error, schema
    /// mismatch, unknown tree, etc.) and the sender should not advance
    /// its cursor past the batch's start.
    /// <para>
    /// Note that <see cref="Accepted"/> is <see langword="true"/> even
    /// when every entry in the batch was de-duplicated by the per-origin
    /// high-water-mark - dedup is a successful idempotent apply, not a
    /// rejection. In that case <see cref="HighestAppliedHlc"/> reflects
    /// the receiver's existing HWM and the sender's cursor still
    /// advances.
    /// </para>
    /// </summary>
    [Id(0)] public bool Accepted { get; init; }

    /// <summary>
    /// The per-origin high-water-mark for
    /// <c>(ReplicationBatch.TreeName, ReplicationBatch.OriginClusterId)</c>
    /// after the receiver finished processing the batch. The sender
    /// advances its per-peer cursor strictly to this value on success;
    /// on a partial apply (some entries applied, some failed) the
    /// receiver still returns the highest HLC it actually advanced its
    /// HWM to, and the sender resumes from there.
    /// <para>
    /// When <see cref="Accepted"/> is <see langword="false"/> this value
    /// is undefined and the sender must not consume it.
    /// </para>
    /// </summary>
    [Id(1)] public HybridLogicalClock HighestAppliedHlc { get; init; }

    /// <summary>
    /// Receiver-side blocked-floor pin for the tree at the moment
    /// the ack was constructed: the lowest HLC across every partially-
    /// staged atomic batch the receiver is currently buffering, or
    /// <see langword="null"/> when the receiver has no in-flight
    /// atomic-batch admissions for this tree (or the receiver is
    /// pre-Phase-9 and never stamped the slot at all).
    /// <para>
    /// On a positive ack the sender publishes this value to its local
    /// <see cref="IWalCursorRegistry"/> under a
    /// peer-specific consumer id so the producer-side WAL GC AND-s
    /// the strict-less <c>entry.Timestamp &lt; blockedFloor</c>
    /// clause into its trim predicate. This is the cross-cluster
    /// propagation channel for the receiver's TX-aware GC pin: with
    /// it, a buffered atomic batch on cluster B prevents cluster A
    /// from trimming the corresponding WAL entries even when wall-
    /// clock TTL would otherwise allow it. When the field is
    /// <see langword="null"/> the producer-side registry pin is
    /// cleared (or never registered), and the sender's GC degrades
    /// cleanly to the cursor + TTL + causal-stable predicate.
    /// </para>
    /// <para>
    /// Strictly additive on the wire: pre-Phase-9 receivers omit the
    /// slot entirely (decodes as <see langword="null"/>); pre-Phase-9
    /// senders ignore the field. The slot is therefore safe to roll
    /// out independently on either side of a peering.
    /// </para>
    /// </summary>
    [Id(2)] public HybridLogicalClock? BlockedAtHlc { get; init; }

    /// <summary>
    /// Receiver-side flow-control hint: the largest batch the receiver
    /// would like the sender to ship on the next pump tick, expressed
    /// as a strictly-positive entry count. The sender clamps the value
    /// to the closed interval
    /// <c>[1, LatticeReplicationOptions.ShipBatchSize]</c> and uses it
    /// as the per-tick batch cap until the receiver lifts or revises
    /// the hint. A value of <see langword="null"/> means "no preference";
    /// the sender resumes shipping at its configured
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/> on the
    /// next tick, which is the canonical re-acceleration signal once
    /// the receiver has recovered from a transient load spike.
    /// <para>
    /// Strictly additive on the wire: receivers built before the
    /// receiver-side flow-control wave omit the slot entirely
    /// (decodes as <see langword="null"/>); senders built before that
    /// wave ignore the field. The slot is therefore safe to roll out
    /// independently on either side of a peering.
    /// </para>
    /// <para>
    /// The receiver-side source of the hint is the pluggable
    /// <c>IReceiverFlowControlPolicy</c> seam. The default
    /// implementation is a no-op that always returns <c>null</c>
    /// hints, preserving today's blind-push behaviour for hosts that
    /// have not opted in.
    /// </para>
    /// </summary>
    [Id(3)] public int? SuggestedBatchSize { get; init; }

    /// <summary>
    /// Receiver-side flow-control hint: number of milliseconds the
    /// sender should pause before its next pump tick to this peer.
    /// Composes with the shipper's existing exponential-backoff retry
    /// budget by advancing the per-peer retry deadline to
    /// <c>max(currentBackoffDeadline, now + PauseForMs)</c>, so a
    /// receiver-requested pause never shortens an in-progress backoff.
    /// A value of <see langword="null"/> or <c>&lt;= 0</c> means
    /// "no pause requested"; the sender's next tick fires on its
    /// normal cadence.
    /// <para>
    /// Strictly additive on the wire (same compat profile as
    /// <see cref="SuggestedBatchSize"/>). The slot is the canonical
    /// way for a struggling receiver to throttle a sender without
    /// timing out the RPC.
    /// </para>
    /// </summary>
    [Id(4)] public int? PauseForMs { get; init; }
}
