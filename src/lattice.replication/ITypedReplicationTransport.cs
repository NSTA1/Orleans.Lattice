namespace Orleans.Lattice.Replication;

/// <summary>
/// Optional capability surface for <see cref="IReplicationTransport"/>
/// implementations that consume the pre-built
/// <see cref="ReplicationBatchEnvelope"/> directly and do not need the
/// outbound shipper to pre-encode the envelope into
/// <see cref="ReplicationBatch.Payload"/>.
/// <para>
/// The shipper probes the configured transport for this interface at
/// activation. When the transport implements it, the shipper skips the
/// per-tick <c>IReplicationBatchEncoder.Encode(envelope, _writeBuffer)</c>
/// call that previously populated <see cref="ReplicationBatch.Payload"/>
/// purely so legacy bytes-only transports could read it, and instead
/// invokes <see cref="SendTypedAsync(ReplicationBatch, CancellationToken)"/>
/// with <see cref="ReplicationBatch.Payload"/> left
/// <see cref="ReadOnlyMemory{T}.Empty"/>. The typed envelope is still
/// stamped onto <see cref="ReplicationBatch.Envelope"/> so the
/// transport reads it from there.
/// </para>
/// <para>
/// When the transport does <b>not</b> implement this interface, the
/// shipper continues to encode into the activation-scoped framing
/// buffer and routes the call through
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>.
/// The seam is therefore strictly additive: existing transports
/// (loopback in-process delivery, custom HTTP framing, the no-op
/// default) keep their bytes-shaped contract.
/// </para>
/// <para>
/// Transports that mix both consumption modes (for example, a transport
/// that prefers the typed envelope but falls back to the encoded bytes
/// for diagnostics) implement this interface and route
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>
/// to <see cref="SendTypedAsync(ReplicationBatch, CancellationToken)"/>;
/// the gRPC streaming push transport is the canonical example.
/// </para>
/// </summary>
public interface ITypedReplicationTransport : IReplicationTransport
{
    /// <summary>
    /// Sends a replication batch whose <see cref="ReplicationBatch.Envelope"/>
    /// is the authoritative payload. <see cref="ReplicationBatch.Payload"/>
    /// is <see cref="ReadOnlyMemory{T}.Empty"/> on this entry point and
    /// transports must consume the typed envelope directly.
    /// </summary>
    /// <param name="batch">
    /// Routing metadata plus the pre-built typed envelope. The
    /// <see cref="ReplicationBatch.Envelope"/> slot is guaranteed
    /// non-<see langword="null"/> for a non-heartbeat batch (i.e. when
    /// the shipper drained at least one entry); a heartbeat batch
    /// carries an empty-entries envelope rather than a
    /// <see langword="null"/> slot.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ReplicationAck> SendTypedAsync(ReplicationBatch batch, CancellationToken cancellationToken);
}
