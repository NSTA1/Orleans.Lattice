namespace Orleans.Lattice.Replication;

/// <summary>
/// Call-site argument supplied to
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>.
/// Carries the routing metadata an outbound shipper attaches at dispatch
/// time alongside the opaque, transport-agnostic payload bytes.
/// <para>
/// The struct is intentionally not Orleans-serialisable: it is the
/// in-process call argument, not the on-the-wire envelope. Wire-format
/// hardening - versioned envelopes, content framing, compression -
/// happens inside <see cref="Payload"/> and is the concern of the
/// binary-framing seam, not the call-shape seam.
/// </para>
/// </summary>
public readonly record struct ReplicationBatch
{
    /// <summary>
    /// Stable identifier of the destination cluster. Implementations of
    /// <see cref="IReplicationTransport"/> route the call by this value.
    /// Required: must be non-<see langword="null"/> and non-empty.
    /// </summary>
    public string TargetClusterId { get; init; }

    /// <summary>
    /// Name of the local tree this batch was drawn from. Receivers that
    /// dispatch per-tree apply pipelines route on this id; the per-origin
    /// high-water-mark dedup key is <c>(TreeName, OriginClusterId)</c>.
    /// Required: must be non-<see langword="null"/> and non-empty.
    /// </summary>
    public string TreeName { get; init; }

    /// <summary>
    /// Stable identifier of the local (sending) cluster. Stamped on every
    /// captured <see cref="ReplogEntry"/> at commit time and surfaced here
    /// so transports that frame the entries themselves do not need to
    /// re-derive the origin from the payload. Required: must be
    /// non-<see langword="null"/> and non-empty.
    /// </summary>
    public string OriginClusterId { get; init; }

    /// <summary>
    /// Opaque, framed batch payload. The byte layout is the responsibility
    /// of the binary-framing seam (typically Orleans-serializer-encoded
    /// <see cref="ReplogEntry"/> records inside a versioned envelope), and
    /// the transport treats this as a black box: it does not parse, peek
    /// into, or otherwise interpret the bytes. May be empty (a heartbeat
    /// or keep-alive batch).
    /// </summary>
    public ReadOnlyMemory<byte> Payload { get; init; }
}
