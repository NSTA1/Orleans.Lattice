using Orleans.Lattice.BPlusTree.Grains;
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
    /// captured <see cref="WalRecord"/> at commit time and surfaced here
    /// so transports that frame the entries themselves do not need to
    /// re-derive the origin from the payload. Required: must be
    /// non-<see langword="null"/> and non-empty.
    /// </summary>
    public string OriginClusterId { get; init; }

    /// <summary>
    /// Opaque, framed batch payload. The byte layout is the responsibility
    /// of the binary-framing seam (typically Orleans-serializer-encoded
    /// <see cref="WalRecord"/> records inside a versioned envelope), and
    /// the transport treats this as a black box: it does not parse, peek
    /// into, or otherwise interpret the bytes. May be empty (a heartbeat
    /// or keep-alive batch).
    /// <para>
    /// Transports that re-encode the bytes onto their own wire frame
    /// (the canonical gRPC streaming push transport, for example) prefer
    /// <see cref="Envelope"/> when it is non-<see langword="null"/> and
    /// fall back to decoding <see cref="Payload"/> only when the call
    /// site predates the typed-envelope slot. The shipper populates
    /// both slots on every send so transports are free to pick.
    /// </para>
    /// </summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>
    /// Pre-built typed envelope corresponding to <see cref="Payload"/>.
    /// Optional and additive: when non-<see langword="null"/>, transports
    /// that frame the envelope onto their own wire (e.g. the gRPC
    /// streaming push transport, which marshals
    /// <see cref="ReplicationBatchEnvelope"/> directly into the gRPC
    /// stream's <see cref="System.Buffers.IBufferWriter{T}"/>) skip the
    /// per-send decode-then-re-encode round-trip the opaque-bytes seam
    /// would otherwise force. When <see langword="null"/>, transports
    /// must decode <see cref="Payload"/> through
    /// <see cref="IReplicationBatchEncoder.Decode(ReadOnlyMemory{byte})"/>
    /// to recover the envelope.
    /// <para>
    /// Transports that ship the opaque bytes verbatim (HTTP body,
    /// disk-cached batch, custom binary framing) ignore this slot and
    /// continue to consume <see cref="Payload"/>; transports that
    /// surface the typed object to their hot path
    /// (<c>GrpcPushTransport.BuildEnvelope</c>, in-process loopback
    /// transports under test) consult it first and avoid the
    /// allocation entirely.
    /// </para>
    /// <para>
    /// The slot is a <see cref="Nullable{T}"/> reference into the
    /// shipper's activation-scoped <c>List&lt;WalRecord&gt;</c> drain
    /// buffer, not a defensive copy. The Orleans single-threaded grain
    /// turn model makes the reference safe for synchronous consumption
    /// inside the <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>
    /// call; transports that need to retain the entry list past the
    /// returned <see cref="Task"/>'s completion must copy it.
    /// </para>
    /// </summary>
    public ReplicationBatchEnvelope? Envelope { get; init; }

    /// <summary>
    /// Pre-built framing-only envelope corresponding to
    /// <see cref="Payload"/>. Optional and additive: when
    /// non-<see langword="null"/>, transports that frame the bytes
    /// onto their own wire (the gRPC streaming push transport, for
    /// example) skip both the typed-envelope decode (<see cref="Envelope"/>)
    /// and the legacy bytes decode (<see cref="Payload"/>) and pull
    /// the pre-encoded entry segments straight from
    /// <see cref="ReplicationBatchEncodedEnvelope.EncodedEntries"/>
    /// into the outbound stream's
    /// <see cref="System.Buffers.IBufferWriter{T}"/>. This is the
    /// one-encode fast path the framing seam exists to feed.
    /// <para>
    /// When <see langword="null"/>, transports fall back to
    /// <see cref="Envelope"/> when it is present, then to decoding
    /// <see cref="Payload"/> through
    /// <see cref="IReplicationBatchEncoder.Decode(ReadOnlyMemory{byte})"/>.
    /// Transports that do not understand the framing seam simply
    /// ignore this slot.
    /// </para>
    /// <para>
    /// The slot's <see cref="ReplicationBatchEncodedEnvelope.EncodedEntries"/>
    /// memory is borrowed from the shipper's activation-scoped read
    /// page; the Orleans single-threaded grain turn model makes the
    /// reference safe for synchronous consumption inside the
    /// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>
    /// call.
    /// </para>
    /// </summary>
    public ReplicationBatchEncodedEnvelope? EncodedEnvelope { get; init; }
}
