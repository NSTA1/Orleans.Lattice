using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Pluggable seam for encoding and decoding the on-the-wire payload
/// supplied to <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>.
/// Implementations frame a batch of <see cref="WalRecord"/> records
/// inside a versioned <see cref="ReplicationBatchEnvelope"/>; the
/// transport treats the resulting bytes as opaque and never inspects
/// them.
/// <para>
/// The default DI registration is
/// <see cref="OrleansBinaryReplicationBatchEncoder"/>, which uses the
/// Orleans serializer as the canonical wire format. Hosts that need a
/// different framing - JSON for debuggability over an HTTP transport,
/// a custom envelope for compatibility with an external pipeline,
/// content-hash-prefixed framing for deduplication - replace the
/// registration via standard DI:
/// </para>
/// <code>
/// services.AddSingleton&lt;IReplicationBatchEncoder, MyEncoder&gt;();
/// </code>
/// <para>
/// Implementations are expected to be safe for concurrent invocation
/// from multiple threads; the canonical Orleans-serializer-backed
/// implementation is, because the underlying <c>Serializer&lt;T&gt;</c>
/// is thread-safe by contract.
/// </para>
/// <para>
/// <b>Allocation contract.</b> The encode path is deliberately
/// expressed in terms of <see cref="IBufferWriter{T}"/> rather than a
/// freshly-allocated <c>byte[]</c>: the canonical streaming push
/// transport hands the gRPC stream's writer in directly so the
/// envelope's bytes never round-trip through a per-batch heap
/// allocation. Callers that need a materialised buffer (tests,
/// debug-tooling, in-process loopback transports) supply an
/// <see cref="ArrayBufferWriter{T}"/> and read
/// <see cref="ArrayBufferWriter{T}.WrittenMemory"/>; the writer's
/// lifetime is the caller's responsibility, which matches the
/// ownership model
/// <see cref="ReplicationBatch.Payload"/> already imposes on the bytes
/// it carries.
/// </para>
/// </summary>
public interface IReplicationBatchEncoder
{
    /// <summary>
    /// Stable identifier for the wire format this encoder produces,
    /// suitable for use as an HTTP <c>Content-Type</c> header or a gRPC
    /// metadata tag. Receivers may use this value to dispatch among
    /// multiple registered encoders (e.g. binary by default, JSON when
    /// a debugging flag is set).
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// The wire-format version this encoder authors when calling
    /// <see cref="Encode"/>. Stamped on every produced
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/> when the caller
    /// left it at the default <c>0</c>; compared strictly against
    /// incoming values during <see cref="Decode"/> (greater than is
    /// rejected, less-than-or-equal is accepted).
    /// </summary>
    int CurrentWireVersion { get; }

    /// <summary>
    /// Encodes the supplied <paramref name="envelope"/> into
    /// <paramref name="writer"/>. Implementations stamp
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/> with their
    /// own <see cref="CurrentWireVersion"/> if the caller left it at
    /// the default <c>0</c>, but must not silently downgrade or
    /// upgrade an explicitly-supplied non-zero version.
    /// <para>
    /// The encoder appends bytes to <paramref name="writer"/> via the
    /// standard <see cref="IBufferWriter{T}"/> contract
    /// (<c>GetSpan</c> / <c>Advance</c>); it does not reset, rewind,
    /// or otherwise mutate any bytes already written by an earlier
    /// call. Callers that expect a single-batch buffer must supply a
    /// fresh writer per call.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="writer"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the envelope is missing required routing metadata
    /// (<see cref="ReplicationBatchEnvelope.TreeName"/> or
    /// <see cref="ReplicationBatchEnvelope.OriginClusterId"/> null or
    /// empty) or carries a negative
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/>.
    /// </exception>
    void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer);

    /// <summary>
    /// Decodes the supplied <paramref name="payload"/> back into a
    /// <see cref="ReplicationBatchEnvelope"/>.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="payload"/> is empty or malformed
    /// (the underlying serializer's exception is wrapped or surfaced as
    /// the implementation sees fit).
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Thrown when the decoded payload's
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/> is strictly
    /// greater than <see cref="CurrentWireVersion"/>; the receiver
    /// fails fast rather than guess at the layout.
    /// </exception>
    ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload);
}
