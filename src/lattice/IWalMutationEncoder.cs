using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// Single-pass codec for the WAL's per-entry payload bytes. Captures
/// both the encode side - producing the bytes a
/// <see cref="LatticeMutation"/> serialises to under the WAL's chosen
/// wire format, emitted into a caller-supplied
/// <see cref="IBufferWriter{T}"/> so the result can be measured
/// against <see cref="LatticeOptions.WalMaxBatchBytes"/> and handed
/// verbatim to
/// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/> without a
/// second encode - and the decode side, used by the in-memory
/// provider on read and by the default
/// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/> fallback
/// to round-trip pre-encoded payloads back to
/// <see cref="LatticeMutation"/> values for third-party providers
/// that did not override the zero-copy overload.
/// <para>
/// Replaces the historical per-entry size heuristic
/// (<c>key.Length * 2 + value.Length + 128</c>). Producing the bytes
/// once at append time and handing the same bytes to the storage
/// provider closes the "WAL append re-encodes what the grain already
/// encoded" gap in the commit hot path; backends that natively store
/// binary payloads (Azure Table Storage, file-backed providers)
/// accept the segments straight through to the persistence row, and
/// the WAL grain accumulates the per-batch byte budget directly from
/// the bytes it just wrote rather than from a heuristic that lost
/// fidelity around vector-clock cardinality.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from
/// multiple threads. The default
/// <see cref="OrleansBinaryWalMutationEncoder"/> wraps the
/// thread-safe <c>Serializer&lt;LatticeMutation&gt;</c> from
/// <c>Orleans.Serialization</c>; replacement implementations are
/// expected to be similarly cheap (no per-call heap allocation
/// beyond the bytes claimed via
/// <see cref="IBufferWriter{T}.GetSpan"/> /
/// <see cref="IBufferWriter{T}.GetMemory"/>, plus whatever the
/// decode path requires).
/// </para>
/// </summary>
public interface IWalMutationEncoder
{
    /// <summary>
    /// Encodes <paramref name="mutation"/> into
    /// <paramref name="writer"/> under the WAL's canonical wire
    /// format. Callers compute the number of bytes written by
    /// snapshotting the writer's running count before and after the
    /// call (or by inspecting <c>WrittenSpan.Length</c> on an
    /// <see cref="ArrayBufferWriter{T}"/>); the encoded bytes can
    /// then participate in the per-batch byte budget
    /// (<see cref="LatticeOptions.WalMaxBatchBytes"/>) and be passed
    /// directly to
    /// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>.
    /// </summary>
    /// <param name="mutation">The mutation to encode. Captured state is read-only; the encoder must not mutate it.</param>
    /// <param name="writer">Destination buffer writer. The encoder calls <see cref="IBufferWriter{T}.GetSpan"/> / <see cref="IBufferWriter{T}.Advance"/> exactly as <c>Orleans.Serialization</c> does. Must not be <see langword="null"/>.</param>
    void Encode(in LatticeMutation mutation, IBufferWriter<byte> writer);

    /// <summary>
    /// Decodes a previously-<see cref="Encode"/>d payload back to a
    /// <see cref="LatticeMutation"/>. The supplied segment is the
    /// exact byte sequence the encoder wrote on the producer side
    /// (the in-memory provider stores it verbatim; the Azure Table
    /// provider reads it back from the row's <c>Payload</c> column).
    /// </summary>
    /// <param name="encoded">The encoded payload bytes. Borrowed for the duration of the call; the implementation must not retain a reference to the underlying array.</param>
    LatticeMutation Decode(ReadOnlySpan<byte> encoded);
}

