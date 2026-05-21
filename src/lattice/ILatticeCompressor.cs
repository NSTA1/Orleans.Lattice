using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// Pluggable seam for compressing and decompressing arbitrary byte
/// payloads inside the Orleans.Lattice stack. The seam was first
/// introduced for the replication batch's framing tail
/// (see the <c>Orleans.Lattice.Replication</c> wire-format docs) but
/// is intentionally byte-shaped so other layers - WAL segment
/// compression, snapshot compression, cold-storage tiers - can reuse
/// the same DI registration without re-deriving the contract.
/// <para>
/// Implementations are matched by <see cref="Algorithm"/> at the call
/// site: replication looks the compressor up by the
/// <see cref="LatticeCompression"/> value carried in the framing
/// header; future layers will dispatch by their own per-segment
/// header byte using the same enum. Hosts register one
/// <see cref="ILatticeCompressor"/> singleton per algorithm via DI;
/// duplicate registrations for the same <see cref="Algorithm"/>
/// value are rejected at construction time by the consuming layer.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from
/// multiple threads and must not retain references to caller-owned
/// buffers past the synchronous return of the called method. Both
/// <see cref="Compress"/> and <see cref="Decompress"/> are expected
/// to be allocation-free on the steady-state hot path; callers size
/// their pooled buffers via <see cref="GetMaxCompressedLength"/> so
/// the compressor never has to grow them.
/// </para>
/// </summary>
public interface ILatticeCompressor
{
    /// <summary>
    /// The <see cref="LatticeCompression"/> value this compressor
    /// handles. Consumers dispatch to a registered compressor by
    /// matching this value against the producer-side option (encode)
    /// or the wire-side header byte (decode); only one compressor per
    /// algorithm value may be registered in DI.
    /// </summary>
    LatticeCompression Algorithm { get; }

    /// <summary>
    /// Returns the worst-case compressed byte count for an input of
    /// <paramref name="uncompressedLength"/> bytes. Consumers use
    /// this to rent a single right-sized buffer from
    /// <see cref="ArrayPool{T}.Shared"/> before invoking
    /// <see cref="Compress"/>, so the steady-state encode hot path
    /// performs no managed allocation. Implementations must return a
    /// value strictly greater than or equal to the maximum number of
    /// bytes <see cref="Compress"/> will write for any input of that
    /// length.
    /// </summary>
    int GetMaxCompressedLength(int uncompressedLength);

    /// <summary>
    /// Compresses the supplied <paramref name="source"/> bytes into
    /// <paramref name="destination"/>. The caller sizes
    /// <paramref name="destination"/> to at least
    /// <see cref="GetMaxCompressedLength"/> bytes; the implementation
    /// writes the compressed bytes into the span and returns the
    /// number of bytes written. The destination span is caller-owned
    /// and must not be retained beyond the synchronous return of
    /// this call.
    /// </summary>
    /// <param name="source">The uncompressed input bytes.</param>
    /// <param name="destination">
    /// A span sized to at least <see cref="GetMaxCompressedLength"/>
    /// bytes for the supplied input.
    /// </param>
    /// <returns>The number of bytes written into <paramref name="destination"/>.</returns>
    int Compress(ReadOnlySpan<byte> source, Span<byte> destination);

    /// <summary>
    /// Decompresses the supplied <paramref name="source"/> bytes into
    /// <paramref name="destination"/>. Callers pre-size
    /// <paramref name="destination"/> to
    /// <paramref name="uncompressedLength"/> bytes and pass the
    /// expected uncompressed length verbatim from the wire format's
    /// length prefix; the implementation must fill the entire span
    /// and validate the recovered length against
    /// <paramref name="uncompressedLength"/>.
    /// </summary>
    /// <param name="source">The compressed bytes.</param>
    /// <param name="destination">
    /// A span sized to exactly <paramref name="uncompressedLength"/>
    /// bytes. The implementation must fill the entire span.
    /// </param>
    /// <param name="uncompressedLength">
    /// The expected uncompressed byte count, taken from the surrounding
    /// wire format's length prefix. Implementations validate that the
    /// decompressed output matches this value and throw on mismatch.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when the decompressed output does not match
    /// <paramref name="uncompressedLength"/> or when
    /// <paramref name="source"/> is malformed for this algorithm.
    /// </exception>
    void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength);
}
