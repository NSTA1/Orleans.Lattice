namespace Orleans.Lattice;

/// <summary>
/// Extends <see cref="ILatticeCompressor"/> with shared-dictionary
/// aware compress / decompress overloads. A compressor implementing this
/// interface can compress and inflate a payload against a dictionary
/// selected by a stable id (resolved through an
/// <see cref="ILatticeCompressionDictionaryProvider"/>), capturing
/// cross-payload redundancy a dictionary-less compressor cannot see.
/// <para>
/// The dictionary-less members inherited from
/// <see cref="ILatticeCompressor"/> remain valid and behave as the
/// dictionary id <c>0</c> ("no dictionary") path, so a dictionary-aware
/// compressor is a drop-in superset of the base contract.
/// </para>
/// </summary>
public interface ILatticeDictionaryCompressor : ILatticeCompressor
{
    /// <summary>
    /// Returns <see langword="true"/> when this compressor can resolve a
    /// dictionary for <paramref name="dictionaryId"/> (the id is
    /// registered with the backing
    /// <see cref="ILatticeCompressionDictionaryProvider"/>). The
    /// reserved id <c>0</c> ("no dictionary") always reports
    /// <see langword="false"/> - the dictionary-less inherited members
    /// cover that path.
    /// </summary>
    /// <param name="dictionaryId">The stable dictionary id to probe.</param>
    /// <returns>
    /// <see langword="true"/> when the dictionary is resolvable;
    /// otherwise <see langword="false"/>.
    /// </returns>
    bool HasDictionary(uint dictionaryId);

    /// <summary>
    /// Worst-case compressed-length bound for an input of
    /// <paramref name="uncompressedLength"/> bytes compressed against the
    /// dictionary identified by <paramref name="dictionaryId"/>. Callers
    /// size their pooled destination buffer with this before calling the
    /// dictionary-aware
    /// <see cref="Compress(ReadOnlySpan{byte}, Span{byte}, uint)"/>.
    /// </summary>
    /// <param name="uncompressedLength">The uncompressed input length.</param>
    /// <param name="dictionaryId">The dictionary id that will be used.</param>
    /// <returns>The worst-case compressed length.</returns>
    int GetMaxCompressedLength(int uncompressedLength, uint dictionaryId);

    /// <summary>
    /// Compresses <paramref name="source"/> into
    /// <paramref name="destination"/> using the dictionary identified by
    /// <paramref name="dictionaryId"/>, returning the number of bytes
    /// written.
    /// </summary>
    /// <param name="source">The bytes to compress.</param>
    /// <param name="destination">
    /// The destination buffer, sized at least
    /// <see cref="GetMaxCompressedLength(int, uint)"/>.
    /// </param>
    /// <param name="dictionaryId">The dictionary id to compress against.</param>
    /// <returns>The number of compressed bytes written.</returns>
    int Compress(ReadOnlySpan<byte> source, Span<byte> destination, uint dictionaryId);

    /// <summary>
    /// Decompresses <paramref name="source"/> into
    /// <paramref name="destination"/> using the dictionary identified by
    /// <paramref name="dictionaryId"/>. The implementation validates that
    /// the recovered length equals <paramref name="uncompressedLength"/>
    /// and throws on mismatch.
    /// </summary>
    /// <param name="source">The compressed bytes.</param>
    /// <param name="destination">
    /// The destination buffer, sized exactly
    /// <paramref name="uncompressedLength"/>.
    /// </param>
    /// <param name="uncompressedLength">
    /// The expected uncompressed length, taken verbatim from the wire
    /// length prefix.
    /// </param>
    /// <param name="dictionaryId">The dictionary id to decompress against.</param>
    void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength, uint dictionaryId);
}
