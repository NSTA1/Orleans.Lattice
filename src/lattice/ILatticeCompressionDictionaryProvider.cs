namespace Orleans.Lattice;

/// <summary>
/// Resolves shared compression-dictionary bytes by a stable numeric
/// id. The dictionary-aware compressor
/// (<see cref="ZstdDictionaryLatticeCompressor"/>) consults a
/// registered provider to turn the dictionary id carried in a wire
/// frame into the dictionary bytes it must load before
/// compressing or decompressing.
/// <para>
/// A dictionary id is a host-assigned, deployment-stable
/// <see cref="uint"/>. The reserved id <c>0</c> always means "no
/// dictionary" - providers must report it as absent
/// (<see cref="TryGetDictionary"/> returns <see langword="false"/>)
/// and the dictionary-aware compressor treats it as a plain,
/// dictionary-less Zstandard frame. Operator-supplied (pre-trained)
/// dictionaries are the primary path: a host ships the dictionary
/// asset with its configuration and registers a provider that maps
/// the id to those bytes. The id travels in the frame so a receiver
/// running the same configuration selects the matching dictionary;
/// a receiver that does not recognise the id surfaces
/// <see cref="System.NotSupportedException"/> from the consuming
/// decoder rather than silently mis-decoding.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from
/// multiple threads and must return the same bytes for the same id
/// for the lifetime of the deployment (the dictionary id is the
/// stable identity of a fixed dictionary; changing the bytes behind
/// an id would silently corrupt in-flight frames). The returned
/// memory must not be mutated by the caller.
/// </para>
/// </summary>
public interface ILatticeCompressionDictionaryProvider
{
    /// <summary>
    /// Attempts to resolve the dictionary bytes registered for
    /// <paramref name="dictionaryId"/>. Returns <see langword="true"/>
    /// and sets <paramref name="dictionary"/> to the bytes when the id
    /// is known; returns <see langword="false"/> and sets
    /// <paramref name="dictionary"/> to
    /// <see cref="ReadOnlyMemory{T}.Empty"/> when it is not (including
    /// the reserved "no dictionary" id <c>0</c>).
    /// </summary>
    /// <param name="dictionaryId">
    /// The stable dictionary id carried in the wire frame. The
    /// reserved value <c>0</c> means "no dictionary" and must report
    /// as absent.
    /// </param>
    /// <param name="dictionary">
    /// On success, the dictionary bytes; otherwise
    /// <see cref="ReadOnlyMemory{T}.Empty"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the id resolves to a registered
    /// dictionary; otherwise <see langword="false"/>.
    /// </returns>
    bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> dictionary);
}
