using System.Collections.Frozen;

namespace Orleans.Lattice;

/// <summary>
/// Operator-supplied <see cref="ILatticeCompressionDictionaryProvider"/>
/// backed by a fixed, pre-trained set of dictionaries keyed by stable
/// id. This is the primary way a host ships shared compression
/// dictionaries: the dictionary bytes are produced offline (trained
/// against representative payloads, or hand-built) and shipped with the
/// deployment's configuration, then registered by id on every silo that
/// produces or consumes dictionary frames.
/// </summary>
public sealed class OperatorSuppliedCompressionDictionaryProvider : ILatticeCompressionDictionaryProvider
{
    private readonly FrozenDictionary<uint, ReadOnlyMemory<byte>> _dictionaries;

    /// <summary>
    /// An empty provider that resolves no dictionary ids. Registered as
    /// the default so the dictionary-aware compressor's dependency is
    /// always satisfiable; a default build never resolves a dictionary.
    /// </summary>
    public static OperatorSuppliedCompressionDictionaryProvider Empty { get; } =
        new(new Dictionary<uint, ReadOnlyMemory<byte>>());

    /// <summary>
    /// Initialises the provider from a map of stable dictionary id to
    /// dictionary bytes. The reserved id <c>0</c> ("no dictionary") must
    /// not be present, and an empty dictionary value is rejected.
    /// </summary>
    /// <param name="dictionaries">
    /// Stable dictionary id to dictionary bytes. Copied into an internal
    /// lookup, so later mutation of the supplied collection does not
    /// affect the provider.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="dictionaries"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="dictionaries"/> contains the reserved id <c>0</c>
    /// or an empty dictionary value.
    /// </exception>
    public OperatorSuppliedCompressionDictionaryProvider(
        IReadOnlyDictionary<uint, ReadOnlyMemory<byte>> dictionaries)
    {
        ArgumentNullException.ThrowIfNull(dictionaries);

        foreach (var (id, bytes) in dictionaries)
        {
            if (id == 0)
            {
                throw new ArgumentException(
                    "Dictionary id 0 is reserved for 'no dictionary' and must not be registered.",
                    nameof(dictionaries));
            }
            if (bytes.IsEmpty)
            {
                throw new ArgumentException(
                    $"Dictionary id {id} maps to an empty dictionary; a shared dictionary must carry bytes.",
                    nameof(dictionaries));
            }
        }

        _dictionaries = dictionaries.ToFrozenDictionary();
    }

    /// <inheritdoc />
    public bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> dictionary)
    {
        if (dictionaryId == 0)
        {
            dictionary = ReadOnlyMemory<byte>.Empty;
            return false;
        }
        return _dictionaries.TryGetValue(dictionaryId, out dictionary);
    }
}
