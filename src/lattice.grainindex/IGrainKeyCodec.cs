using System.Diagnostics.CodeAnalysis;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

// The non-generic IGrainKeyCodec and the generic IGrainKeyCodec<TGrain> are
// declared together, in the same way the BCL pairs IEnumerable with
// IEnumerable<T>: they are one contract at two levels of typing, and separating
// them would only hide that the non-generic form exists purely so a
// heterogeneous list of index definitions can encode a key without knowing the
// grain interface type at compile time.

/// <summary>
/// Encodes an indexed grain's identity into the stable string an index entry
/// stores, and resolves that string back into a grain reference. The
/// non-generic half of the contract, consumed where the grain interface type is
/// not known statically (for example by a heterogeneous list of
/// <see cref="IGrainIndexDefinition"/>).
/// </summary>
/// <remarks>
/// An implementation must be a pure, thread-safe, allocation-light function of
/// the supplied <see cref="GrainId"/>: it is invoked once per indexed grain per
/// mutation. It must also round-trip, so that
/// <c>Resolve(factory, Encode(grainId))</c> addresses the grain
/// <c>grainId</c> named.
/// </remarks>
public interface IGrainKeyCodec
{
    /// <summary>The grain interface type this codec encodes keys for.</summary>
    Type GrainInterfaceType { get; }

    /// <summary>
    /// Attempts to encode <paramref name="grainId"/> without throwing. Use this
    /// to probe whether a grain is indexable; use <see cref="Encode(GrainId)"/>
    /// on the projection path, where a non-encodable key is a fault rather than
    /// a condition.
    /// </summary>
    /// <param name="grainId">The identity of the grain being indexed.</param>
    /// <param name="encodedKey">On success, the encoded key; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when the key was encoded; otherwise <c>false</c>.</returns>
    bool TryEncode(GrainId grainId, [NotNullWhen(true)] out string? encodedKey);

    /// <summary>
    /// Encodes <paramref name="grainId"/> into the string an index entry stores.
    /// </summary>
    /// <param name="grainId">The identity of the grain being indexed.</param>
    /// <returns>The encoded key.</returns>
    /// <exception cref="GrainIndexKeyEncodingException">The key cannot be encoded, so the grain is not indexable.</exception>
    string Encode(GrainId grainId);

    /// <summary>
    /// Resolves a previously encoded key back into a grain reference.
    /// </summary>
    /// <param name="grainFactory">The factory used to address the grain. Must not be <c>null</c>.</param>
    /// <param name="encodedKey">A key previously produced by <see cref="Encode(GrainId)"/>. Must not be <c>null</c>.</param>
    /// <returns>A reference to the grain the key names.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="GrainIndexKeyEncodingException"><paramref name="encodedKey"/> is not a key this codec produced.</exception>
    IGrain Resolve(IGrainFactory grainFactory, string encodedKey);
}

/// <summary>
/// Encodes an indexed grain's identity into the stable string an index entry
/// stores, and resolves that string back into a strongly typed grain reference.
/// This is the pluggable seam a declaration overrides with
/// <see cref="GrainIndexBuilder{TGrain, TState}.WithKeyCodec(IGrainKeyCodec{TGrain})"/>
/// when the built-in string, <see cref="Guid"/>, and integer codecs do not fit.
/// </summary>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
public interface IGrainKeyCodec<TGrain> : IGrainKeyCodec
    where TGrain : IGrain
{
    /// <summary>
    /// Resolves a previously encoded key back into a strongly typed grain
    /// reference.
    /// </summary>
    /// <param name="grainFactory">The factory used to address the grain. Must not be <c>null</c>.</param>
    /// <param name="encodedKey">A key previously produced by <see cref="IGrainKeyCodec.Encode(GrainId)"/>. Must not be <c>null</c>.</param>
    /// <returns>A reference to the grain the key names.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="GrainIndexKeyEncodingException"><paramref name="encodedKey"/> is not a key this codec produced.</exception>
    new TGrain Resolve(IGrainFactory grainFactory, string encodedKey);
}
