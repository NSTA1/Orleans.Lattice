using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The built-in <see cref="IGrainKeyCodec{TGrain}"/> for a grain whose primary
/// key is a <see cref="Guid"/>. The encoded key is the 32-character lowercase
/// hexadecimal ("N") form, which is fixed width so index entries stay
/// lexicographically comparable.
/// </summary>
/// <remarks>
/// A compound key (a <see cref="Guid"/> plus a string extension) is not
/// encodable by this codec, because the extension is not part of the encoded
/// form and the grain could not be resolved back. Such a grain is not indexable
/// with the default codec and must supply its own.
/// </remarks>
/// <typeparam name="TGrain">The indexed <see cref="Guid"/>-keyed grain interface type.</typeparam>
public sealed class GuidGrainKeyCodec<TGrain> : IGrainKeyCodec<TGrain>
    where TGrain : IGrainWithGuidKey
{
    /// <summary>The number of characters in an encoded key.</summary>
    private const int EncodedLength = 32;

    /// <summary>
    /// The shared, stateless instance. The codec holds no state, so one
    /// instance serves every declaration and the projection path never
    /// allocates a codec.
    /// </summary>
    public static GuidGrainKeyCodec<TGrain> Instance { get; } = new();

    /// <inheritdoc />
    public Type GrainInterfaceType => typeof(TGrain);

    /// <inheritdoc />
    public bool TryEncode(GrainId grainId, [NotNullWhen(true)] out string? encodedKey)
    {
        if (!grainId.IsDefault
            && grainId.TryGetGuidKey(out var key, out var keyExtension)
            && keyExtension is null)
        {
            encodedKey = key.ToString("N", CultureInfo.InvariantCulture);
            return true;
        }

        encodedKey = null;
        return false;
    }

    /// <inheritdoc />
    public string Encode(GrainId grainId) =>
        TryEncode(grainId, out var encodedKey)
            ? encodedKey
            : throw new GrainIndexKeyEncodingException(
                typeof(TGrain).FullName ?? typeof(TGrain).Name,
                grainId.ToString(),
                "The default Guid codec encodes a plain Guid key only; a compound "
                + "(Guid plus string extension) key needs a custom IGrainKeyCodec.");

    /// <inheritdoc />
    public TGrain Resolve(IGrainFactory grainFactory, string encodedKey)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(encodedKey);
        if (encodedKey.Length != EncodedLength
            || !Guid.TryParseExact(encodedKey, "N", out var key))
        {
            throw new GrainIndexKeyEncodingException(
                typeof(TGrain).FullName ?? typeof(TGrain).Name,
                encodedKey,
                $"An encoded Guid key must be {EncodedLength} hexadecimal characters.");
        }

        return grainFactory.GetGrain<TGrain>(key);
    }

    /// <inheritdoc />
    IGrain IGrainKeyCodec.Resolve(IGrainFactory grainFactory, string encodedKey) =>
        Resolve(grainFactory, encodedKey);
}
