using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The built-in <see cref="IGrainKeyCodec{TGrain}"/> for a grain whose primary
/// key is a 64-bit integer. The encoded key is a fixed-width 16-character
/// lowercase hexadecimal rendering of the key with its sign bit flipped, so the
/// lexicographic order of encoded keys matches the numeric order of the keys
/// themselves across the whole signed range.
/// </summary>
/// <remarks>
/// A compound key (an integer plus a string extension) is not encodable by this
/// codec, because the extension is not part of the encoded form and the grain
/// could not be resolved back. Such a grain is not indexable with the default
/// codec and must supply its own.
/// </remarks>
/// <typeparam name="TGrain">The indexed integer-keyed grain interface type.</typeparam>
public sealed class IntegerGrainKeyCodec<TGrain> : IGrainKeyCodec<TGrain>
    where TGrain : IGrainWithIntegerKey
{
    /// <summary>The number of characters in an encoded key.</summary>
    private const int EncodedLength = 16;

    /// <summary>
    /// The bias added (by XOR) before rendering, which maps
    /// <see cref="long.MinValue"/> to zero so unsigned hexadecimal ordering
    /// reproduces signed numeric ordering.
    /// </summary>
    private const ulong SignBias = 0x8000_0000_0000_0000UL;

    /// <summary>
    /// The shared, stateless instance. The codec holds no state, so one
    /// instance serves every declaration and the projection path never
    /// allocates a codec.
    /// </summary>
    public static IntegerGrainKeyCodec<TGrain> Instance { get; } = new();

    /// <inheritdoc />
    public Type GrainInterfaceType => typeof(TGrain);

    /// <inheritdoc />
    public bool TryEncode(GrainId grainId, [NotNullWhen(true)] out string? encodedKey)
    {
        if (!grainId.IsDefault
            && grainId.TryGetIntegerKey(out var key, out var keyExtension)
            && keyExtension is null)
        {
            encodedKey = ((ulong)key ^ SignBias).ToString("x16", CultureInfo.InvariantCulture);
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
                "The default integer codec encodes a plain integer key only; a compound "
                + "(integer plus string extension) key needs a custom IGrainKeyCodec.");

    /// <inheritdoc />
    public TGrain Resolve(IGrainFactory grainFactory, string encodedKey)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(encodedKey);
        if (encodedKey.Length != EncodedLength
            || !ulong.TryParse(encodedKey, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var biased))
        {
            throw new GrainIndexKeyEncodingException(
                typeof(TGrain).FullName ?? typeof(TGrain).Name,
                encodedKey,
                $"An encoded integer key must be {EncodedLength} hexadecimal characters.");
        }

        return grainFactory.GetGrain<TGrain>((long)(biased ^ SignBias));
    }

    /// <inheritdoc />
    IGrain IGrainKeyCodec.Resolve(IGrainFactory grainFactory, string encodedKey) =>
        Resolve(grainFactory, encodedKey);
}
