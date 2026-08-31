using System.Diagnostics.CodeAnalysis;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The built-in <see cref="IGrainKeyCodec{TGrain}"/> for a grain whose primary
/// key is a <see cref="string"/>. The encoded key is the grain's string key
/// verbatim, which keeps index entries ordered by the same collation the grain
/// keys themselves use.
/// </summary>
/// <typeparam name="TGrain">The indexed string-keyed grain interface type.</typeparam>
public sealed class StringGrainKeyCodec<TGrain> : IGrainKeyCodec<TGrain>
    where TGrain : IGrainWithStringKey
{
    /// <summary>
    /// The shared, stateless instance. The codec holds no state, so one
    /// instance serves every declaration and the projection path never
    /// allocates a codec.
    /// </summary>
    public static StringGrainKeyCodec<TGrain> Instance { get; } = new();

    /// <inheritdoc />
    public Type GrainInterfaceType => typeof(TGrain);

    /// <inheritdoc />
    public bool TryEncode(GrainId grainId, [NotNullWhen(true)] out string? encodedKey)
    {
        if (grainId.IsDefault)
        {
            encodedKey = null;
            return false;
        }

        var key = grainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
        {
            encodedKey = null;
            return false;
        }

        encodedKey = key;
        return true;
    }

    /// <inheritdoc />
    public string Encode(GrainId grainId) =>
        TryEncode(grainId, out var encodedKey)
            ? encodedKey
            : throw new GrainIndexKeyEncodingException(
                typeof(TGrain).FullName ?? typeof(TGrain).Name,
                grainId.ToString(),
                "A string-keyed grain must have a non-empty primary key.");

    /// <inheritdoc />
    public TGrain Resolve(IGrainFactory grainFactory, string encodedKey)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(encodedKey);
        if (encodedKey.Length == 0)
        {
            throw new GrainIndexKeyEncodingException(
                typeof(TGrain).FullName ?? typeof(TGrain).Name,
                encodedKey,
                "An empty encoded key does not name a string-keyed grain.");
        }

        return grainFactory.GetGrain<TGrain>(encodedKey);
    }

    /// <inheritdoc />
    IGrain IGrainKeyCodec.Resolve(IGrainFactory grainFactory, string encodedKey) =>
        Resolve(grainFactory, encodedKey);
}
