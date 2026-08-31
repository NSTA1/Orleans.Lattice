using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Selects the built-in <see cref="IGrainKeyCodec{TGrain}"/> that matches a
/// grain's primary-key shape. A declaration that does not call
/// <see cref="GrainIndexBuilder{TGrain, TState}.WithKeyCodec(IGrainKeyCodec{TGrain})"/>
/// gets its codec from here.
/// </summary>
public static class GrainKeyCodec
{
    /// <summary>
    /// Returns the built-in codec for <typeparamref name="TGrain"/>: the string,
    /// <see cref="Guid"/>, or integer codec, chosen from the key interface the
    /// grain declares.
    /// </summary>
    /// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
    /// <returns>The shared built-in codec instance for that key shape.</returns>
    /// <exception cref="GrainIndexKeyEncodingException">
    /// <typeparamref name="TGrain"/> declares no supported key interface (a
    /// compound-keyed grain, for example), or declares more than one, so no
    /// built-in codec can encode its key and the grain is not indexable without
    /// a custom codec.
    /// </exception>
    public static IGrainKeyCodec<TGrain> CreateDefault<TGrain>()
        where TGrain : IGrain
    {
        if (TryCreateDefault<TGrain>(out var codec))
        {
            return codec;
        }

        var grainType = typeof(TGrain);
        var reason = CountSupportedKeyInterfaces(grainType) > 1
            ? "It declares more than one of IGrainWithStringKey, IGrainWithGuidKey, and "
                + "IGrainWithIntegerKey, so the key shape is ambiguous."
            : "It declares none of IGrainWithStringKey, IGrainWithGuidKey, or "
                + "IGrainWithIntegerKey. Compound-keyed grains are not covered by a built-in "
                + "codec; supply one with WithKeyCodec.";

        throw new GrainIndexKeyEncodingException(
            grainType.FullName ?? grainType.Name,
            string.Empty,
            reason);
    }

    /// <summary>
    /// Attempts to select the built-in codec for <typeparamref name="TGrain"/>
    /// without throwing.
    /// </summary>
    /// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
    /// <param name="codec">On success, the shared built-in codec instance; otherwise <c>null</c>.</param>
    /// <returns>
    /// <c>true</c> when <typeparamref name="TGrain"/> declares exactly one
    /// supported key interface; otherwise <c>false</c>.
    /// </returns>
    public static bool TryCreateDefault<TGrain>([NotNullWhen(true)] out IGrainKeyCodec<TGrain>? codec)
        where TGrain : IGrain
    {
        var grainType = typeof(TGrain);
        var openCodecType = SelectOpenCodecType(grainType);
        if (openCodecType is null)
        {
            codec = null;
            return false;
        }

        codec = (IGrainKeyCodec<TGrain>)openCodecType
            .MakeGenericType(grainType)
            .GetProperty(nameof(StringGrainKeyCodec<IGrainWithStringKey>.Instance),
                BindingFlags.Public | BindingFlags.Static)!
            .GetValue(null)!;
        return true;
    }

    /// <summary>
    /// Returns the open generic codec type matching <paramref name="grainType"/>'s
    /// single supported key interface, or <c>null</c> when it declares none or
    /// more than one.
    /// </summary>
    private static Type? SelectOpenCodecType(Type grainType)
    {
        Type? selected = null;
        var matches = 0;

        if (typeof(IGrainWithStringKey).IsAssignableFrom(grainType))
        {
            selected = typeof(StringGrainKeyCodec<>);
            matches++;
        }

        if (typeof(IGrainWithGuidKey).IsAssignableFrom(grainType))
        {
            selected = typeof(GuidGrainKeyCodec<>);
            matches++;
        }

        if (typeof(IGrainWithIntegerKey).IsAssignableFrom(grainType))
        {
            selected = typeof(IntegerGrainKeyCodec<>);
            matches++;
        }

        return matches == 1 ? selected : null;
    }

    /// <summary>
    /// Counts how many of the three supported key interfaces
    /// <paramref name="grainType"/> declares, used only to word the failure.
    /// </summary>
    private static int CountSupportedKeyInterfaces(Type grainType)
    {
        var matches = 0;
        if (typeof(IGrainWithStringKey).IsAssignableFrom(grainType))
        {
            matches++;
        }

        if (typeof(IGrainWithGuidKey).IsAssignableFrom(grainType))
        {
            matches++;
        }

        if (typeof(IGrainWithIntegerKey).IsAssignableFrom(grainType))
        {
            matches++;
        }

        return matches;
    }
}
