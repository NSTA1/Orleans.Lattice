namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Derives the stable identity string of an <see cref="IGrainKeyCodec"/>, which
/// is the form the index registry persists and the
/// <see cref="GrainIndexFingerprint"/> hashes.
/// <para>
/// The codec determines both how a grain's identity is written into an index
/// entry's key and the lexicographic order those keys take on the tree, so it is
/// one of the drift-significant fields. It cannot be persisted as an object, so
/// it is reduced to a name a later process can compare without loading the
/// codec's assembly.
/// </para>
/// </summary>
/// <remarks>
/// The identity is the codec's CLR type name including any generic arguments -
/// so <c>StringGrainKeyCodec&lt;IUserGrain&gt;</c> and
/// <c>GuidGrainKeyCodec&lt;IUserGrain&gt;</c> are distinct - but excluding the
/// assembly and version, because a codec staying the same type across a package
/// upgrade is not a drift event. A custom codec that changes its encoding
/// without changing its type name is therefore invisible here; the
/// <see cref="GrainIndexFingerprint.CurrentVersion"/> stamp is the documented
/// lever for forcing a rebuild in that case.
/// </remarks>
public static class GrainIndexKeyCodecIdentity
{
    /// <summary>
    /// Returns the stable identity of <paramref name="codec"/>.
    /// </summary>
    /// <param name="codec">The grain-key codec. Must not be <c>null</c>.</param>
    /// <returns>
    /// The codec's CLR type name, or its simple name when the type has no full
    /// name (which only happens for an open generic type parameter).
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="codec"/> is <c>null</c>.</exception>
    public static string For(IGrainKeyCodec codec)
    {
        ArgumentNullException.ThrowIfNull(codec);
        var type = codec.GetType();
        return type.FullName ?? type.Name;
    }
}
