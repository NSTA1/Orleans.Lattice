namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Thrown when a grain's key cannot be encoded into (or decoded back out of) an
/// index entry by the supplied or default <see cref="IGrainKeyCodec{TGrain}"/>,
/// which makes that grain not indexable.
/// <para>
/// A grain index entry has to point back at the grain that produced it, so a key
/// the codec cannot round-trip is a hard failure rather than a silently skipped
/// grain: skipping would leave the index quietly incomplete and every query over
/// it quietly wrong.
/// </para>
/// </summary>
/// <remarks>
/// The type derives directly from <see cref="Exception"/> so Orleans can deep-copy
/// it across a co-located grain-call boundary without a hand-written copier, and
/// is Orleans-serializable so the failure propagates intact across a silo boundary.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.GrainIndexKeyEncodingException)]
public sealed class GrainIndexKeyEncodingException : Exception
{
    /// <summary>
    /// The grain interface type whose key could not be encoded, as a CLR type
    /// name. Empty on the message-only constructors.
    /// </summary>
    [Id(0)]
    public string GrainInterfaceTypeName { get; }

    /// <summary>
    /// The offending grain key rendered for diagnostics, or the offending
    /// encoded key when the failure happened while decoding. Empty on the
    /// message-only constructors.
    /// </summary>
    [Id(1)]
    public string GrainKey { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception-construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public GrainIndexKeyEncodingException()
    {
        GrainInterfaceTypeName = string.Empty;
        GrainKey = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty context.</summary>
    /// <param name="message">Diagnostic context describing the failure.</param>
    public GrainIndexKeyEncodingException(string message) : base(message)
    {
        GrainInterfaceTypeName = string.Empty;
        GrainKey = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the failure.</param>
    /// <param name="innerException">The underlying cause.</param>
    public GrainIndexKeyEncodingException(string message, Exception innerException)
        : base(message, innerException)
    {
        GrainInterfaceTypeName = string.Empty;
        GrainKey = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the grain interface type, the
    /// offending key, and the reason it could not be encoded. The primary
    /// production throw shape.
    /// </summary>
    /// <param name="grainInterfaceTypeName">The grain interface type name. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The offending grain or encoded key. Must not be <c>null</c>.</param>
    /// <param name="reason">Why the key is not encodable. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexKeyEncodingException(string grainInterfaceTypeName, string grainKey, string reason)
        : base(BuildMessage(grainInterfaceTypeName, grainKey, reason))
    {
        GrainInterfaceTypeName = grainInterfaceTypeName;
        GrainKey = grainKey;
    }

    private static string BuildMessage(string grainInterfaceTypeName, string grainKey, string reason)
    {
        ArgumentNullException.ThrowIfNull(grainInterfaceTypeName);
        ArgumentNullException.ThrowIfNull(grainKey);
        ArgumentNullException.ThrowIfNull(reason);
        return $"The key '{grainKey}' of grain type '{grainInterfaceTypeName}' cannot be encoded "
            + $"into a grain-index entry, so the grain is not indexable. {reason}";
    }
}
