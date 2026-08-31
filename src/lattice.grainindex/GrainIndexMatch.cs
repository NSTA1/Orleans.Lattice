namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// One grain matched by an index query, together with the index entry that
/// matched it.
/// <para>
/// The payload is the projected document for a <b>single</b> property - the one
/// the driving scan ran over - not the grain's whole state and not its whole
/// projection, because that is all an index entry ever holds. Use
/// <see cref="PropertyName"/> to know which property the payload describes, and
/// resolve the grain itself when the rest of its state is needed.
/// </para>
/// </summary>
/// <remarks>
/// Equality compares the grain key and property name ordinally and the payload
/// byte for byte, mirroring <see cref="GrainIndexEntry"/>; the hash code is
/// derived from the grain key and property name alone.
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexMatch)]
public readonly record struct GrainIndexMatch
{
    /// <summary>Initialises a match.</summary>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="propertyName">The projected property the entry carries. Must not be <c>null</c>.</param>
    /// <param name="value">The entry payload. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexMatch(string grainKey, string propertyName, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(grainKey);
        ArgumentNullException.ThrowIfNull(propertyName);
        ArgumentNullException.ThrowIfNull(value);
        GrainKey = grainKey;
        PropertyName = propertyName;
        Value = value;
    }

    /// <summary>
    /// The matched grain's encoded key, as produced by the index's
    /// <see cref="IGrainKeyCodec"/>.
    /// </summary>
    [Id(0)] public string GrainKey { get; init; }

    /// <summary>The projected property whose entry matched.</summary>
    [Id(1)] public string PropertyName { get; init; }

    /// <summary>
    /// The matched entry's payload: a UTF-8 JSON document carrying the projected
    /// value under the property's own name, plus the metadata fields named by
    /// <see cref="GrainIndexEntryValue"/>. Empty when the query only asked for
    /// grain identities, because that path never transfers payloads.
    /// </summary>
    [Id(2)] public byte[] Value { get; init; }

    /// <summary>Compares the identity fields ordinally and the payload by content.</summary>
    /// <param name="other">The match to compare with.</param>
    /// <returns><c>true</c> when the grain key, property name, and payload bytes all match.</returns>
    public bool Equals(GrainIndexMatch other) =>
        string.Equals(GrainKey, other.GrainKey, StringComparison.Ordinal)
        && string.Equals(PropertyName, other.PropertyName, StringComparison.Ordinal)
        && Value.AsSpan().SequenceEqual(other.Value.AsSpan());

    /// <summary>Hashes the identity fields, which equal matches always share.</summary>
    /// <returns>The combined ordinal hash code of the grain key and property name.</returns>
    public override int GetHashCode() => HashCode.Combine(
        StringComparer.Ordinal.GetHashCode(GrainKey ?? string.Empty),
        StringComparer.Ordinal.GetHashCode(PropertyName ?? string.Empty));
}
