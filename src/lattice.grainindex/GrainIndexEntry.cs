namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// One index entry: the on-tree key that locates it and the JSON payload the
/// server-side predicate evaluator folds a predicate against.
/// </summary>
/// <remarks>
/// <para>
/// The key is built by <see cref="GrainIndexKeyEncoder"/> and the payload by
/// the projector; see <see cref="GrainIndexEntryValue"/> for the payload's
/// field-name contract.
/// </para>
/// <para>
/// Equality compares the key ordinally and the payload byte for byte, because
/// the projection diff decides whether an entry needs rewriting by comparing
/// two entries. The default record equality would have compared the payload
/// array by reference, which is never the question being asked. The hash code
/// is derived from the key alone, which is consistent (equal entries have equal
/// keys) and avoids hashing a payload on every lookup.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexEntry)]
public readonly record struct GrainIndexEntry
{
    /// <summary>Initialises an entry.</summary>
    /// <param name="key">The on-tree key. Must not be <c>null</c>.</param>
    /// <param name="value">The JSON payload. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexEntry(string key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        Key = key;
        Value = value;
    }

    /// <summary>The on-tree key that locates this entry within the index's tree.</summary>
    [Id(0)] public string Key { get; init; }

    /// <summary>
    /// The entry payload: a UTF-8 JSON document carrying the projected property
    /// value under the property's own name, plus the grain-key and
    /// property-name metadata fields.
    /// </summary>
    [Id(1)] public byte[] Value { get; init; }

    /// <summary>
    /// Compares the key ordinally and the payload by content.
    /// </summary>
    /// <param name="other">The entry to compare with.</param>
    /// <returns><c>true</c> when both the key and the payload bytes match.</returns>
    public bool Equals(GrainIndexEntry other) =>
        string.Equals(Key, other.Key, StringComparison.Ordinal)
        && Value.AsSpan().SequenceEqual(other.Value.AsSpan());

    /// <summary>Hashes the key, which equal entries always share.</summary>
    /// <returns>The key's ordinal hash code.</returns>
    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(Key ?? string.Empty);
}
