namespace Orleans.Lattice.Schema;

/// <summary>
/// A serializable dead-letter record retained when strict-mode ingest diverts a
/// non-compliant item instead of applying it. Entries are stored per tree in the
/// reserved <c>sys-schema-dlq</c> tree and surfaced (list / count) through
/// <see cref="ILatticeSchemaDeadLetterStore"/> for inspection and replay.
/// </summary>
/// <remarks>
/// <see cref="ValuePreview"/> is a bounded copy of the offending value's leading
/// bytes (never the full value, which may be large); <see cref="ValueByteLength"/>
/// records the original length so a consumer can tell the preview was truncated.
/// </remarks>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaDeadLetterEntry)]
[Immutable]
public sealed class LatticeSchemaDeadLetterEntry
{
    /// <summary>
    /// Initializes a new <see cref="LatticeSchemaDeadLetterEntry"/>.
    /// </summary>
    /// <param name="key">The key of the diverted item. Must not be <c>null</c>.</param>
    /// <param name="valuePreview">A bounded copy of the offending value's leading bytes. Must not be <c>null</c>.</param>
    /// <param name="valueByteLength">The original (untruncated) value byte length.</param>
    /// <param name="reason">The validation reason the item was dead-lettered. Must not be <c>null</c>.</param>
    /// <param name="source">The ingest path the item arrived on.</param>
    /// <param name="timestampUtc">The UTC instant the item was dead-lettered.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/>, <paramref name="valuePreview"/>, or <paramref name="reason"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="valueByteLength"/> is negative.</exception>
    public LatticeSchemaDeadLetterEntry(
        string key,
        byte[] valuePreview,
        int valueByteLength,
        string reason,
        LatticeSchemaDeadLetterSource source,
        DateTimeOffset timestampUtc)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(valuePreview);
        ArgumentNullException.ThrowIfNull(reason);
        ArgumentOutOfRangeException.ThrowIfNegative(valueByteLength);
        Key = key;
        // Defensively copy the caller-owned buffer so a mutation of the original
        // array after construction cannot change this [Immutable] entry.
        _valuePreview = (byte[])valuePreview.Clone();
        ValueByteLength = valueByteLength;
        Reason = reason;
        Source = source;
        TimestampUtc = timestampUtc;
    }

    /// <summary>The key of the diverted item.</summary>
    [Id(0)]
    public string Key { get; }

    // Serialized backing field for ValuePreview. The [Id] lives on the field so
    // the public getter can hand back a defensive copy on every read, keeping the
    // [Immutable] contract even against a caller that mutates the returned array.
    [Id(1)]
    private readonly byte[] _valuePreview;

    /// <summary>A bounded copy of the offending value's leading bytes.</summary>
    public byte[] ValuePreview => (byte[])_valuePreview.Clone();

    /// <summary>
    /// The original value byte length. Greater than <see cref="ValuePreview"/>
    /// length when the preview was truncated.
    /// </summary>
    [Id(2)]
    public int ValueByteLength { get; }

    /// <summary>The validation reason the item was dead-lettered.</summary>
    [Id(3)]
    public string Reason { get; }

    /// <summary>The ingest path the item arrived on.</summary>
    [Id(4)]
    public LatticeSchemaDeadLetterSource Source { get; }

    /// <summary>The UTC instant the item was dead-lettered.</summary>
    [Id(5)]
    public DateTimeOffset TimestampUtc { get; }
}
