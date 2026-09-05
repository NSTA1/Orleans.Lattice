using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.DeadLetter;

/// <summary>
/// The explorer's view of a single strict-mode dead-letter entry, projected from
/// the state-API <see cref="DeadLetterEntryRecord"/>. Carries the (already
/// preview-bounded) value bytes plus the reason, source, and timestamp the DLQ
/// tab surfaces. Read-only: the explorer never mutates or requeues the entry.
/// </summary>
public sealed record DeadLetterEntry
{
    /// <summary>The key of the diverted item.</summary>
    public required string Key { get; init; }

    /// <summary>The size-bounded preview of the offending value's leading bytes (never widened by the explorer).</summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>The original (untruncated) value byte length.</summary>
    public int ValueByteLength { get; init; }

    /// <summary>Whether <see cref="Value"/> is a truncated preview of the full value.</summary>
    public bool Truncated { get; init; }

    /// <summary>The validation reason the item was dead-lettered.</summary>
    public required string Reason { get; init; }

    /// <summary>The ingest path the item arrived on.</summary>
    public DeadLetterSource Source { get; init; }

    /// <summary>The UTC instant the item was dead-lettered.</summary>
    public DateTimeOffset TimestampUtc { get; init; }

    /// <summary>Projects a state-API <see cref="DeadLetterEntryRecord"/> into a <see cref="DeadLetterEntry"/>.</summary>
    public static DeadLetterEntry From(DeadLetterEntryRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        return new DeadLetterEntry
        {
            Key = record.Key,
            Value = record.ValuePreview,
            ValueByteLength = record.ValueByteLength,
            Truncated = record.PreviewTruncated,
            Reason = record.Reason,
            Source = MapSource(record.Source),
            TimestampUtc = record.TimestampUtc,
        };
    }

    private static DeadLetterSource MapSource(DeadLetterSourceKind kind) => kind switch
    {
        DeadLetterSourceKind.Replication => DeadLetterSource.Replication,
        DeadLetterSourceKind.Restore => DeadLetterSource.Restore,
        DeadLetterSourceKind.LocalRejected => DeadLetterSource.LocalRejected,
        _ => DeadLetterSource.Unknown,
    };

    /// <summary>
    /// Compares two entries by value, with <see cref="Value"/> compared by content.
    /// The compiler-generated record equality compares the <see cref="byte"/> array
    /// with <see cref="EqualityComparer{T}.Default"/> (reference equality), so two
    /// structurally identical entries - and, in particular, an entry and its
    /// post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The entry to compare against.</param>
    public bool Equals(DeadLetterEntry? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ValueByteLength == other.ValueByteLength
        && Truncated == other.Truncated
        && string.Equals(Reason, other.Reason, StringComparison.Ordinal)
        && Source == other.Source
        && TimestampUtc == other.TimestampUtc
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        hash.Add(ValueByteLength);
        hash.Add(Truncated);
        hash.Add(Reason, StringComparer.Ordinal);
        hash.Add(Source);
        hash.Add(TimestampUtc);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
