namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only record of a single strict-mode dead-letter entry surfaced by
/// <see cref="ILatticeStateQuery.ListDeadLettersAsync"/>. Each entry captures an
/// item that schema enforcement diverted (rather than applied) because it failed
/// validation, along with why and where it arrived.
/// </summary>
/// <remarks>
/// <see cref="ValuePreview"/> is a size-bounded copy of the offending value's
/// leading bytes produced by the enforcement layer; the state API never widens
/// it. <see cref="ValueByteLength"/> reports the original (untruncated) length so
/// a consumer can tell the preview was truncated (also surfaced directly by
/// <see cref="PreviewTruncated"/>).
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.DeadLetterEntryRecord)]
[Immutable]
public sealed record DeadLetterEntryRecord
{
    /// <summary>The key of the diverted item.</summary>
    [Id(0)] public required string Key { get; init; }

    /// <summary>
    /// A size-bounded preview of the offending value's leading bytes. When
    /// <see cref="PreviewTruncated"/> is <see langword="true"/> this is shorter
    /// than the full value; <see cref="ValueByteLength"/> always reports the full
    /// length.
    /// </summary>
    [Id(1)] public byte[] ValuePreview { get; init; } = Array.Empty<byte>();

    /// <summary>The original (untruncated) value byte length.</summary>
    [Id(2)] public int ValueByteLength { get; init; }

    /// <summary>The validation reason the item was dead-lettered.</summary>
    [Id(3)] public required string Reason { get; init; }

    /// <summary>The ingest path the item arrived on.</summary>
    [Id(4)] public DeadLetterSourceKind Source { get; init; }

    /// <summary>The UTC instant the item was dead-lettered.</summary>
    [Id(5)] public DateTimeOffset TimestampUtc { get; init; }

    /// <summary>
    /// Whether <see cref="ValuePreview"/> was truncated to the enforcement
    /// layer's preview budget (that is, <see cref="ValueByteLength"/> exceeds the
    /// preview length).
    /// </summary>
    [Id(6)] public bool PreviewTruncated { get; init; }

    /// <summary>
    /// Compares two records by value, with <see cref="ValuePreview"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical records - and, in
    /// particular, a record and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The record to compare against.</param>
    public bool Equals(DeadLetterEntryRecord? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ValueByteLength == other.ValueByteLength
        && string.Equals(Reason, other.Reason, StringComparison.Ordinal)
        && Source == other.Source
        && TimestampUtc == other.TimestampUtc
        && PreviewTruncated == other.PreviewTruncated
        && BytesEqual(ValuePreview, other.ValuePreview);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        hash.Add(ValueByteLength);
        hash.Add(Reason, StringComparer.Ordinal);
        hash.Add(Source);
        hash.Add(TimestampUtc);
        hash.Add(PreviewTruncated);
        if (ValuePreview is { } preview)
        {
            hash.AddBytes(preview);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
