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
}
