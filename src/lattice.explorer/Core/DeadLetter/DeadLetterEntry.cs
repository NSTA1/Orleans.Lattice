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
}
