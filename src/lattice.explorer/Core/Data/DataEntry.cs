using Orleans.Lattice.Api.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// The explorer's view of a single entry, projected from the state-API
/// <see cref="EntryRecord"/>. Carries the (possibly truncated) value bytes plus
/// the metadata the Data tab surfaces.
/// </summary>
public sealed record DataEntry
{
    /// <summary>The entry key.</summary>
    public required string Key { get; init; }

    /// <summary>The fetched value bytes, truncated to a preview when <see cref="Truncated"/> is set.</summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>The full length, in bytes, of the stored value.</summary>
    public int ValueLength { get; init; }

    /// <summary>Whether <see cref="Value"/> is a truncated preview of the full value.</summary>
    public bool Truncated { get; init; }

    /// <summary>The entry's hybrid-logical-clock timestamp.</summary>
    public HybridLogicalClock Hlc { get; init; }

    /// <summary>Whether the entry is a tombstone (deleted) marker.</summary>
    public bool IsTombstone { get; init; }

    /// <summary>Absolute UTC tick at which the entry expires, or <c>0</c> when it does not expire.</summary>
    public long ExpiresAtTicks { get; init; }

    /// <summary>The CRDT shape tag when the value is a typed CRDT, or <see langword="null"/>.</summary>
    public string? CrdtShape { get; init; }

    /// <summary>Projects a state-API <see cref="EntryRecord"/> into a <see cref="DataEntry"/>.</summary>
    public static DataEntry From(EntryRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        return new DataEntry
        {
            Key = record.Key,
            Value = record.ValuePreview,
            ValueLength = record.ValueLength,
            Truncated = record.Truncated,
            Hlc = record.Hlc,
            IsTombstone = record.IsTombstone,
            ExpiresAtTicks = record.ExpiresAtTicks,
            CrdtShape = record.CrdtShape,
        };
    }
}
