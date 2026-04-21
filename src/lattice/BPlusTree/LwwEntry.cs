using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A single (<c>Key</c>, <see cref="LwwValue{T}"/>) pair used by
/// snapshot / restore bulk-load paths so TTL metadata and source
/// <see cref="Primitives.HybridLogicalClock"/> version survive transfer
/// between shards or trees.
/// <para>
/// The <see cref="LwwValue{T}"/> fields are stored flat (rather than as
/// a nested <c>LwwValue&lt;byte[]&gt;</c> property) because the Orleans
/// type-alias encoder has a codec-generation race when a DTO used in a
/// grain-interface signature embeds <c>LwwValue&lt;byte[]&gt;</c> as a
/// field — it intermittently produces malformed alias strings like
/// <c>ol.lwv[[byte[]]]]]</c>. Flat scalar fields sidestep the race while
/// preserving all LWW metadata.
/// </para>
/// Not part of the end-user API surface.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LwwEntry)]
[Immutable]
internal readonly record struct LwwEntry
{
    /// <summary>The key this entry belongs to.</summary>
    [Id(0)] public string Key { get; init; }

    /// <summary>The value; <c>null</c> for tombstones.</summary>
    [Id(1)] public byte[]? Value { get; init; }

    /// <summary>The hybrid-logical-clock timestamp of the write.</summary>
    [Id(2)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary><c>true</c> when this entry represents a delete.</summary>
    [Id(3)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which this entry expires, or <c>0</c>
    /// when the entry never expires.
    /// </summary>
    [Id(4)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Constructs an <see cref="LwwEntry"/> from a <see cref="LwwValue{T}"/>,
    /// preserving all LWW metadata (value, timestamp, tombstone flag, and
    /// expiry).
    /// </summary>
    public LwwEntry(string key, LwwValue<byte[]> lww)
    {
        Key = key;
        Value = lww.Value;
        Timestamp = lww.Timestamp;
        IsTombstone = lww.IsTombstone;
        ExpiresAtTicks = lww.ExpiresAtTicks;
    }

    /// <summary>
    /// Rehydrates the flattened fields back into an <see cref="LwwValue{T}"/>.
    /// </summary>
    public LwwValue<byte[]> ToLwwValue() => new()
    {
        Value = Value,
        Timestamp = Timestamp,
        IsTombstone = IsTombstone,
        ExpiresAtTicks = ExpiresAtTicks,
    };
}

