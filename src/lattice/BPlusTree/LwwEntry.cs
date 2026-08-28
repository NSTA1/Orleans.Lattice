using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A single (<c>Key</c>, <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>) pair used by
/// snapshot / restore bulk-load paths so TTL metadata and source
/// <see cref="Orleans.Lattice.HybridLogicalClock"/> version survive transfer
/// between shards or trees.
/// <para>
/// The <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> fields are stored flat (rather than as
/// a nested <c>LwwValue&lt;byte[]&gt;</c> property) because the Orleans
/// type-alias encoder has a codec-generation race when a DTO used in a
/// grain-interface signature embeds <c>LwwValue&lt;byte[]&gt;</c> as a
/// field - it intermittently produces malformed alias strings like
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
    /// Identifier of the cluster that authored the mutation the entry
    /// represents, or <c>null</c> for a local write. Round-trips verbatim
    /// through <see cref="Orleans.Lattice.Primitives.LwwValue{T}.OriginClusterId"/> across every raw
    /// bulk-load / snapshot / saga-pre-value path. Wire-compatible: legacy
    /// persisted state decodes to <c>null</c>.
    /// </summary>
    [Id(5)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Sparse <c>{originClusterId → HybridLogicalClock}</c> frontier
    /// captured at commit time, or <c>null</c> when the writer did not
    /// supply one. Round-trips verbatim through
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.VectorClock"/> across every raw bulk-load /
    /// snapshot / saga-pre-value path so the frontier survives transfer
    /// between shards or trees. Wire-compatible: legacy persisted state
    /// decodes to <c>null</c>.
    /// </summary>
    [Id(6)] public VersionVector? VectorClock { get; init; }

    /// <summary>
    /// Durable per-key convergence discriminator: the
    /// <see cref="LatticeMergeMode"/> the key was last written under, carried
    /// verbatim from the snapshot projection so the backup capture engine can
    /// label each key with its true merge mode rather than the coarse declared
    /// tree mode. <c>null</c> for a plain last-writer-wins key and for legacy
    /// entries authored before the discriminator existed, in which case the
    /// consumer falls back to the declared tree mode. Wire-compatible: legacy
    /// persisted / streamed state decodes to <c>null</c>.
    /// </summary>
    [Id(7)] public LatticeMergeMode? MergeMode { get; init; }

    /// <summary>
    /// Constructs an <see cref="LwwEntry"/> from a <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>,
    /// preserving all LWW metadata (value, timestamp, tombstone flag,
    /// expiry, origin cluster id, and vector clock) and, optionally, the
    /// per-key <see cref="LatticeMergeMode"/> discriminator.
    /// </summary>
    public LwwEntry(string key, LwwValue<byte[]> lww, LatticeMergeMode? mergeMode = null)
    {
        Key = key;
        Value = lww.Value;
        Timestamp = lww.Timestamp;
        IsTombstone = lww.IsTombstone;
        ExpiresAtTicks = lww.ExpiresAtTicks;
        OriginClusterId = lww.OriginClusterId;
        // Egress copy: this entry is an [Immutable] carrier handed to a caller,
        // so sharing the stored frontier would give that caller a live handle on
        // the grain's durable state (and, co-located, Orleans elides the copy
        // that would otherwise hide it). Null on every purely local write, so the
        // dominant path pays nothing.
        VectorClock = lww.VectorClock?.Clone();
        MergeMode = mergeMode;
    }

    /// <summary>
    /// Rehydrates the flattened fields back into an <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>.
    /// </summary>
    public LwwValue<byte[]> ToLwwValue() => new()
    {
        Value = Value,
        Timestamp = Timestamp,
        IsTombstone = IsTombstone,
        ExpiresAtTicks = ExpiresAtTicks,
        OriginClusterId = OriginClusterId,
        VectorClock = VectorClock,
    };
}

