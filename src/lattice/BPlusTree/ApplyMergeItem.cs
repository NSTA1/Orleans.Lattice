using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Single item in a batched LWW apply request submitted via
/// <see cref="IReplicationApplyGrain.ApplyMergeManyAsync"/>. Carries the
/// authoring cluster's metadata (<see cref="SourceHlc"/>,
/// <see cref="OriginClusterId"/>, <see cref="SourceVectorClock"/>,
/// <see cref="ExpiresAtTicks"/>) verbatim so the persisted
/// <see cref="LwwValue{T}"/> matches the authoring cluster bit-identically,
/// the same contract as the per-entry
/// <see cref="IReplicationApplyGrain.ApplySetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyDeleteAsync"/> methods.
/// <para>
/// <see cref="IsTombstone"/> distinguishes a remote Set from a remote
/// Delete: tombstone items have a <see langword="null"/>
/// <see cref="Value"/> and are persisted as a tombstone
/// <see cref="LwwValue{T}"/>; non-tombstone items carry the committed
/// payload bytes. The <see cref="ExpiresAtTicks"/> slot is preserved
/// for non-tombstone items only and must be <c>0</c> on tombstones -
/// the apply path mirrors the per-entry behaviour where deletes never
/// carry an expiry.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ApplyMergeItem)]
[Immutable]
internal readonly record struct ApplyMergeItem
{
    /// <summary>The key the remote write targeted.</summary>
    [Id(0)] public string Key { get; init; }

    /// <summary>
    /// The committed value bytes for a remote Set; <see langword="null"/>
    /// for a remote Delete (where <see cref="IsTombstone"/> is
    /// <see langword="true"/>).
    /// </summary>
    [Id(1)] public byte[]? Value { get; init; }

    /// <summary>The HLC stamped by the remote cluster.</summary>
    [Id(2)] public HybridLogicalClock SourceHlc { get; init; }

    /// <summary>The id of the remote cluster that authored the write.</summary>
    [Id(3)] public string OriginClusterId { get; init; }

    /// <summary>
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <see langword="null"/> when the producing cluster does not
    /// stamp a frontier. Stamped verbatim onto the persisted
    /// <see cref="LwwValue{T}.VectorClock"/>.
    /// </summary>
    [Id(4)] public VersionVector? SourceVectorClock { get; init; }

    /// <summary>
    /// Absolute UTC tick expiry; <c>0</c> means no expiry. Must be
    /// <c>0</c> when <see cref="IsTombstone"/> is <see langword="true"/>.
    /// </summary>
    [Id(5)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// <see langword="true"/> when this item represents a remote Delete
    /// (the persisted entry is a tombstone <see cref="LwwValue{T}"/>);
    /// <see langword="false"/> when it represents a remote Set.
    /// </summary>
    [Id(6)] public bool IsTombstone { get; init; }
}