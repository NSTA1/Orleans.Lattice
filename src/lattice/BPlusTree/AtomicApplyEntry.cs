using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Single item in a cross-cluster atomic batch submitted via
/// <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/>. Carries the
/// authoring cluster's per-key metadata
/// (<see cref="Timestamp"/>, <see cref="ExpiresAtTicks"/>,
/// <see cref="VectorClock"/>, <see cref="IsTombstone"/>) so the receiver-side
/// saga can re-stamp the persisted <see cref="LwwValue{T}"/> bit-identically
/// — preserving the source-HLC-preservation invariant the per-entry apply
/// seam already enforces.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IsTombstone"/> distinguishes a remote Set from a remote
/// Delete: tombstone items have a <see langword="null"/>
/// <see cref="Value"/> and are persisted as a tombstone
/// <see cref="LwwValue{T}"/>; non-tombstone items carry the committed
/// payload bytes. <see cref="ExpiresAtTicks"/> is preserved for
/// non-tombstone items only and must be <c>0</c> on tombstones.
/// </para>
/// <para>
/// The vector-clock frontier is per-entry on the wire so a future
/// producer that captures per-key VCs need not break the contract;
/// today every entry in a saga batch shares the same frontier (the
/// saga-wide VC captured by the source-side
/// <c>SetManyAtomicAsync</c>'s ambient
/// <see cref="LatticeVectorClockContext"/>).
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.AtomicApplyEntry)]
[Immutable]
internal readonly record struct AtomicApplyEntry
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
    [Id(2)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// Absolute UTC tick expiry; <c>0</c> means no expiry. Must be
    /// <c>0</c> when <see cref="IsTombstone"/> is <see langword="true"/>.
    /// </summary>
    [Id(3)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <see langword="null"/> when the producing cluster does not
    /// stamp a frontier. Stamped verbatim onto the persisted
    /// <see cref="LwwValue{T}.VectorClock"/>.
    /// </summary>
    [Id(4)] public VersionVector? VectorClock { get; init; }

    /// <summary>
    /// <see langword="true"/> when this item represents a remote Delete
    /// (the persisted entry is a tombstone <see cref="LwwValue{T}"/>);
    /// <see langword="false"/> when it represents a remote Set.
    /// </summary>
    [Id(5)] public bool IsTombstone { get; init; }
}
