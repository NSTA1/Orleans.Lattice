using Orleans.Lattice;

namespace Orleans.Lattice.Primitives;

/// <summary>
/// A delta representing changes to a leaf node since a given version.
/// Contains only the entries whose <see cref="LwwValue{T}.Timestamp"/> is
/// strictly newer than the corresponding entry in the requester's version vector.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.StateDelta)]
internal sealed record StateDelta
{
    /// <summary>The changed entries: key → LWW-wrapped value (including tombstones).</summary>
    [Id(0)] public required Dictionary<string, LwwValue<byte[]>> Entries { get; init; }

    /// <summary>
    /// The version vector of the sender <em>at the time the delta was extracted</em>.
    /// The receiver should merge this into its own vector after applying the entries.
    /// </summary>
    [Id(1)] public required VersionVector Version { get; init; }

    /// <summary>
    /// If the leaf has been split, the key at which it was split. Cache consumers
    /// should prune any locally held entries with keys ≥ this value because those
    /// entries now belong to a different leaf.
    /// </summary>
    [Id(2)] public string? SplitKey { get; init; }

    /// <summary>
    /// Sorted virtual-slot indices that have migrated away from the source
    /// leaf since the requester's <see cref="Version"/>. Cache consumers
    /// should prune any locally held entries whose key hashes (via
    /// <c>ShardMap.GetVirtualSlot</c> with <see cref="MovedAwayVsc"/>) into
    /// one of these slots, because those keys are now owned by a different
    /// shard and the leaf's read entrypoints have started returning null
    /// for them.
    /// </summary>
    [Id(3)] public int[]? MovedAwaySlots { get; init; }

    /// <summary>
    /// The virtual shard count in force when <see cref="MovedAwaySlots"/>
    /// was populated. Required by cache consumers to map keys into slots
    /// for the prune pass.
    /// </summary>
    [Id(4)] public int? MovedAwayVsc { get; init; }

    /// <summary><c>true</c> if there were no changes to send.</summary>
    public bool IsEmpty => Entries.Count == 0;
}
