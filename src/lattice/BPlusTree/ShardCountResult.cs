using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result of a strongly-consistent shard count operation
/// (<see cref="IShardRootGrain.CountWithMovedAwayAsync"/>): the live key
/// count from the shard at scan time, plus the set of virtual slots the
/// shard filtered out because they are being or have been moved to another
/// physical shard by an adaptive split (F-011).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardCountResult)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly record struct ShardCountResult
{
    /// <summary>
    /// The number of live, non-tombstoned keys this shard authoritatively
    /// owns at scan time, excluding any keys whose virtual slot is in
    /// <see cref="MovedAwaySlots"/>.
    /// </summary>
    [Id(0)] public int Count { get; init; }

    /// <summary>
    /// Virtual slots that the shard excluded from <see cref="Count"/>
    /// because the slot has been (or is being) moved to another physical
    /// shard. <c>null</c> or empty when no slots were filtered.
    /// </summary>
    [Id(1)] public int[]? MovedAwaySlots { get; init; }
}
