using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A read-only inspection of a tree's shard-map topology: how the virtual routing
/// space maps onto physical shards. A summary, not the raw slot array - the
/// per-virtual-slot assignment table (which can be thousands of entries) is
/// distilled into the set of distinct physical shard indices plus counts, which is
/// what an operator or a rebalancing tool actually needs.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.ShardMapInspection)]
[Immutable]
public sealed record ShardMapInspection
{
    /// <summary>The logical tree id the shard map was inspected for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The physical tree id the logical tree currently resolves to. Differs from
    /// <see cref="TreeId"/> after a reshard swaps the physical backing tree.
    /// </summary>
    [Id(1)] public required string PhysicalTreeId { get; init; }

    /// <summary>The size of the virtual routing space (number of virtual slots).</summary>
    [Id(2)] public int VirtualShardCount { get; init; }

    /// <summary>The number of distinct physical shards the virtual slots map onto.</summary>
    [Id(3)] public int PhysicalShardCount { get; init; }

    /// <summary>
    /// The monotonic version of the shard map. Increments on every reshard, so a
    /// client can detect a topology change between two inspections.
    /// </summary>
    [Id(4)] public long MapVersion { get; init; }

    /// <summary>
    /// The distinct physical shard indices the virtual slots resolve to, ascending.
    /// Never the default (empty only for a tree with no shards).
    /// </summary>
    [Id(5)] public ImmutableArray<int> PhysicalShardIndices { get; init; } = ImmutableArray<int>.Empty;
}
