using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A read-only view of a tree's <b>registry-persisted</b> shard map: the durable
/// record of how the virtual routing space maps onto physical shards, as stored in
/// the tree registry. Distinct from the live-routing shard-map inspection - this
/// reflects what the registry has durably persisted and explicitly signals whether a
/// custom map has been written at all.
/// </summary>
/// <remarks>
/// A tree that has never been resharded uses the default identity map, which the
/// registry does not persist; that case is reported as <see cref="HasCustomMap"/>
/// <see langword="false"/> with an empty <see cref="PhysicalShardIndices"/> and a
/// zero <see cref="MapVersion"/>. Only once an adaptive split or reshard writes a
/// custom map does the registry hold one, reported here as <see cref="HasCustomMap"/>
/// <see langword="true"/> with the persisted slot topology.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeShardMapView)]
[Immutable]
public sealed record TreeShardMapView
{
    /// <summary>The logical tree id the shard map was read for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the registry holds a custom persisted shard map for
    /// the tree; <see langword="false"/> when the tree uses the default identity map
    /// (no persisted map), in which case the remaining fields are at their unset
    /// defaults.
    /// </summary>
    [Id(1)] public bool HasCustomMap { get; init; }

    /// <summary>
    /// The monotonic version of the persisted shard map, incremented on every
    /// persist. Zero when no custom map is persisted.
    /// </summary>
    [Id(2)] public long MapVersion { get; init; }

    /// <summary>The size of the virtual routing space (number of virtual slots). Zero when no custom map is persisted.</summary>
    [Id(3)] public int VirtualShardCount { get; init; }

    /// <summary>The number of distinct physical shards the virtual slots map onto. Zero when no custom map is persisted.</summary>
    [Id(4)] public int PhysicalShardCount { get; init; }

    /// <summary>
    /// The distinct physical shard indices the virtual slots resolve to, ascending.
    /// Empty when no custom map is persisted.
    /// </summary>
    [Id(5)] public ImmutableArray<int> PhysicalShardIndices { get; init; } = ImmutableArray<int>.Empty;
}
