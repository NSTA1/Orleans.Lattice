using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A point-in-time view of a tree's durable WAL placement, returned by the
/// tree-admin WAL placement inspection verb. Reports which storage provider key
/// backs each WAL partition and the placement version used for compare-and-swap
/// when moving a partition. The control-API mirror of the core WAL placement DTO.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalPlacement)]
[Immutable]
public sealed record TreeWalPlacement
{
    /// <summary>The tree this placement describes.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Monotonic placement version. A move computed from this placement aborts if
    /// the placement changed underneath the caller.
    /// </summary>
    [Id(1)] public long Version { get; init; }

    /// <summary>
    /// The catalog key used for any partition without an explicit override.
    /// </summary>
    [Id(2)] public string DefaultProviderKey { get; init; } = string.Empty;

    /// <summary>
    /// One entry per WAL partition, ordered by
    /// <see cref="TreeWalPartitionPlacement.Partition"/>.
    /// </summary>
    [Id(3)] public ImmutableArray<TreeWalPartitionPlacement> Partitions { get; init; }
}
