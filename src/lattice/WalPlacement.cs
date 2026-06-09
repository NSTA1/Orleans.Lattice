using System.Collections.Immutable;

namespace Orleans.Lattice;

/// <summary>
/// A point-in-time view of a tree's durable WAL placement, returned by
/// <see cref="ILatticeAdmin.GetWalPlacementAsync"/>. Reports which
/// <see cref="IWalStorageProviderCatalog"/> key backs each WAL partition and
/// the placement version used for compare-and-swap when moving a partition.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalPlacement)]
[Immutable]
public readonly record struct WalPlacement
{
    /// <summary>The tree this placement describes.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// Monotonic placement version. Pass this to
    /// <see cref="ILatticeAdmin.ExecuteWalMoveAsync"/> via the plan so a move
    /// aborts if the placement changed underneath the caller.
    /// </summary>
    [Id(1)] public long Version { get; init; }

    /// <summary>
    /// The catalog key used for any partition without an explicit override
    /// (<see cref="IWalStorageProviderCatalog.DefaultProviderKey"/> by default).
    /// </summary>
    [Id(2)] public string DefaultProviderKey { get; init; }

    /// <summary>
    /// One entry per WAL partition, ordered by
    /// <see cref="WalPartitionPlacement.Partition"/>.
    /// </summary>
    [Id(3)] public ImmutableArray<WalPartitionPlacement> Partitions { get; init; }
}
