using System.Collections.Immutable;

namespace Orleans.Lattice;

/// <summary>
/// A health audit of a tree's WAL placement against the resolving silo's
/// <see cref="IWalStorageProviderCatalog"/>, returned by
/// <see cref="ILatticeAdmin.AuditWalPlacementAsync"/>. Surfaces any partition
/// pinned to a provider key the silo cannot resolve so an operator can detect
/// configuration drift before WAL shards begin to fail closed.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalPlacementAudit)]
[Immutable]
public readonly record struct WalPlacementAudit
{
    /// <summary>The tree this audit describes.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The current placement version.</summary>
    [Id(1)] public long Version { get; init; }

    /// <summary>The pinned WAL partition count for the tree.</summary>
    [Id(2)] public int PartitionCount { get; init; }

    /// <summary>Per-partition placement with per-partition resolvability flags.</summary>
    [Id(3)] public ImmutableArray<WalPartitionPlacement> Partitions { get; init; }

    /// <summary>
    /// <see langword="true"/> when every partition's provider key resolves on
    /// the silo that produced this audit. When <see langword="false"/>, at
    /// least one partition's WAL shards fail closed on this silo.
    /// </summary>
    [Id(4)] public bool AllResolvableOnThisSilo { get; init; }

    /// <summary>
    /// The provider keys registered in this silo's catalog (always including
    /// <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>), sorted.
    /// </summary>
    [Id(5)] public ImmutableArray<string> KnownProviderKeys { get; init; }
}
