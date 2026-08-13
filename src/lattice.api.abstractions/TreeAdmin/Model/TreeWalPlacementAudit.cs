using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A health audit of a tree's WAL placement against the resolving silo's WAL
/// storage provider catalog, returned by the tree-admin WAL placement audit verb.
/// Surfaces any partition pinned to a provider key the silo cannot resolve so an
/// operator can detect configuration drift before WAL shards begin to fail closed.
/// The control-API mirror of the core WAL placement audit DTO.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalPlacementAudit)]
[Immutable]
public sealed record TreeWalPlacementAudit
{
    /// <summary>The tree this audit describes.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The current placement version.</summary>
    [Id(1)] public long Version { get; init; }

    /// <summary>The pinned WAL partition count for the tree.</summary>
    [Id(2)] public int PartitionCount { get; init; }

    /// <summary>Per-partition placement with per-partition resolvability flags.</summary>
    [Id(3)] public ImmutableArray<TreeWalPartitionPlacement> Partitions { get; init; }

    /// <summary>
    /// <see langword="true"/> when every partition's provider key resolves on the
    /// silo that produced this audit. When <see langword="false"/>, at least one
    /// partition's WAL shards fail closed on this silo.
    /// </summary>
    [Id(4)] public bool AllResolvableOnThisSilo { get; init; }

    /// <summary>
    /// The provider keys registered in this silo's catalog (always including the
    /// default provider key), sorted.
    /// </summary>
    [Id(5)] public ImmutableArray<string> KnownProviderKeys { get; init; }
}
