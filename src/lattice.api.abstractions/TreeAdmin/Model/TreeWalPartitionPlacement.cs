namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The provider placement of a single WAL partition: which storage provider key
/// backs it and whether the reporting silo can currently resolve that key. The
/// control-API mirror of the core WAL partition placement, so the tree-admin wire
/// contract stays decoupled from the core DTO.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalPartitionPlacement)]
[Immutable]
public readonly record struct TreeWalPartitionPlacement
{
    /// <summary>The WAL partition index.</summary>
    [Id(0)] public int Partition { get; init; }

    /// <summary>The storage provider catalog key backing this partition's log.</summary>
    [Id(1)] public string ProviderKey { get; init; }

    /// <summary>
    /// <see langword="true"/> when the silo that produced this report can resolve
    /// <see cref="ProviderKey"/> through its WAL storage provider catalog. A
    /// <see langword="false"/> value means WAL shards for this partition fail closed
    /// on that silo until the key is registered there.
    /// </summary>
    [Id(2)] public bool ResolvableOnThisSilo { get; init; }
}
