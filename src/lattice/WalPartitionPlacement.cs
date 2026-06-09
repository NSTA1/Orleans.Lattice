namespace Orleans.Lattice;

/// <summary>
/// The provider placement of a single WAL partition: which
/// <see cref="IWalStorageProviderCatalog"/> key backs it and whether the
/// resolving silo can currently resolve that key.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalPartitionPlacement)]
[Immutable]
public readonly record struct WalPartitionPlacement
{
    /// <summary>The WAL partition index.</summary>
    [Id(0)] public int Partition { get; init; }

    /// <summary>The catalog key backing this partition's log.</summary>
    [Id(1)] public string ProviderKey { get; init; }

    /// <summary>
    /// <see langword="true"/> when the silo that produced this report can
    /// resolve <see cref="ProviderKey"/> through its
    /// <see cref="IWalStorageProviderCatalog"/>. A <see langword="false"/> value
    /// means WAL shards for this partition fail closed on that silo until the
    /// key is registered there.
    /// </summary>
    [Id(2)] public bool ResolvableOnThisSilo { get; init; }
}
