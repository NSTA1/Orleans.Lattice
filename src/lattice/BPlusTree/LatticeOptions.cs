namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Configuration constants for the Lattice.
/// </summary>
public static class LatticeOptions
{
    /// <summary>Maximum number of keys per leaf node before a split is triggered.</summary>
    public const int MaxLeafKeys = 128;

    /// <summary>Maximum number of children per internal node before a split is triggered.</summary>
    public const int MaxInternalChildren = 128;

    /// <summary>Default number of shards for the shard router.</summary>
    public const int DefaultShardCount = 64;
}
