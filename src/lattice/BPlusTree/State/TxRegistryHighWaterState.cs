namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryHighWaterGrain"/>: the
/// durable, monotone per-tree saga decision registry shard high-water mark
/// (issue #3501).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistryHighWaterState)]
internal sealed class TxRegistryHighWaterState
{
    /// <summary>
    /// One more than the highest registry shard index that may hold a decision.
    /// Zero for a tree no shard has written to: the legacy registry only.
    /// </summary>
    [Id(0)]
    public int ShardHighWater { get; set; }
}
