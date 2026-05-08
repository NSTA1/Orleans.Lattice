namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A sequenced <see cref="WalRecord"/> as returned from a WAL shard
/// read. Pairs the captured mutation record with the per-shard sequence
/// number assigned at append time.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalShardSequencedEntry)]
[Immutable]
internal readonly record struct WalShardSequencedEntry
{
    /// <summary>The per-shard, monotonically-increasing sequence number assigned at append time.</summary>
    [Id(0)] public long Sequence { get; init; }

    /// <summary>The captured mutation record.</summary>
    [Id(1)] public WalRecord Entry { get; init; }
}
