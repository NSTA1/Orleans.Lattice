namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A sequenced <see cref="ReplogEntry"/> as returned from a WAL shard
/// read. Pairs the captured mutation record with the per-shard sequence
/// number assigned at append time.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplogShardEntry)]
[Immutable]
internal readonly record struct ReplogShardEntry
{
    /// <summary>The per-shard, monotonically-increasing sequence number assigned at append time.</summary>
    [Id(0)] public long Sequence { get; init; }

    /// <summary>The captured mutation record.</summary>
    [Id(1)] public ReplogEntry Entry { get; init; }
}
